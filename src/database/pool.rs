//! Pooled random-access readers for archive shards.
//!
//! A point lookup is: locate the frame containing the offset, read that
//! frame's compressed bytes, decode until the event, slice it out. Everything
//! except the decode is cheap, so the pool exists to remove the two avoidable
//! per-lookup costs:
//!
//! * `open()` + `seek()` (20-60us) - file handles are cached and read with
//!   positioned reads (`pread`), so one handle serves concurrent lookups with
//!   no seek races;
//! * `zstd::Decoder::new()` (20-50us, allocates a 1-2 MB window) - decode
//!   contexts are reset and reused.
//!
//! Frame tables are cached too (~16 bytes per frame).
//!
//! # Threading model
//!
//! The read path is deliberately **synchronous**, and async callers reach it
//! through [`ShardReaderPool::read_many_async`], which fans work out onto
//! tokio's blocking pool:
//!
//! * There is no true async file IO on Linux without io_uring - `tokio::fs` is
//!   itself a `spawn_blocking` wrapper around the same `pread`, so an async
//!   read here would add overhead and remove nothing.
//! * Decoding is CPU-bound (~15us/event). Running it on runtime worker threads
//!   would stall the executor, so it belongs on the blocking pool regardless.
//!
//! Parallelism is over *frame groups*, so 100 ids inside a single archive fan
//! out exactly as well as 100 ids spread across 20 archives.

use crate::database::frames::{FrameSpan, FrameTable, sidecar_path};
use anyhow::{Result, anyhow, bail};
use dashmap::DashMap;
use log::warn;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use zstd::zstd_safe::{DCtx, InBuffer, OutBuffer, ResetDirective};

/// Max cached file handles. Archives can hold thousands of shards; keep well
/// clear of the process fd limit.
const DEFAULT_MAX_FILES: usize = 256;

/// Upper bound on a single decoded frame we are willing to buffer. Frames are
/// written at ~512 KiB, so this only trips on hand-made/hostile input.
const MAX_FRAME_BYTES: u64 = 256 * 1024 * 1024;

struct CachedFile {
    file: Arc<File>,
    /// Monotonic tick of last use, for cheap approximate LRU eviction.
    last_used: AtomicU64,
}

/// Reusable decode scratch space.
struct DecodeSlot {
    dctx: DCtx<'static>,
    cin: Vec<u8>,
    out: Vec<u8>,
}

impl DecodeSlot {
    fn new() -> Self {
        Self {
            dctx: DCtx::create(),
            cin: Vec::new(),
            out: Vec::new(),
        }
    }
}

/// A single random-access read request.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub path: std::path::PathBuf,
    pub offset: u64,
    pub len: u32,
}

/// Shared, thread-safe pool of shard readers.
pub struct ShardReaderPool {
    files: DashMap<std::path::PathBuf, CachedFile>,
    frames: DashMap<std::path::PathBuf, Arc<FrameTable>>,
    slots: Mutex<Vec<DecodeSlot>>,
    max_files: usize,
    max_slots: usize,
    /// Worker threads used by [`ShardReaderPool::read_many`].
    concurrency: usize,
    clock: AtomicU64,
}

impl Default for ShardReaderPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardReaderPool {
    pub fn new() -> Self {
        let par = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            files: DashMap::new(),
            frames: DashMap::new(),
            slots: Mutex::new(Vec::new()),
            max_files: DEFAULT_MAX_FILES,
            max_slots: par * 2,
            concurrency: par,
            clock: AtomicU64::new(0),
        }
    }

    /// Number of worker threads used for batch reads (default: CPU count).
    /// Reader threads this pool will use for one batch.
    ///
    /// Exposed so a caller can report it alongside read timings: a hydration
    /// whose cost is flat in the number of events is queueing, and the queue
    /// depth is only interpretable next to this number.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Drop cached state for a shard (e.g. after it was rewritten or removed).
    pub fn invalidate(&self, path: &Path) {
        self.files.remove(path);
        self.frames.remove(path);
    }

    pub fn clear(&self) {
        self.files.clear();
        self.frames.clear();
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn file(&self, path: &Path) -> Result<Arc<File>> {
        if let Some(entry) = self.files.get(path) {
            entry.last_used.store(self.tick(), Ordering::Relaxed);
            return Ok(entry.file.clone());
        }
        let file = Arc::new(File::open(path)?);
        // Evict before insert so the cache never exceeds the cap.
        if self.files.len() >= self.max_files {
            self.evict_one();
        }
        self.files.insert(
            path.to_path_buf(),
            CachedFile {
                file: file.clone(),
                last_used: AtomicU64::new(self.tick()),
            },
        );
        Ok(file)
    }

    fn evict_one(&self) {
        let victim = self
            .files
            .iter()
            .min_by_key(|e| e.last_used.load(Ordering::Relaxed))
            .map(|e| e.key().clone());
        if let Some(v) = victim {
            self.files.remove(&v);
        }
    }

    /// Frame table for a shard; absent sidecar yields an empty table, which
    /// behaves as "one frame starting at 0" (the legacy layout).
    pub fn frame_table(&self, path: &Path) -> Arc<FrameTable> {
        if let Some(t) = self.frames.get(path) {
            return t.clone();
        }
        let table = match FrameTable::load(&sidecar_path(path)) {
            Ok(Some(t)) => t,
            Ok(None) => FrameTable::default(),
            Err(e) => {
                warn!(
                    "{}: unusable frame index ({e}), decoding from start",
                    path.display()
                );
                FrameTable::default()
            }
        };
        let table = Arc::new(table);
        self.frames.insert(path.to_path_buf(), table.clone());
        table
    }

    fn take_slot(&self) -> DecodeSlot {
        self.slots
            .lock()
            .ok()
            .and_then(|mut s| s.pop())
            .unwrap_or_else(DecodeSlot::new)
    }

    fn give_slot(&self, mut slot: DecodeSlot) {
        // Don't let one oversized event permanently pin memory in the pool.
        const MAX_RETAINED: usize = 4 * 1024 * 1024;
        if slot.cin.capacity() > MAX_RETAINED {
            slot.cin = Vec::new();
        }
        if slot.out.capacity() > MAX_RETAINED {
            slot.out = Vec::new();
        }
        if let Ok(mut slots) = self.slots.lock()
            && slots.len() < self.max_slots
        {
            slots.push(slot);
        }
    }

    /// Read `len` bytes at decompressed `offset` from a zstd shard.
    ///
    /// Only the containing frame is read from disk, and decoding stops as soon
    /// as the requested bytes are available.
    pub fn read_zstd_range(&self, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>> {
        let mut out = self.read_zstd_group(path, &[(offset, len)]);
        out.remove(0)
    }

    /// Read several ranges that share one frame: the frame is read from disk
    /// and decoded exactly once, then each range is sliced out of the decoded
    /// buffer. Decoding stops at the furthest requested byte.
    ///
    /// `ranges` must all fall inside the same frame (as grouped by
    /// [`read_many`](Self::read_many)); anything else still returns correct
    /// data, just without the sharing.
    pub fn read_zstd_group(&self, path: &Path, ranges: &[(u64, u32)]) -> Vec<Result<Vec<u8>>> {
        match self.read_zstd_group_inner(path, ranges) {
            Ok(v) => v,
            // Whole-group failure (bad file/frame index): report per range.
            Err(e) => ranges
                .iter()
                .map(|_| Err(anyhow!("{}: {e}", path.display())))
                .collect(),
        }
    }

    fn read_zstd_group_inner(
        &self,
        path: &Path,
        ranges: &[(u64, u32)],
    ) -> Result<Vec<Result<Vec<u8>>>> {
        let first = ranges.first().ok_or_else(|| anyhow!("empty group"))?.0;
        let table = self.frame_table(path);
        let start = table.span_for(first);

        // Decode only as far as the furthest byte anyone in the group wants.
        let mut want = 0u64;
        for (offset, len) in ranges {
            let rel = offset
                .checked_sub(start.uncompressed_start)
                .ok_or_else(|| anyhow!("offset before frame start"))?;
            want = want.max(rel + *len as u64);
        }
        if want > MAX_FRAME_BYTES {
            bail!("refusing to decode {want} bytes for one frame");
        }

        // An event can straddle a frame boundary (reframed or externally
        // produced archives split on byte counts, not event boundaries), so
        // take every frame the range touches.
        let span = table.span_for_range(first, start.uncompressed_start + want);
        let file = self.file(path)?;
        let file_len = file.metadata()?.len();

        let compressed_end = span.compressed_end.unwrap_or(file_len).min(file_len);
        if compressed_end <= span.compressed_start {
            bail!(
                "frame index points past end of file ({} >= {})",
                span.compressed_start,
                compressed_end
            );
        }

        let mut slot = self.take_slot();
        let decoded = self.decode_span(&file, span, compressed_end, want, &mut slot);
        let out = match decoded {
            Ok(produced) => ranges
                .iter()
                .map(|(offset, len)| {
                    let start = (offset - span.uncompressed_start) as usize;
                    let end = start + *len as usize;
                    if end > produced {
                        Err(anyhow!(
                            "{}: event truncated: decoded {produced} of {end} bytes",
                            path.display()
                        ))
                    } else {
                        Ok(slot.out[start..end].to_vec())
                    }
                })
                .collect(),
            Err(e) => ranges
                .iter()
                .map(|_| Err(anyhow!("{}: {e}", path.display())))
                .collect(),
        };
        self.give_slot(slot);
        Ok(out)
    }

    /// Decode `want` bytes from the start of `span` into `slot.out`.
    /// Returns how many bytes were actually produced.
    fn decode_span(
        &self,
        file: &File,
        span: FrameSpan,
        compressed_end: u64,
        want: u64,
        slot: &mut DecodeSlot,
    ) -> Result<usize> {
        let clen = (compressed_end - span.compressed_start) as usize;
        slot.cin.clear();
        slot.cin.resize(clen, 0);
        read_exact_at(file, &mut slot.cin, span.compressed_start)?;

        let want = want as usize;
        slot.out.clear();
        slot.out.reserve(want);

        slot.dctx
            .reset(ResetDirective::SessionOnly)
            .map_err(|c| anyhow!("zstd reset failed: {}", zstd::zstd_safe::get_error_name(c)))?;

        let DecodeSlot { dctx, cin, out } = slot;
        let mut input = InBuffer::around(&cin[..]);
        let produced = {
            let mut output = OutBuffer::around(out);
            loop {
                let before = output.pos();
                let hint = dctx
                    .decompress_stream(&mut output, &mut input)
                    .map_err(|c| {
                        anyhow!("zstd decode failed: {}", zstd::zstd_safe::get_error_name(c))
                    })?;
                if output.pos() >= want {
                    break; // early exit: we have everything the group wants
                }
                if hint == 0 && input.pos() >= cin.len() {
                    break; // frame finished
                }
                if output.pos() == before && input.pos() >= cin.len() {
                    break; // no progress and no more input
                }
            }
            output.pos()
        };
        Ok(produced)
    }

    /// Read many ranges at once, in parallel.
    ///
    /// Requests are grouped by `(shard, frame)` so a frame that contains
    /// several wanted events is read and decoded exactly once, and groups are
    /// ordered by compressed offset so each shard is walked forward.
    ///
    /// Parallelism is over *frame groups*, not shards: 100 ids inside one big
    /// archive fan out just as well as 100 ids spread over 20 archives.
    /// Positioned reads mean the cached fd is shared without seek races.
    ///
    /// Returns one slot per input request, `None` where the read failed.
    ///
    /// Blocking: call from a blocking context, or use
    /// [`read_many_async`](Self::read_many_async) from async code.
    pub fn read_many(&self, requests: &[ReadRequest]) -> Vec<Option<Vec<u8>>> {
        let mut results: Vec<Option<Vec<u8>>> = vec![None; requests.len()];
        if requests.is_empty() {
            return results;
        }

        let work = self.plan(requests);
        let workers = self.concurrency.min(work.len()).max(1);
        let next = std::sync::atomic::AtomicUsize::new(0);
        let out = Mutex::new(Vec::<(usize, Vec<u8>)>::new());

        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    let mut local: Vec<(usize, Vec<u8>)> = Vec::new();
                    loop {
                        let w = next.fetch_add(1, Ordering::Relaxed);
                        let Some(idxs) = work.get(w) else { break };
                        self.run_group(requests, idxs, &mut local);
                    }
                    if let Ok(mut out) = out.lock() {
                        out.append(&mut local);
                    }
                });
            }
        });

        for (i, bytes) in out.into_inner().unwrap_or_default() {
            results[i] = Some(bytes);
        }
        results
    }

    /// Async batch read: same grouping, but the work runs on tokio's blocking
    /// pool instead of threads spawned per call, so it composes with the
    /// runtime (and never blocks a worker thread).
    ///
    /// Exactly `concurrency` blocking tasks are spawned regardless of batch
    /// size - groups are dealt round-robin between them.
    pub async fn read_many_async(
        self: &Arc<Self>,
        requests: Arc<Vec<ReadRequest>>,
    ) -> Vec<Option<Vec<u8>>> {
        let mut results: Vec<Option<Vec<u8>>> = vec![None; requests.len()];
        if requests.is_empty() {
            return results;
        }

        let work = self.plan(&requests);
        let workers = self.concurrency.min(work.len()).max(1);
        let work = Arc::new(work);

        let mut tasks = Vec::with_capacity(workers);
        for w in 0..workers {
            let pool = self.clone();
            let work = work.clone();
            let requests = requests.clone();
            // spawn_blocking returns immediately, so all buckets run
            // concurrently even though we await the handles in order.
            tasks.push(tokio::task::spawn_blocking(move || {
                let mut local = Vec::new();
                for idxs in work.iter().skip(w).step_by(workers) {
                    pool.run_group(&requests, idxs, &mut local);
                }
                local
            }));
        }

        for task in tasks {
            match task.await {
                Ok(local) => {
                    for (i, bytes) in local {
                        results[i] = Some(bytes);
                    }
                }
                Err(e) => warn!("shard read task failed: {e}"),
            }
        }
        results
    }

    /// Serve one frame group: a single read + decode for zstd shards, with
    /// every requested range sliced out of the same decoded buffer.
    fn run_group(&self, requests: &[ReadRequest], idxs: &[usize], out: &mut Vec<(usize, Vec<u8>)>) {
        let path = &requests[idxs[0]].path;
        let reads: Vec<Result<Vec<u8>>> = match path.extension().and_then(|e| e.to_str()) {
            Some("zst") | Some("zstd") => {
                // One read + one decode for the whole group.
                let ranges: Vec<(u64, u32)> = idxs
                    .iter()
                    .map(|&i| (requests[i].offset, requests[i].len))
                    .collect();
                self.read_zstd_group(path, &ranges)
            }
            Some("gz") | Some("bz2") => idxs
                .iter()
                .map(|&i| self.read_stream_range(path, requests[i].offset, requests[i].len))
                .collect(),
            _ => idxs
                .iter()
                .map(|&i| self.read_plain_range(path, requests[i].offset, requests[i].len))
                .collect(),
        };
        for (&i, read) in idxs.iter().zip(reads) {
            match read {
                Ok(bytes) => out.push((i, bytes)),
                Err(e) => warn!(
                    "{}: read at {} failed: {e}",
                    requests[i].path.display(),
                    requests[i].offset
                ),
            }
        }
    }

    /// Group requests by `(shard, frame)` and order them so each shard is
    /// walked forward. Returns one bucket of request indices per frame.
    fn plan(&self, requests: &[ReadRequest]) -> Vec<Vec<usize>> {
        let mut groups: std::collections::HashMap<(std::path::PathBuf, u64), Vec<usize>> =
            std::collections::HashMap::new();
        let mut plain = Vec::new();
        for (i, r) in requests.iter().enumerate() {
            match r.path.extension().and_then(|e| e.to_str()) {
                Some("zst") | Some("zstd") => {
                    let span = self.frame_table(&r.path).span_for(r.offset);
                    groups
                        .entry((r.path.clone(), span.compressed_start))
                        .or_default()
                        .push(i);
                }
                // Non-zstd shards have no frames to share; handle individually.
                _ => plain.push(i),
            }
        }

        let mut keyed: Vec<((std::path::PathBuf, u64), Vec<usize>)> = groups.into_iter().collect();
        keyed.sort_by(|a, b| a.0.0.cmp(&b.0.0).then(a.0.1.cmp(&b.0.1)));
        let mut work: Vec<Vec<usize>> = keyed.into_iter().map(|(_, v)| v).collect();
        work.extend(plain.into_iter().map(|i| vec![i]));
        work
    }

    /// Read `len` bytes at `offset` from an uncompressed `.jsonl`/`.json`
    /// shard - a single positioned read, no decode.
    pub fn read_plain_range(&self, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>> {
        let file = self.file(path)?;
        let mut buf = vec![0u8; len as usize];
        read_exact_at(&file, &mut buf, offset)?;
        Ok(buf)
    }

    /// Slow path for gzip/bzip2 shards: no frame seeking is possible, so
    /// decode from the start and discard until the offset.
    pub fn read_stream_range(&self, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>> {
        let mut reader = open_stream_decoder(path)?;
        std::io::copy(&mut (&mut reader).take(offset), &mut std::io::sink())?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

pub(crate) fn open_stream_decoder(path: &Path) -> Result<Box<dyn Read + Send>> {
    let f = File::open(path)?;
    match path.extension().and_then(|e| e.to_str()) {
        #[cfg(feature = "sync")]
        Some("gz") => Ok(Box::new(flate2::read::GzDecoder::new(f))),
        #[cfg(feature = "sync")]
        Some("bz2") => Ok(Box::new(bzip2::read::BzDecoder::new(f))),
        #[cfg(not(feature = "sync"))]
        Some(ext @ ("gz" | "bz2")) => {
            bail!("{ext} archives need the `sync` feature for random access")
        }
        Some("zst") | Some("zstd") => Ok(Box::new(zstd::stream::Decoder::new(f)?)),
        _ => Ok(Box::new(f)),
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut read = 0;
    while read < buf.len() {
        match file.seek_read(&mut buf[read..], offset + read as u64)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read",
                ));
            }
            n => read += n,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::file::CompressedJsonLFile;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Ev {
        n: u64,
        pad: String,
    }

    fn write_archive(path: &Path, count: u64, frame_target: u64) -> Vec<crate::EventLoc> {
        let mut w = CompressedJsonLFile::with_frame_target(path, frame_target).unwrap();
        let locs = (0..count)
            .map(|n| {
                w.write_event(&Ev {
                    n,
                    pad: "x".repeat(64),
                })
                .unwrap()
            })
            .collect();
        w.finish().unwrap();
        locs
    }

    #[test]
    fn reads_events_across_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let locs = write_archive(&path, 1000, 2048);
        let pool = ShardReaderPool::new();
        assert!(pool.frame_table(&path).len() > 5, "want several frames");

        for (n, loc) in locs.iter().enumerate() {
            let raw = pool.read_zstd_range(&path, loc.offset, loc.len).unwrap();
            let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), n as u64, "wrong event at {n}");
        }
    }

    #[test]
    fn works_without_a_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let locs = write_archive(&path, 200, 2048);
        std::fs::remove_file(crate::database::frames::sidecar_path(&path)).unwrap();

        let pool = ShardReaderPool::new();
        let loc = locs[150];
        let raw = pool.read_zstd_range(&path, loc.offset, loc.len).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["n"].as_u64().unwrap(), 150);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_batch_groups_and_parallelises() {
        let dir = tempfile::tempdir().unwrap();
        // Two shards, so the batch spans files as well as frames.
        let mut all = Vec::new();
        for s in 0..2u64 {
            let path = dir.path().join(format!("events_{s}.jsonl.zst"));
            let locs = write_archive(&path, 500, 4096);
            all.push((path, locs));
        }

        let pool = Arc::new(ShardReaderPool::new());
        let mut requests = Vec::new();
        let mut expect = Vec::new();
        for (path, locs) in &all {
            // Neighbouring events share a frame - exercises group sharing.
            for n in [0usize, 1, 2, 250, 251, 499] {
                requests.push(ReadRequest {
                    path: path.clone(),
                    offset: locs[n].offset,
                    len: locs[n].len,
                });
                expect.push(n as u64);
            }
        }

        let got = pool.read_many_async(Arc::new(requests)).await;
        assert_eq!(got.len(), expect.len());
        for (raw, n) in got.iter().zip(&expect) {
            let v: serde_json::Value = serde_json::from_slice(raw.as_ref().unwrap()).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), *n);
        }
    }

    #[test]
    fn sync_batch_matches_individual_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let locs = write_archive(&path, 400, 2048);
        let pool = ShardReaderPool::new();

        let requests: Vec<ReadRequest> = locs
            .iter()
            .map(|l| ReadRequest {
                path: path.clone(),
                offset: l.offset,
                len: l.len,
            })
            .collect();
        let batched = pool.read_many(&requests);
        for (n, got) in batched.iter().enumerate() {
            let one = pool
                .read_zstd_range(&path, locs[n].offset, locs[n].len)
                .unwrap();
            assert_eq!(got.as_ref().unwrap(), &one, "batch != single at {n}");
        }
    }

    #[test]
    fn concurrent_lookups_are_correct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let locs = Arc::new(write_archive(&path, 2000, 4096));
        let pool = Arc::new(ShardReaderPool::new());

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let pool = pool.clone();
                let locs = locs.clone();
                let path = path.clone();
                std::thread::spawn(move || {
                    for i in (t..locs.len()).step_by(8) {
                        let loc = locs[i];
                        let raw = pool.read_zstd_range(&path, loc.offset, loc.len).unwrap();
                        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                        assert_eq!(v["n"].as_u64().unwrap(), i as u64);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn fd_cache_evicts_without_breaking_reads() {
        let dir = tempfile::tempdir().unwrap();
        let mut pool = ShardReaderPool::new();
        pool.max_files = 2;
        let pool = pool;

        let mut shards = Vec::new();
        for s in 0..5u64 {
            let path = dir.path().join(format!("events_{s}.jsonl.zst"));
            let locs = write_archive(&path, 50, 2048);
            shards.push((path, locs));
        }
        for _ in 0..3 {
            for (path, locs) in &shards {
                let loc = locs[25];
                let raw = pool.read_zstd_range(path, loc.offset, loc.len).unwrap();
                let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                assert_eq!(v["n"].as_u64().unwrap(), 25);
            }
        }
        assert!(pool.files.len() <= 2, "fd cache exceeded its cap");
    }

    #[test]
    fn plain_file_range_is_a_single_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, b"{\"n\":0}\n{\"n\":1}\n").unwrap();
        let pool = ShardReaderPool::new();
        let raw = pool.read_plain_range(&path, 8, 7).unwrap();
        assert_eq!(raw, b"{\"n\":1}");
    }
}
