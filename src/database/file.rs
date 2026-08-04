use crate::database::frames::{FrameLog, FrameStart, FrameTable, ScanReport, sidecar_path};
use crate::database::value::EventLoc;
use anyhow::{Context, Result, bail};
use log::warn;
use serde::Serialize;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zstd::Encoder;

/// Default amount of *uncompressed* data per zstd frame.
///
/// This is the main lookup-latency knob: a point lookup decodes from the start
/// of the containing frame, so average decode work is ~half a frame.
///
/// Measured on 200k **real** events from a nostr.band archive
/// (`examples/real_archive_check.rs`):
///
/// ```text
/// frame        archive   vs single frame   single lookup   batched
///   8 KiB     57.7 MiB        +46%              22 us      4.2 us/ev
///  32 KiB     52.0 MiB        +32%              35 us      4.4 us/ev
/// 128 KiB     48.5 MiB        +23%              85 us      5.7 us/ev
/// 512 KiB     44.6 MiB        +13%             472 us      7.1 us/ev  <- default
///   4 MiB     40.0 MiB        +1.5%           2131 us     10.9 us/ev
/// single      39.4 MiB          -            (whole shard decoded)
/// ```
///
/// Smaller frames are *not* free on real data: events share pubkeys, tags and
/// phrasing, so a bigger zstd window compresses better. Raising the
/// compression level does not recover it (a frame cannot see past itself);
/// only a trained dictionary would, at the cost of the archive no longer being
/// a plain `.zst`.
///
/// 512 KiB trades single-event latency - which batching largely hides, since
/// [`ShardReaderPool::read_many`](crate::ShardReaderPool::read_many) decodes
/// each frame once for the whole group - for 19% less disk than 32 KiB.
/// Lower it if interactive single-id fetches dominate.
pub const DEFAULT_FRAME_TARGET: u64 = 512 * 1024;

/// Take an advisory exclusive lock on a shard.
///
/// Two writers on one shard is *the* way to corrupt an archive, and it does
/// not look like corruption while it happens: both open with `O_APPEND`, both
/// write valid zstd, and the kernel interleaves their chunks at EOF. The file
/// then contains two shuffled streams - each frame's block headers describe
/// byte ranges that now belong to the other writer, so the walk derails and
/// every decode stops a few MB in. Nothing reports an error at write time.
///
/// The lock is per open file handle (`flock` on unix, `LockFileEx` on
/// Windows), so it also catches a second `CompressedJsonLFile` inside the
/// *same* process - the case a `fcntl` record lock would happily allow. It is
/// advisory: readers and external tools are unaffected.
///
/// The lock lives as long as the handle, i.e. until the writer is dropped, and
/// is released by the kernel if the process dies - no stale lock files to
/// clean up after a crash.
fn lock_shard_exclusive(file: &File, path: &Path) -> Result<()> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => bail!(
            "{}: already open by another archive writer; refusing to append \
             (concurrent writers interleave zstd frames and corrupt the shard)",
            path.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("{}: locking shard", path.display()))
        }
    }
}

/// Hold an advisory lock on a shard while rewriting it.
///
/// Repair, reframe and convert all rename a new file into place. Doing that
/// under a live writer silently orphans it: its descriptor still points at the
/// unlinked inode, so everything it writes afterwards lands nowhere.
fn lock_shard_standalone(path: &Path) -> Result<File> {
    let file = File::options().read(true).open(path)?;
    lock_shard_exclusive(&file, path)?;
    Ok(file)
}

/// Open a zstd encoder with a frame checksum.
///
/// The 4-byte trailer costs nothing measurable per 512 KiB frame and turns
/// silent corruption into a decode error attributed to the *frame that owns
/// it*, which is what makes [`repair_archive`] able to say which events are
/// suspect instead of guessing.
fn new_encoder(file: File, level: i32) -> Result<Encoder<'static, File>> {
    let mut enc = Encoder::new(file, level)?;
    enc.include_checksum(true)?;
    Ok(enc)
}

/// A ZSTD compressed JSON-L appender that reports where each event landed.
///
/// Events are written as concatenated bounded zstd frames (still a plain
/// valid `.zst` file), and every frame boundary is recorded in a
/// `<shard>.frames` sidecar so a reader can seek near an offset instead of
/// decoding the whole shard.
pub struct CompressedJsonLFile {
    /// `None` only between finishing one frame and starting the next.
    stream: Option<Encoder<'static, File>>,
    frames: FrameLog,
    path: PathBuf,
    level: i32,
    frame_target: u64,
    /// Offset in the decompressed stream where the next event will start.
    uncompressed_pos: u64,
    /// Offset in the file where the current frame starts.
    frame_compressed_start: u64,
    /// Decompressed offset where the current frame starts.
    frame_uncompressed_start: u64,
    /// Scratch buffer for serialising events.
    buf: Vec<u8>,
}

impl CompressedJsonLFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_frame_target(path, DEFAULT_FRAME_TARGET)
    }

    /// Open an archive with a custom frame size (see [`DEFAULT_FRAME_TARGET`]).
    pub fn with_frame_target<P: AsRef<Path>>(path: P, frame_target: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let level = 3;
        let mut file = File::options().create(true).append(true).open(&path)?;
        lock_shard_exclusive(&file, &path)?;
        let compressed_len = file.seek(SeekFrom::End(0))?;

        // Resume: work out the decompressed length already in the file so new
        // offsets continue the logical stream.
        let uncompressed_pos = Self::resume_position(&path, compressed_len)?;

        let mut frames = FrameLog::open(&path)?;
        // Record the boundary for the frame we are about to open -- unless it
        // would repeat the last one.
        //
        // Every open appends a boundary at EOF, so a process that opened the
        // shard and died before writing an event left the sidecar's last record
        // pointing exactly where the next open computes its own. Appending it
        // again makes the sidecar non-monotonic, which `FrameTable::load`
        // rejects -- so a crash loop manufactured the very corruption that made
        // the next start fail. A boundary that does not advance carries no
        // information, so skipping it is both safe and sufficient.
        let last = FrameTable::load(&sidecar_path(&path))?.and_then(|t| t.last());
        let boundary = FrameStart {
            uncompressed: uncompressed_pos,
            compressed: compressed_len,
        };
        // Stricter than what `FrameTable::load` accepts, deliberately. A
        // sidecar may legitimately *contain* empty frames (equal `uncompressed`
        // offsets), but there is no reason to *write* one: a boundary that adds
        // no decompressed bytes describes a frame with nothing to seek to.
        //
        // Requiring `uncompressed` to advance is also what stops a crash loop
        // from growing the sidecar: closing the encoder on drop writes a frame
        // footer, so `compressed` creeps forward on every open even when no
        // event was written, and a `compressed`-only test would append forever.
        let advances = last.is_none_or(|l| {
            boundary.uncompressed > l.uncompressed && boundary.compressed > l.compressed
        });
        if advances {
            frames.append(boundary)?;
        }

        Ok(Self {
            stream: Some(new_encoder(file, level)?),
            frames,
            path,
            level,
            frame_target: frame_target.max(1),
            uncompressed_pos,
            frame_compressed_start: compressed_len,
            frame_uncompressed_start: uncompressed_pos,
            buf: Vec::with_capacity(4096),
        })
    }

    /// Decompressed length of an existing archive, cheaply where possible.
    ///
    /// The sidecar's last record is the start of the last frame, so only that
    /// frame (bounded by the frame target) needs decoding. Without a sidecar
    /// we must decode the whole file once - correct, but slow, hence the warn.
    fn resume_position(path: &Path, compressed_len: u64) -> Result<u64> {
        if compressed_len == 0 {
            return Ok(0);
        }
        let (tail_uncompressed, tail_compressed) =
            match FrameTable::load_or_rebuild(path)? {
                Some(t) => match t.last() {
                    Some(last) => (last.uncompressed, last.compressed),
                    None => (0, 0),
                },
                None => {
                    warn!(
                        "{}: no frame index, decoding the whole shard once to resume appending",
                        path.display()
                    );
                    (0, 0)
                }
            };
        if tail_compressed >= compressed_len {
            // Sidecar already covers the whole file (clean shutdown).
            return Ok(tail_uncompressed);
        }
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(tail_compressed))?;
        let mut decoder = zstd::stream::Decoder::new(file)?;
        // Count as we go: the last frame is normally *unfinished* (the writer
        // block-flushes rather than closing frames), and a crash can also
        // truncate it. Either way we keep every byte that decoded cleanly and
        // append after it - `io::copy` would discard that count on error.
        let mut n = 0u64;
        let mut buf = vec![0u8; 128 * 1024];
        loop {
            match decoder.read(&mut buf) {
                Ok(0) => break,
                Ok(read) => n += read as u64,
                Err(e) => {
                    warn!(
                        "{}: incomplete tail frame ({e}), resuming after {n} decoded bytes",
                        path.display()
                    );
                    break;
                }
            }
        }
        Ok(tail_uncompressed + n)
    }

    /// Append one event, returning where its JSON line landed in the
    /// decompressed stream.
    pub fn write_event<O: Serialize>(&mut self, event: &O) -> Result<EventLoc> {
        self.buf.clear();
        serde_json::to_writer(&mut self.buf, event)?;
        self.buf.push(b'\n');

        let offset = self.uncompressed_pos;
        let len = (self.buf.len() - 1) as u32; // excluding the newline

        let stream = self
            .stream
            .as_mut()
            .expect("encoder is only detached inside roll_frame");
        stream.write_all(&self.buf)?;
        self.uncompressed_pos += self.buf.len() as u64;

        if self.uncompressed_pos - self.frame_uncompressed_start >= self.frame_target {
            self.roll_frame()?;
        }

        Ok(EventLoc {
            shard: 0, // filled in by the caller, which knows the file name
            offset,
            len,
        })
    }

    /// Close the current zstd frame and open a new one, recording the boundary.
    fn roll_frame(&mut self) -> Result<()> {
        let encoder = self.stream.take().expect("encoder present");
        let mut file = encoder.finish()?;
        let compressed = file.stream_position()?;
        self.frames.append(FrameStart {
            uncompressed: self.uncompressed_pos,
            compressed,
        })?;
        self.frame_compressed_start = compressed;
        self.frame_uncompressed_start = self.uncompressed_pos;
        self.stream = Some(new_encoder(file, self.level)?);
        Ok(())
    }

    /// Current decompressed length (offset the next event would get).
    pub fn uncompressed_len(&self) -> u64 {
        self.uncompressed_pos
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Make everything written so far readable, without ending the frame.
    ///
    /// This is a zstd block flush (`ZSTD_e_flush`): the bytes hit the file and
    /// a decoder streaming the frame can produce them, but the frame stays
    /// open so it keeps growing to [`frame_target`](Self::with_frame_target).
    ///
    /// Ending the frame here instead would make frames as small as a writer
    /// batch - one event per frame for a quiet relay - which bloats the frame
    /// index (16 bytes per frame) and wastes frame headers.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.as_mut() {
            stream.flush()?;
        }
        Ok(())
    }

    /// Close the current frame early (e.g. before rotating shards).
    pub fn flush_frame(&mut self) -> Result<()> {
        if self.uncompressed_pos > self.frame_uncompressed_start {
            self.roll_frame()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        if let Some(encoder) = self.stream.take() {
            let mut file = encoder.finish()?;
            file.flush()?;
        }
        Ok(())
    }
}

impl Drop for CompressedJsonLFile {
    fn drop(&mut self) {
        // Close the frame cleanly so the file is not left with a partial one.
        if let Some(encoder) = self.stream.take() {
            match encoder.finish() {
                Ok(mut f) => {
                    let _ = f.flush();
                }
                Err(e) => warn!("{}: failed to finish zstd frame: {e}", self.path.display()),
            }
        }
    }
}

/// Rebuild the `.frames` sidecar for an existing zstd archive by walking its
/// frame headers and decoding each frame to measure its decompressed size.
///
/// Needed for shards written before framing existed and for archives dropped
/// into the directory by an external relay backup. Costs one full decompress.
pub fn rebuild_frame_index(path: &Path) -> Result<usize> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(0);
    }
    // Streamed, not slurped: a shard can be larger than RAM, and both this scan
    // and the per-frame decode below only ever move forward.
    let mut src = BufReader::new(file);
    let report = crate::database::frames::scan_zstd_frames(&mut src, len)?;

    // Damage means the decompressed stream is no longer contiguous, so offsets
    // past the first fault would be lies. Index the clean prefix - which for a
    // truncated tail, the usual crash, is the entire useful file - and say so.
    // `repair_archive` is the way to get the rest back.
    // The last indexed frame ends where the damage starts, not at EOF: the
    // bytes after it are a half-written frame, and decoding into them to
    // measure the good frame would fail the whole rebuild.
    let usable_end = report.damage.first().map(|d| d.frame_start).unwrap_or(len);
    let usable = if report.is_clean() {
        report.offsets.len()
    } else {
        let d = &report.damage[0];
        warn!(
            "{}: damaged at offset {} ({}); indexing the {} clean frame(s) before it, \
             {} recoverable frame(s) after it need `repair_archive`",
            path.display(),
            d.offset,
            d.reason,
            report.clean_prefix,
            report.offsets.len() - report.clean_prefix,
        );
        report.clean_prefix
    };
    let offsets = &report.offsets[..usable];

    // Write to a temp path then rename, so a crash can't leave a sidecar that
    // disagrees with the shard.
    let final_path = sidecar_path(path);
    let mut tmp = final_path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let mut log = FrameLog::open_at(&tmp)?;
    let mut uncompressed = 0u64;
    for (i, &start) in offsets.iter().enumerate() {
        log.append(FrameStart {
            uncompressed,
            compressed: start,
        })?;
        let end = offsets.get(i + 1).copied().unwrap_or(usable_end);
        src.seek(SeekFrom::Start(start))?;
        let mut frame = (&mut src).take(end - start);
        let mut decoder = zstd::stream::Decoder::new(&mut frame)?;
        uncompressed += std::io::copy(&mut decoder, &mut std::io::sink())?;
    }
    drop(log);
    std::fs::rename(&tmp, &final_path)?;
    Ok(offsets.len())
}

/// What [`repair_archive`] managed to save.
#[derive(Debug, Clone, Default)]
pub struct RepairReport {
    /// Structural scan the repair was based on.
    pub scan: ScanReport,
    /// Complete JSON lines written to the repaired archive.
    pub lines: u64,
    /// Decompressed bytes written.
    pub bytes: u64,
    /// Decompressed bytes dropped as an incomplete trailing line at a fault.
    pub dropped: u64,
    /// Where the damaged original was moved, when it was replaced.
    pub original: Option<PathBuf>,
}

/// Salvage a damaged zstd archive into a clean, framed one.
///
/// A corrupt block header destroys the *remainder of its frame* - zstd has no
/// intra-frame resync - but the frames after it are independent and usually
/// fine. This walks every frame the scanner can find, decodes each as far as
/// it goes, keeps only whole JSON lines, and rewrites the survivors as a
/// normal bounded-frame archive with a matching sidecar.
///
/// **Offsets change.** Salvage drops the unreadable regions, so decompressed
/// positions shift and any existing index entries for this shard are stale -
/// re-index the shard afterwards. Event *ids* are unaffected, so re-indexing
/// simply re-records them.
///
/// The damaged file is kept as `<path>.corrupt` rather than deleted; nothing
/// here is clever enough to be trusted with the only copy.
///
/// Returns `Ok(None)` when the archive is structurally intact (nothing to do).
pub fn repair_archive(path: &Path, frame_target: u64) -> Result<Option<RepairReport>> {
    // Held until the swap is done: see `lock_shard_standalone`.
    let lock = lock_shard_standalone(path)?;
    let len = lock.metadata()?.len();
    if len == 0 {
        return Ok(None);
    }
    let report = {
        let mut src = BufReader::new(File::open(path)?);
        crate::database::frames::scan_zstd_frames(&mut src, len)?
    };
    if report.is_clean() {
        return Ok(None);
    }
    for d in &report.damage {
        warn!(
            "{}: damage at offset {} ({}), resync at {}",
            path.display(),
            d.offset,
            d.reason,
            d.resync
                .map(|r| r.to_string())
                .unwrap_or_else(|| "end of file".into()),
        );
    }

    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".repair.tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(sidecar_path(&tmp));

    let starts = {
        let mut src = BufReader::new(File::open(path)?);
        crate::database::frames::scan_zstd_frame_starts(&mut src, len)?
    };
    let mut salvage = SalvageReader::open(path, starts, len)?;
    write_framed(&mut salvage, &tmp, frame_target)?;
    let (lines, bytes, dropped) = salvage.stats();

    let mut corrupt = path.as_os_str().to_os_string();
    corrupt.push(".corrupt");
    let corrupt = PathBuf::from(corrupt);
    std::fs::rename(path, &corrupt)?;
    let _ = std::fs::remove_file(sidecar_path(path));
    std::fs::rename(sidecar_path(&tmp), sidecar_path(path))?;
    std::fs::rename(&tmp, path)?;
    drop(lock);

    Ok(Some(RepairReport {
        scan: report,
        lines,
        bytes,
        dropped,
        original: Some(corrupt),
    }))
}

/// Counts the compressed bytes a decoder actually *consumes*, so salvage knows
/// exactly how far into the file a frame reached.
///
/// Counting `read` calls instead would count the buffer's read-ahead too, and
/// overshoot by up to a buffer - enough to skip the next real frame. `consume`
/// is the decoder saying "I used these bytes", which is the number we want.
struct Counted {
    inner: BufReader<File>,
    consumed: u64,
}

impl Read for Counted {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = {
            let src = self.fill_buf()?;
            let n = src.len().min(buf.len());
            buf[..n].copy_from_slice(&src[..n]);
            n
        };
        self.consume(n);
        Ok(n)
    }
}

impl std::io::BufRead for Counted {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.consumed += amt as u64;
        self.inner.consume(amt);
    }
}

/// Reader over the salvageable content of a damaged archive, emitting only
/// whole JSON lines.
///
/// Each candidate frame is decoded as far as zstd will go - to the end of the
/// file if the frame is healthy, to the corruption otherwise. Both ends of
/// every region are trimmed to line boundaries:
///
/// * the **tail**, because damage cuts the last line in half;
/// * the **head**, because a frame boundary is a *byte* boundary - a frame
///   picked up after a resync almost always starts mid-event.
///
/// Missing the head trim is silent: the archive still passes `zstd -t`, and
/// the fragment only surfaces later as an indexer that stops at that line and
/// drops the rest of the shard.
///
/// Lines that are not a JSON object are dropped too, which is what keeps a
/// coincidental frame magic - decoding to plausible-looking noise - from
/// putting garbage in the repaired archive.
///
/// Candidates that start inside a region an earlier decode already covered are
/// skipped: that both avoids re-emitting the same events and keeps the work
/// near one pass over the file instead of one pass per candidate.
struct SalvageReader {
    starts: std::vec::IntoIter<u64>,
    len: u64,
    /// Compressed offset the last successful decode reached.
    covered_to: u64,
    decoder: Option<zstd::stream::Decoder<'static, Counted>>,
    /// Where the frame being decoded starts.
    frame_start: u64,
    path: PathBuf,
    /// Whole lines waiting to be read out.
    ready: Vec<u8>,
    ready_pos: usize,
    /// Output after the last newline seen in the current frame.
    held: Vec<u8>,
    /// Waiting to drop this region's leading partial line.
    skip_head: bool,
    scratch: Vec<u8>,
    lines: u64,
    bytes: u64,
    dropped: u64,
}

impl SalvageReader {
    fn open(path: &Path, starts: Vec<u64>, len: u64) -> Result<Self> {
        Ok(Self {
            starts: starts.into_iter(),
            len,
            covered_to: 0,
            decoder: None,
            frame_start: 0,
            path: path.to_path_buf(),
            ready: Vec::new(),
            ready_pos: 0,
            held: Vec::new(),
            skip_head: false,
            scratch: vec![0u8; 256 * 1024],
            lines: 0,
            bytes: 0,
            dropped: 0,
        })
    }

    /// Queue the whole lines in `held`, keeping only JSON objects.
    fn emit_lines(&mut self, cut: usize) {
        let chunk: Vec<u8> = self.held.drain(..=cut).collect();
        for line in chunk.split_inclusive(|&b| b == b'\n') {
            let body = line.strip_suffix(b"\n").unwrap_or(line).trim_ascii();
            if body.first() == Some(&b'{') && body.last() == Some(&b'}') {
                self.ready.extend_from_slice(line);
                self.lines += 1;
            } else {
                self.dropped += line.len() as u64;
            }
        }
    }

    fn stats(&self) -> (u64, u64, u64) {
        (self.lines, self.bytes, self.dropped)
    }

    /// Drop `held` once it is too long to be a line, returning whether it did.
    ///
    /// Bounds the salvage buffer: see [`MAX_HELD`].
    fn discard_overlong(&mut self) -> bool {
        if self.held.len() <= MAX_HELD {
            return false;
        }
        warn!(
            "{}: {} bytes with no line boundary at frame {}; dropping (not JSONL)",
            self.path.display(),
            self.held.len(),
            self.frame_start
        );
        self.dropped += self.held.len() as u64;
        self.held.clear();
        true
    }

    /// Finish with the current frame, discarding any half-line it ended on and
    /// recording how far it got.
    fn end_frame(&mut self) {
        if let Some(d) = self.decoder.take() {
            let consumed = d.finish().consumed;
            self.covered_to = self.covered_to.max(self.frame_start + consumed);
        }
        self.dropped += self.held.len() as u64;
        self.held.clear();
    }
}

/// Longest run without a newline that can still plausibly be part of a line.
///
/// Salvage decodes damaged frames, and a damaged frame can decode to a long
/// stretch of binary garbage containing no newline at all. `held` buffers until
/// it sees one, so without a ceiling a single damaged frame buffers its entire
/// decoded output into memory -- which on a 149 GB shard is gigabytes of anon
/// growing at decode speed until the process is OOM-killed.
///
/// No JSONL event line is remotely this long, so a run this size is not a line:
/// drop it and resynchronise at the next newline.
const MAX_HELD: usize = 64 * 1024 * 1024;

impl Read for SalvageReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.ready_pos < self.ready.len() {
                let n = (self.ready.len() - self.ready_pos).min(out.len());
                out[..n].copy_from_slice(&self.ready[self.ready_pos..self.ready_pos + n]);
                self.ready_pos += n;
                self.bytes += n as u64;
                return Ok(n);
            }
            self.ready.clear();
            self.ready_pos = 0;

            if self.decoder.is_none() {
                let start = loop {
                    let Some(start) = self.starts.next() else {
                        return Ok(0);
                    };
                    if start >= self.covered_to && start < self.len {
                        break start;
                    }
                };
                let mut f = File::open(&self.path)?;
                f.seek(SeekFrom::Start(start))?;
                self.frame_start = start;
                // Only a frame at byte 0 is guaranteed to begin an event.
                self.skip_head = start != 0;
                let counted = Counted {
                    inner: BufReader::new(f),
                    consumed: 0,
                };
                match zstd::stream::Decoder::with_buffer(counted) {
                    Ok(d) => self.decoder = Some(d),
                    // Not a frame after all (coincidental magic).
                    Err(_) => continue,
                }
            }

            let decoder = self.decoder.as_mut().expect("decoder present");
            match decoder.read(&mut self.scratch) {
                Ok(0) => {
                    self.end_frame();
                    continue;
                }
                Err(e) => {
                    // Expected at every damage point: keep what decoded.
                    warn!(
                        "{}: frame at {} ended early: {e}",
                        self.path.display(),
                        self.frame_start
                    );
                    self.end_frame();
                    continue;
                }
                Ok(n) => {
                    // Everything already in `held` has been scanned and holds no
                    // newline, so only the newly decoded bytes can introduce
                    // one. Rescanning the whole buffer on every read makes a
                    // long newline-free run quadratic in its length.
                    let mut search_from = self.held.len();
                    self.held.extend_from_slice(&self.scratch[..n]);

                    if self.skip_head {
                        // Everything up to the first newline belongs to an
                        // event whose start went with the previous frame.
                        match self.held[search_from..].iter().position(|&b| b == b'\n') {
                            Some(rel) => {
                                let nl = search_from + rel;
                                self.dropped += nl as u64 + 1;
                                self.held.drain(..=nl);
                                self.skip_head = false;
                                // What survives the drain is only ever the tail
                                // of this read, so scanning it whole is cheap.
                                search_from = 0;
                            }
                            None => {
                                // Still no newline to resynchronise on. Keep
                                // discarding rather than buffering garbage.
                                self.discard_overlong();
                                continue;
                            }
                        }
                    }

                    match self.held[search_from..].iter().rposition(|&b| b == b'\n') {
                        Some(rel) => self.emit_lines(search_from + rel),
                        // No line boundary yet - keep buffering, but only up to
                        // a plausible line length (see MAX_HELD).
                        None => {
                            if self.discard_overlong() {
                                // Mid-garbage: whatever follows up to the next
                                // newline is the tail of something unusable.
                                self.skip_head = true;
                            }
                            continue;
                        }
                    }
                }
            }
        }
    }
}

/// Convert any supported archive (`.gz`, `.bz2`, plain `.jsonl`, or a
/// single-frame `.zst`) into a **framed** zstd archive that lookups can seek
/// into, writing `<stem>.jsonl.zst` beside it plus its frame sidecar.
///
/// gzip and bzip2 cannot be seeked at all, so an event in a 550 GB `.gz` costs
/// a full decompression to read. Converting is the only way to make such an
/// import usable.
///
/// The decompressed bytes are preserved exactly, so the file is the same
/// JSON-L stream, just packaged differently. The shard id is derived from the
/// file *name*, which changes here, so convert **before** indexing - or
/// re-index afterwards, which self-heals because the same event ids are simply
/// re-recorded against the new shard.
///
/// The original is left in place; delete or move it once you are satisfied,
/// otherwise both copies get indexed (harmless, but wasted work).
/// Returns the path written.
pub fn convert_archive_to_zst(path: &Path, frame_target: u64) -> Result<PathBuf> {
    let stem = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("{}: no file name", path.display()))?;
    // Strip the compression extension, keeping any `.jsonl`.
    let base = stem
        .strip_suffix(".gz")
        .or_else(|| stem.strip_suffix(".bz2"))
        .or_else(|| stem.strip_suffix(".zst"))
        .or_else(|| stem.strip_suffix(".zstd"))
        .unwrap_or(stem);
    let base = if base.ends_with(".jsonl") || base.ends_with(".json") {
        base.to_string()
    } else {
        format!("{base}.jsonl")
    };
    let out_path = path.with_file_name(format!("{base}.zst"));
    if out_path == path {
        // Already a .zst: just fix its framing in place.
        reframe_archive(path, frame_target)?;
        return Ok(out_path);
    }
    if out_path.exists() {
        bail!("{} already exists", out_path.display());
    }

    let mut src: Box<dyn Read> = match path.extension().and_then(|e| e.to_str()) {
        #[cfg(feature = "sync")]
        Some("gz") => Box::new(flate2::read::GzDecoder::new(File::open(path)?)),
        #[cfg(feature = "sync")]
        Some("bz2") => Box::new(bzip2::read::BzDecoder::new(File::open(path)?)),
        #[cfg(not(feature = "sync"))]
        Some(ext @ ("gz" | "bz2")) => {
            bail!("converting {ext} archives needs the `sync` feature")
        }
        Some("zst") | Some("zstd") => Box::new(zstd::stream::Decoder::new(File::open(path)?)?),
        _ => Box::new(File::open(path)?),
    };

    let mut tmp = out_path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(sidecar_path(&tmp));

    let (_, truncated) = write_framed(&mut src, &tmp, frame_target)?;
    if truncated {
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(sidecar_path(&tmp));
        bail!(
            "{}: source is damaged and decoded only partially; \
             repair it first rather than converting away the rest",
            path.display()
        );
    }

    std::fs::rename(sidecar_path(&tmp), sidecar_path(&out_path))?;
    std::fs::rename(&tmp, &out_path)?;
    Ok(out_path)
}

/// Stream `src` into `dst` as concatenated zstd frames of at most
/// `frame_target` uncompressed bytes each, recording every boundary in
/// `dst`'s sidecar. Shared by conversion, reframing and repair.
///
/// Returns the frame count and whether the source ended early - a decode error
/// partway through, i.e. the input is damaged and `dst` holds only its
/// readable prefix. Callers that rename `dst` over the original **must** check
/// that flag: doing the rename anyway turns "damaged shard" into "deleted
/// events".
fn write_framed(src: &mut dyn Read, dst: &Path, frame_target: u64) -> Result<(usize, bool)> {
    let frame_target = frame_target.max(1);
    let level = 3;
    let mut out = Some(File::create(dst)?);
    let mut encoder: Option<Encoder<'static, File>> = None;
    let mut log = FrameLog::open(dst)?;

    let mut buf = vec![0u8; 256 * 1024];
    let mut uncompressed = 0u64;
    let mut frames = 0usize;
    let mut frame_bytes = 0u64;
    let mut truncated = false;

    loop {
        let read = match src.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                // Truncated or still-open final frame: keep what decoded.
                warn!("{}: stopping at {uncompressed} bytes: {e}", dst.display());
                truncated = true;
                break;
            }
        };

        let mut written = 0usize;
        while written < read {
            if let Some(mut file) = out.take() {
                let compressed = file.seek(SeekFrom::End(0))?;
                log.append(FrameStart {
                    uncompressed,
                    compressed,
                })?;
                frames += 1;
                frame_bytes = 0;
                encoder = Some(new_encoder(file, level)?);
            }
            let room = (frame_target - frame_bytes) as usize;
            let take = room.min(read - written);
            let enc = encoder.as_mut().expect("encoder present");
            enc.write_all(&buf[written..written + take])?;
            written += take;
            frame_bytes += take as u64;
            uncompressed += take as u64;
            if frame_bytes >= frame_target {
                out = Some(encoder.take().expect("encoder present").finish()?);
            }
        }
    }
    if let Some(enc) = encoder.take() {
        out = Some(enc.finish()?);
    }
    if let Some(mut file) = out.take() {
        file.flush()?;
    }
    drop(log);
    Ok((frames, truncated))
}

/// Rewrite a zstd archive into bounded frames, so lookups into it can seek.
///
/// **The decompressed bytes are unchanged**, so any offsets already stored in
/// the index stay valid - only the compressed framing changes. This is the fix
/// for an archive imported from elsewhere as one giant frame, where every
/// lookup would otherwise decode from the start of the file.
///
/// The new archive is written beside the original and renamed into place with
/// its sidecar, so a crash leaves the original intact. Returns the frame count.
///
/// Only `.zst` archives are handled: converting `.gz`/`.bz2` would change the
/// file name, and the shard id is derived from that name.
pub fn reframe_archive(path: &Path, frame_target: u64) -> Result<usize> {
    let lock = lock_shard_standalone(path)?;
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".reframe.tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(sidecar_path(&tmp));

    let mut src = zstd::stream::Decoder::new(File::open(path)?)?;
    let (frames, truncated) = write_framed(&mut src, &tmp, frame_target)?;
    // Renaming a partial decode over the original would silently delete every
    // event after the damage. Leave the shard alone; `repair_archive` is the
    // path that handles this, and it keeps the original.
    if truncated {
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(sidecar_path(&tmp));
        bail!(
            "{}: damaged, decoded only {frames} frame(s) before failing; \
             repair it instead of reframing",
            path.display()
        );
    }

    std::fs::rename(sidecar_path(&tmp), sidecar_path(path))?;
    std::fs::rename(&tmp, path)?;
    drop(lock);
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::frames::FrameTable;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Ev {
        id: String,
        n: u64,
    }

    fn ev(n: u64) -> Ev {
        Ev {
            id: format!("{n:064x}"),
            n,
        }
    }

    /// Decode the whole archive and return the raw decompressed bytes.
    fn decode_all(path: &Path) -> Vec<u8> {
        let f = File::open(path).unwrap();
        let mut out = Vec::new();
        zstd::stream::Decoder::new(f)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn offsets_point_at_the_right_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        let mut locs = Vec::new();
        for n in 0..500u64 {
            locs.push(w.write_event(&ev(n)).unwrap());
        }
        w.finish().unwrap();

        let all = decode_all(&path);
        for (n, loc) in locs.iter().enumerate() {
            let line = &all[loc.offset as usize..loc.offset as usize + loc.len as usize];
            let v: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), n as u64);
        }
    }

    #[test]
    fn small_frame_target_produces_many_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..500u64 {
            w.write_event(&ev(n)).unwrap();
        }
        let total = w.uncompressed_len();
        w.finish().unwrap();

        let t = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert!(t.len() > 5, "expected multiple frames, got {}", t.len());
        let starts = t.starts();
        // Every recorded boundary must be a real zstd frame start.
        let mut data = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut data).unwrap();
        let real = crate::database::frames::scan_zstd_frame_offsets(&data).unwrap();
        for s in &starts {
            assert!(
                real.contains(&s.compressed),
                "boundary {} is not a zstd frame start",
                s.compressed
            );
        }
        assert_eq!(decode_all(&path).len() as u64, total);
    }

    #[test]
    fn reopening_continues_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..100u64 {
            w.write_event(&ev(n)).unwrap();
        }
        let len_before = w.uncompressed_len();
        w.finish().unwrap();

        // Reopen (process restart, same day) and keep appending.
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        assert_eq!(
            w.uncompressed_len(),
            len_before,
            "resume must continue the logical stream"
        );
        let mut locs = Vec::new();
        for n in 100..200u64 {
            locs.push(w.write_event(&ev(n)).unwrap());
        }
        w.finish().unwrap();

        let all = decode_all(&path);
        for (i, loc) in locs.iter().enumerate() {
            let line = &all[loc.offset as usize..loc.offset as usize + loc.len as usize];
            let v: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), 100 + i as u64);
        }
    }

    #[test]
    fn resume_without_sidecar_recovers_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..100u64 {
            w.write_event(&ev(n)).unwrap();
        }
        let len_before = w.uncompressed_len();
        w.finish().unwrap();
        std::fs::remove_file(sidecar_path(&path)).unwrap();

        let w = CompressedJsonLFile::new(&path).unwrap();
        assert_eq!(w.uncompressed_len(), len_before);
    }

    #[test]
    fn reframe_preserves_bytes_and_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import.jsonl.zst");

        // An archive imported as one giant frame (what `zstd file` produces).
        let mut raw = Vec::new();
        let mut locs = Vec::new();
        for n in 0..2000u64 {
            let line = serde_json::to_vec(&ev(n)).unwrap();
            locs.push((raw.len() as u64, line.len() as u32));
            raw.extend_from_slice(&line);
            raw.push(b'\n');
        }
        std::fs::write(&path, zstd::encode_all(raw.as_slice(), 3).unwrap()).unwrap();
        assert_eq!(
            crate::database::frames::scan_zstd_frame_offsets(&std::fs::read(&path).unwrap())
                .unwrap()
                .len(),
            1,
            "fixture should be a single frame"
        );

        let frames = reframe_archive(&path, 4096).unwrap();
        assert!(frames > 5, "expected many frames, got {frames}");

        // Decompressed bytes identical => previously indexed offsets still valid.
        assert_eq!(decode_all(&path), raw, "reframing must not change content");

        let table = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert_eq!(table.len(), frames);

        let pool = crate::ShardReaderPool::new();
        for (n, (offset, len)) in locs.iter().enumerate() {
            let got = pool.read_zstd_range(&path, *offset, *len).unwrap();
            let v: serde_json::Value = serde_json::from_slice(&got).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), n as u64);
        }
    }

    #[test]
    fn rebuild_sidecar_matches_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..300u64 {
            w.write_event(&ev(n)).unwrap();
        }
        w.finish().unwrap();
        let original = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();

        std::fs::remove_file(sidecar_path(&path)).unwrap();
        let n = rebuild_frame_index(&path).unwrap();
        let rebuilt = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert_eq!(n, rebuilt.len());
        assert_eq!(original.starts(), rebuilt.starts());
    }

    /// A non-monotonic sidecar (residue of a crash that interleaved two
    /// writers) must not be a fatal panic on open: the sidecar is derived data,
    /// so opening drops it, rebuilds it from the shard's frames, and resumes.
    ///
    /// Regression for the ingest segfault where `CompressedJsonLFile::
    /// with_frame_target` propagated a "non-monotonic frame index" error up to
    /// a `.expect()` on the RocksDB writer thread, unwinding across the C FFI
    /// boundary and killing the process with SIGSEGV (exit 139).
    #[test]
    fn a_non_monotonic_sidecar_is_rebuilt_not_panicked_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        write_shard(&path, 500, 1024);
        let table = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert!(table.len() > 5, "multi-frame shard");

        // Corrupt the sidecar the way a torn crash-window leaves it: duplicate
        // the final boundary, which makes the last two records non-monotonic
        // (a boundary can't repeat) and so `FrameTable::load` rejects the file.
        let mut raw = std::fs::read(sidecar_path(&path)).unwrap();
        let n = table.len();
        let last = table.get(n - 1).unwrap();
        let mut dup = [0u8; 16];
        dup[..8].copy_from_slice(&last.uncompressed.to_le_bytes());
        dup[8..].copy_from_slice(&last.compressed.to_le_bytes());
        raw.extend_from_slice(&dup);
        std::fs::write(sidecar_path(&path), &raw).unwrap();

        // Before the fix, this bailed out and the caller's `.expect` panicked.
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        // The reopened writer appends after the last frame and stays valid.
        w.write_event(&ev(999)).unwrap();
        w.finish().unwrap();

        // The rebuilt sidecar is loadable and its frame count is sane.
        let again = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert!(again.len() > 0);
        // Resuming after the rebuilt sidecar still decodes all the events.
        assert!(events_in(&path).contains(&999));
    }

    /// Opening a shard without writing to it must not corrupt its sidecar.
    ///
    /// Every open records the boundary of the frame it is about to write. A
    /// process that opened the shard and died before writing an event (a crash
    /// loop) would otherwise append that same boundary again on the next open,
    /// leaving a duplicate -- a non-monotonic sidecar that the next start then
    /// refuses to load. The crash loop manufactured its own poison.
    #[test]
    fn reopening_without_writing_does_not_corrupt_the_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        write_shard(&path, 200, 1024);

        // The first reopen legitimately records one boundary: `finish` closed
        // the encoder, so the file now extends past the last recorded frame
        // start and the frame about to be opened begins at EOF.
        drop(CompressedJsonLFile::with_frame_target(&path, 1024).unwrap());
        let settled = FrameTable::load(&sidecar_path(&path))
            .expect("loadable after first reopen")
            .unwrap();

        // Ten more open/drop cycles with no events written, as a crash loop
        // does. None of them may add a boundary, and the sidecar must stay
        // loadable -- becoming unloadable is the bug.
        for i in 0..10 {
            drop(CompressedJsonLFile::with_frame_target(&path, 1024).unwrap());
            let now = FrameTable::load(&sidecar_path(&path))
                .unwrap_or_else(|e| panic!("sidecar corrupt after reopen {i}: {e}"))
                .unwrap();
            assert_eq!(
                settled.starts(),
                now.starts(),
                "reopen {i} appended a boundary without writing anything"
            );
        }

        // And the shard is still writable and readable afterwards.
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        w.write_event(&ev(1234)).unwrap();
        w.finish().unwrap();
        assert!(events_in(&path).contains(&1234));
        FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
    }

    /// Salvaging a frame that decodes to a long run with no newline must not
    /// buffer it all in memory.
    ///
    /// A damaged shard can decode to gigabytes of binary garbage containing no
    /// line boundary. `held` buffers until it sees one, so without a ceiling a
    /// single frame grows anon memory at decode speed until the process is
    /// OOM-killed -- observed in production as ~65 MB/s of linear growth that
    /// ate a 16 GiB budget in four minutes.
    #[test]
    fn salvage_does_not_buffer_a_newline_free_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.jsonl.zst");

        // One frame holding well over MAX_HELD bytes without a single newline.
        let junk = vec![b'x'; MAX_HELD + (8 << 20)];
        let mut enc = zstd::stream::Encoder::new(File::create(&path).unwrap(), 1).unwrap();
        enc.write_all(&junk).unwrap();
        // Then a real event, so we can prove salvage resynchronises.
        enc.write_all(b"\n").unwrap();
        enc.write_all(serde_json::to_string(&ev(7)).unwrap().as_bytes())
            .unwrap();
        enc.write_all(b"\n").unwrap();
        enc.finish().unwrap().flush().unwrap();

        let len = std::fs::metadata(&path).unwrap().len();
        let mut r = SalvageReader::open(&path, vec![0], len).unwrap();

        // Drain it fully; `held` must never exceed the ceiling (plus one read).
        let mut out = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = r.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            assert!(
                r.held.len() <= MAX_HELD + 256 * 1024,
                "held grew unbounded: {} bytes",
                r.held.len()
            );
        }

        // The garbage was dropped, and the real event after it survived.
        let (_, _, dropped) = r.stats();
        assert!(dropped as usize >= MAX_HELD, "garbage should be dropped");
        let text = String::from_utf8_lossy(&out);
        assert!(
            text.contains("\"n\":7"),
            "salvage must resynchronise and keep the event after the garbage, got {} bytes",
            out.len()
        );
    }

    /// A shard whose frames are empty decompresses to nothing, so consecutive
    /// boundaries share an `uncompressed` offset. That is legitimate, and
    /// rejecting it was unrecoverable: rebuilding regenerates the identical
    /// table, so the shard could never be opened again and the writer dropped
    /// every event routed to it.
    #[test]
    fn empty_frames_are_a_valid_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.jsonl.zst");

        // Two empty zstd frames back to back: no decompressed bytes at all.
        let mut f = File::create(&path).unwrap();
        for _ in 0..2 {
            let enc = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
            f.write_all(&enc.finish().unwrap()).unwrap();
        }
        f.flush().unwrap();
        drop(f);

        let n = rebuild_frame_index(&path).unwrap();
        assert!(n >= 1, "frames should be found");
        let table = FrameTable::load(&sidecar_path(&path))
            .expect("a sidecar of empty frames must load")
            .unwrap();
        // Same uncompressed offset, advancing compressed offset.
        if table.len() >= 2 {
            let (a, b) = (table.get(0).unwrap(), table.get(1).unwrap());
            assert_eq!(a.uncompressed, b.uncompressed, "empty frames add no bytes");
            assert!(b.compressed > a.compressed, "but do advance in the file");
        }

        // And the writer can open it -- the bug was that it never could again.
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        w.write_event(&ev(5)).unwrap();
        w.finish().unwrap();
        assert!(events_in(&path).contains(&5));
        FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
    }

    /// A multi-frame archive plus the events it holds.
    fn write_shard(path: &Path, count: u64, frame_target: u64) -> Vec<u64> {
        let mut w = CompressedJsonLFile::with_frame_target(path, frame_target).unwrap();
        for n in 0..count {
            w.write_event(&ev(n)).unwrap();
        }
        w.finish().unwrap();
        (0..count).collect()
    }

    fn events_in(path: &Path) -> Vec<u64> {
        decode_all(path)
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_slice::<serde_json::Value>(l).ok())
            .filter_map(|v| v["n"].as_u64())
            .collect()
    }

    #[test]
    fn a_truncated_tail_still_indexes_its_clean_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        write_shard(&path, 400, 1024);
        let whole = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert!(whole.len() > 5);

        // Kill the process mid-frame: lose the last frame's tail.
        let len = std::fs::metadata(&path).unwrap().len();
        let last = whole.last().unwrap().compressed;
        let cut = last + (len - last) / 2;
        let file = File::options().write(true).open(&path).unwrap();
        file.set_len(cut).unwrap();
        drop(file);
        std::fs::remove_file(sidecar_path(&path)).unwrap();

        // The strict scan gives up on the whole file...
        let mut src = BufReader::new(File::open(&path).unwrap());
        assert!(crate::database::frames::scan_zstd_frame_offsets_reader(&mut src, cut).is_err());

        // ...but the index keeps every frame before the damage.
        let n = rebuild_frame_index(&path).unwrap();
        assert_eq!(n, whole.len() - 1, "all frames but the truncated one");
        let rebuilt = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert_eq!(rebuilt.starts(), whole.starts()[..n]);
    }

    #[test]
    fn repair_salvages_frames_around_the_damage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        write_shard(&path, 600, 1024);
        let table = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert!(table.len() > 10);

        // Corrupt a block header in the middle: that frame dies, the rest live.
        let victim = table.get(table.len() / 2).unwrap().compressed;
        let mut bytes = std::fs::read(&path).unwrap();
        let hdr = victim as usize + 6;
        bytes[hdr] |= 0b110; // block type 3 (reserved)
        std::fs::write(&path, &bytes).unwrap();

        let report = repair_archive(&path, 1024).unwrap().expect("damage found");
        assert!(!report.scan.damage.is_empty());
        assert!(report.lines > 0);
        assert!(
            std::fs::metadata(report.original.as_ref().unwrap()).is_ok(),
            "original must be kept"
        );

        // The repaired shard is clean, seekable and holds the survivors.
        let len = std::fs::metadata(&path).unwrap().len();
        let mut src = BufReader::new(File::open(&path).unwrap());
        assert!(
            crate::database::frames::scan_zstd_frames(&mut src, len)
                .unwrap()
                .is_clean()
        );
        let survivors = events_in(&path);
        assert_eq!(survivors.len() as u64, report.lines);
        assert!(
            survivors.contains(&0) && survivors.contains(&599),
            "frames on both sides of the damage must survive"
        );
        assert!(
            survivors.len() < 600,
            "the damaged frame's events are genuinely gone"
        );
        // Every salvaged line is a whole event, never half of one.
        assert!(survivors.windows(2).all(|w| w[0] < w[1]), "order preserved");

        // A clean archive reports nothing to do.
        assert!(repair_archive(&path, 1024).unwrap().is_none());
    }

    #[test]
    fn repaired_shard_is_seekable_by_the_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        write_shard(&path, 400, 1024);
        let table = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let hdr = table.get(table.len() / 2).unwrap().compressed as usize + 6;
        bytes[hdr] |= 0b110;
        std::fs::write(&path, &bytes).unwrap();
        repair_archive(&path, 1024).unwrap().unwrap();

        // Offsets moved, so read positions come from the fresh sidecar + data.
        let data = decode_all(&path);
        let pool = crate::ShardReaderPool::new();
        let mut offset = 0u64;
        for line in data.split_inclusive(|&b| b == b'\n') {
            let len = (line.len() - 1) as u32;
            let got = pool.read_zstd_range(&path, offset, len).unwrap();
            assert_eq!(got, &line[..line.len() - 1], "seek at {offset}");
            offset += line.len() as u64;
        }
    }

    #[test]
    fn a_second_writer_is_refused_instead_of_corrupting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut first = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        first.write_event(&ev(0)).unwrap();
        first.flush().unwrap();

        let err = match CompressedJsonLFile::with_frame_target(&path, 1024) {
            Err(e) => e,
            Ok(_) => panic!("a second writer must not be allowed to interleave frames"),
        };
        assert!(err.to_string().contains("another archive writer"), "{err}");

        // Releasing the first hands the shard over cleanly.
        first.finish().unwrap();
        let mut second = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        second.write_event(&ev(1)).unwrap();
        second.finish().unwrap();
        assert_eq!(events_in(&path), vec![0, 1]);
    }

    #[test]
    fn reframing_a_damaged_shard_refuses_instead_of_truncating_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        write_shard(&path, 600, 1024);
        let table = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        let hdr = table.get(table.len() / 2).unwrap().compressed as usize + 6;
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[hdr] |= 0b110;
        std::fs::write(&path, &bytes).unwrap();

        // Reframing decodes until the damage. Writing that prefix over the
        // original would delete every event after it.
        assert!(reframe_archive(&path, 4096).is_err());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "shard must be untouched"
        );

        // Repair is the path that handles damage, and it keeps the original.
        let report = repair_archive(&path, 1024).unwrap().unwrap();
        let survivors = events_in(&path);
        assert_eq!(survivors.len() as u64, report.lines);
        assert!(survivors.contains(&599), "events past the damage survive");
    }

    #[test]
    fn maintenance_refuses_to_swap_a_live_shard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..200u64 {
            w.write_event(&ev(n)).unwrap();
        }
        w.flush().unwrap();

        assert!(
            reframe_archive(&path, 4096).is_err(),
            "reframing under a live writer would orphan its descriptor"
        );
        assert!(repair_archive(&path, 4096).is_err());

        w.finish().unwrap();
        assert!(reframe_archive(&path, 4096).is_ok());
    }
}
