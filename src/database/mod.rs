use anyhow::{Result, anyhow};
use chrono::{DateTime, NaiveDate, Utc};
use dashmap::DashMap;
use log::{debug, warn};
use nostr_sdk::prelude::{
    Backend, BoxedFuture, DatabaseError, DatabaseEventStatus, Events, NostrDatabase,
    RejectedReason, SaveEventStatus,
};
use nostr_sdk::{Event, EventId, Filter, Timestamp};
use std::fmt::{Debug, Formatter};
use std::fs::create_dir_all;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod file;
pub mod frames;
pub use frames::{FrameSpan, FrameStart, FrameTable, sidecar_path};
mod pool;
mod value;
pub use file::*;
pub use pool::*;
pub use value::*;
mod rocksdb;
#[cfg(feature = "db-rocksdb")]
pub use crate::database::rocksdb::*;

/// KV index database for tracking event ids + timestamps
pub trait IndexDb: Clone + Send + Sync {
    /// List (event id, created_at) pairs with `min <= created_at <= max`.
    /// May return an empty list until [`time_index_ready`](Self::time_index_ready).
    fn list_ids(&self, min: u64, max: u64) -> Vec<([u8; 32], u64)>;
    /// Whether time-ranged [`list_ids`](Self::list_ids) queries are servable.
    /// Backends that build a time index in the background return `false`
    /// until the index covers all existing entries.
    fn time_index_ready(&self) -> bool {
        true
    }
    fn count_keys(&self) -> u64;
    fn contains_key(&self, id: &[u8; 32]) -> Result<bool>;
    fn is_index_empty(&self) -> bool;
    /// Reconfigure the database for faster bulk loading
    fn setup_for_reindex(&mut self) -> Result<()>;
    fn insert(&self, k: [u8; 32], v: IndexEntry) -> Result<()>;
    /// Insert a batch, returning how many keys were **new**.
    ///
    /// Re-inserting an existing id is an idempotent overwrite, so the count
    /// must not grow for it - that is what used to make the cached event count
    /// drift and require an O(n) [`repair_count`](Self::repair_count) after
    /// every reindex.
    fn insert_batch(&self, items: Vec<([u8; 32], IndexEntry)>) -> Result<usize>;
    /// Look up a single entry (timestamp + location, when known).
    fn get(&self, id: &[u8; 32]) -> Result<Option<IndexEntry>>;
    /// Read an indexer bookkeeping value (per-shard state). Stored apart from
    /// event keys so it can never be mistaken for one.
    fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Write an indexer bookkeeping value.
    fn put_meta(&self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Drop all indexer bookkeeping (used by [`wipe`](Self::wipe)).
    fn clear_meta(&self) -> Result<()>;
    /// Look up many entries at once. Backends that support a batched read
    /// (RocksDB `multi_get`) override this; the default just loops.
    fn get_many(&self, ids: &[[u8; 32]]) -> Vec<Option<IndexEntry>> {
        ids.iter().map(|id| self.get(id).ok().flatten()).collect()
    }
    fn wipe(&mut self) -> Result<()>;
    /// Recompute the cached event count from the actual index data and persist it.
    /// Used to repair a stale/incorrect cached count (e.g. after concurrent writers
    /// or a crash overwrote it). Returns the corrected count.
    fn repair_count(&self) -> Result<u64>;
}

/// File information about existing archive files
#[derive(Debug, Clone)]
pub struct ArchiveFile {
    pub path: PathBuf,
    pub size: u64,
    pub created: DateTime<Utc>,
    /// The actual date of the file
    pub timestamp: DateTime<Utc>,
}

/// Compressed JSON-L file database for nostr_sdk
#[derive(Clone)]
pub struct JsonFilesDatabase<D> {
    /// Directory where flat files are contained
    out_dir: PathBuf,
    /// Event id index database
    database: D,
    /// Writer to send events to the file writer thread
    tx_writer: tokio::sync::mpsc::Sender<WriterMsg>,
    /// Pooled random-access readers (fds, decode contexts, frame tables)
    pool: Arc<ShardReaderPool>,
    /// `shard_hash` -> shard path, refreshed by walking `out_dir` on a miss so
    /// archives dropped in by an external backup resolve without a restart.
    shards: Arc<DashMap<u64, PathBuf>>,
    /// Joins the writer thread when the last handle is dropped. Declared
    /// after `tx_writer` so the channel closes (ending the writer loop) before
    /// we wait for it.
    ///
    /// Without this the writer is still compressing and writing to RocksDB
    /// while the process tears down its statics, which aborts at exit.
    #[allow(dead_code)] // held for its Drop impl
    writer: Arc<WriterHandle>,
    /// Uncompressed bytes per zstd frame; also the target used when reframing
    /// an imported archive.
    frame_target: u64,
    /// Ids accepted by `save_event` but not yet indexed by the writer thread.
    /// Keeps dedupe and `check_id` synchronous even though the index write
    /// happens after the event has been written (that is when its offset is
    /// known).
    in_flight: Arc<DashMap<[u8; 32], ()>>,
    /// What to do when the index knows an event exists but not where it is
    /// (a v0 entry, or a shard that moved).
    scan_fallback: ScanFallback,
}

/// How hard to look for an event whose location the index does not know.
///
/// Shards are named after the time they were *written*, which for a live relay
/// is close to the events' `created_at`, but not for a historical import - so
/// [`Day`](ScanFallback::Day) is a cheap guess, not a guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanFallback {
    /// Never scan: unknown location means "not found". Strictly O(1) lookups.
    Off,
    /// Scan the shard named for the event's `created_at` day (default).
    #[default]
    Day,
    /// Scan that day's shard, then every other shard. Correct for archives
    /// imported out of order, but O(archive) per miss - use for migration or
    /// small archives only.
    All,
}

/// What one pass of [`JsonFilesDatabase::index_new_shards`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IndexReport {
    /// Shards found in the archive directory.
    pub shards: usize,
    /// Shards skipped because their size and mtime were unchanged.
    pub unchanged: usize,
    /// Shards read and indexed.
    pub indexed: usize,
    /// Shards rewritten into bounded frames first.
    pub reframed: usize,
    /// Event ids that were not already in the index.
    pub new_events: u64,
}

/// Per-shard indexer bookkeeping: if a shard's size and mtime are unchanged,
/// it has already been indexed and can be skipped.
///
/// Deliberately not a content hash - hashing a 5 GB shard to decide whether to
/// read a 5 GB shard saves nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShardState {
    size: u64,
    mtime: u64,
    events: u64,
}

impl ShardState {
    fn key(rel_name: &str) -> Vec<u8> {
        crate::database::rocksdb::meta_key(rel_name)
    }

    fn of(path: &Path, events: u64) -> Result<Self> {
        let meta = std::fs::metadata(path)?;
        Ok(Self {
            size: meta.len(),
            mtime: meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0),
            events,
        })
    }

    fn encode(&self) -> [u8; 24] {
        let mut b = [0u8; 24];
        b[..8].copy_from_slice(&self.size.to_le_bytes());
        b[8..16].copy_from_slice(&self.mtime.to_le_bytes());
        b[16..].copy_from_slice(&self.events.to_le_bytes());
        b
    }

    fn decode(v: &[u8]) -> Option<Self> {
        if v.len() < 24 {
            return None;
        }
        Some(Self {
            size: u64::from_le_bytes(v[..8].try_into().ok()?),
            mtime: u64::from_le_bytes(v[8..16].try_into().ok()?),
            events: u64::from_le_bytes(v[16..24].try_into().ok()?),
        })
    }

    /// Same bytes on disk as when we indexed it?
    fn matches(&self, other: &Self) -> bool {
        self.size == other.size && self.mtime == other.mtime
    }
}

/// Joins the archive writer thread on drop, so a closing database always
/// leaves a complete zstd frame and a consistent index behind.
struct WriterHandle {
    join: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.join.lock().ok().and_then(|mut j| j.take())
            && handle.join().is_err()
        {
            warn!("archive writer thread panicked");
        }
    }
}

/// [`shard_hash`] of a shard path's file name.
fn shard_hash_of(path: &Path) -> u64 {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(shard_hash)
        .unwrap_or(0)
}

/// Does this JSON line belong to `id`? Guards against a stale/corrupt offset
/// silently returning a different event.
fn line_has_id(line: &[u8], id: &EventId) -> bool {
    let mut needle = Vec::with_capacity(72);
    needle.extend_from_slice(b"\"id\":\"");
    needle.extend_from_slice(id.to_hex().as_bytes());
    needle.push(b'"');
    line.windows(needle.len()).any(|w| w == needle)
}

/// Linear scan of one shard for an event id (fallback path).
fn scan_file_for_id(path: &Path, id: &EventId) -> Option<Vec<u8>> {
    let reader = match crate::database::pool::open_stream_decoder(path) {
        Ok(r) => std::io::BufReader::new(r),
        Err(e) => {
            warn!("{}: {e}", path.display());
            return None;
        }
    };
    let mut reader = reader;
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => return None,
            Ok(_) => {
                while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line_has_id(&line, id) {
                    return Some(std::mem::take(&mut line));
                }
            }
            Err(e) => {
                warn!("{}: scan failed: {e}", path.display());
                return None;
            }
        }
    }
}

impl<D> Debug for JsonFilesDatabase<D> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

/// How many events the writer thread drains before flushing the frame and
/// applying one batched index update.
const WRITER_BATCH: usize = 1000;

/// Work item for the writer thread.
enum WriterMsg {
    Event(Box<Event>),
    /// Acknowledged once every event queued before it has been written *and*
    /// indexed. Used by [`JsonFilesDatabase::flush`].
    Flush(tokio::sync::oneshot::Sender<()>),
}

impl<D> JsonFilesDatabase<D> {
    pub const EVENT_FORMAT: &'static str = "%Y%m%d";

    pub fn get_archive_path(base: &Path, time: &DateTime<Utc>) -> PathBuf {
        base.join(format!(
            "events_{}.jsonl.zst",
            time.format(Self::EVENT_FORMAT)
        ))
    }

    /// Parse the timestamp from the file name
    pub fn parse_timestamp(path: &Path) -> Option<DateTime<Utc>> {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|s| s.split('_').next_back()) // split events_{date}
            .and_then(|s| s.split('.').next()) // remove any more extensions
            .and_then(|s| match NaiveDate::parse_from_str(s, Self::EVENT_FORMAT) {
                Ok(n) => Some(n),
                Err(e) => {
                    warn!("Failed to parse timestamp from {}: {}", path.display(), e);
                    None
                }
            })
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|d| d.and_utc())
    }

    pub async fn list_files(&self) -> Result<Vec<ArchiveFile>> {
        let mut list = tokio::fs::read_dir(&self.out_dir).await?;
        let mut files = Vec::new();
        while let Ok(Some(entry)) = list.next_entry().await {
            if entry.file_type().await?.is_dir() {
                continue;
            }

            let meta = entry.metadata().await?;
            let created_date = meta.created()?.into();
            let parsed_date = if let Some(d) = Self::parse_timestamp(&entry.path()) {
                d
            } else {
                created_date
            };

            files.push(ArchiveFile {
                path: entry.path(),
                size: meta.len(),
                created: created_date,
                timestamp: parsed_date,
            });
        }
        Ok(files)
    }

    /// Return archive file if it exists
    pub fn get_file(&self, path: &str) -> Result<ArchiveFile> {
        let p = self.out_dir.join(&path[1..]);
        if p.exists() && p.is_file() {
            let meta = p.metadata()?;
            let parsed_date = Self::parse_timestamp(&p).ok_or(anyhow!("Filename invalid"))?;
            Ok(ArchiveFile {
                path: p,
                size: meta.len(),
                created: meta.created()?.into(),
                timestamp: parsed_date,
            })
        } else {
            Err(anyhow!("No such file or directory"))
        }
    }
}

impl<D> JsonFilesDatabase<D>
where
    D: IndexDb + 'static,
{
    pub fn new_with_index<P>(dir: P, index: D) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        Self::new_with_index_and_frame_target(dir, index, DEFAULT_FRAME_TARGET)
    }

    /// As [`new_with_index`](Self::new_with_index), with a custom zstd frame
    /// size. Smaller frames make point lookups faster (a lookup decodes from
    /// the frame start) at negligible cost in compression - see
    /// [`DEFAULT_FRAME_TARGET`].
    pub fn new_with_index_and_frame_target<P>(dir: P, index: D, frame_target: u64) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        let dir = PathBuf::from(&dir);
        create_dir_all(&dir)?;

        let in_flight: Arc<DashMap<[u8; 32], ()>> = Arc::new(DashMap::new());

        // Bounded channel applies backpressure to callers (save_event) when the
        // writer falls behind, instead of buffering unboundedly in memory.
        const WRITER_CHANNEL_CAPACITY: usize = 20_000;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WriterMsg>(WRITER_CHANNEL_CAPACITY);
        let dir_writer = dir.clone();
        let index_writer = index.clone();
        let in_flight_writer = in_flight.clone();
        let join = std::thread::Builder::new()
            .name("JsonFilesDatabase::writer".into())
            .spawn(move || {
                // The writer owns the index write: an event's offset only
                // exists once the event has been written, and batching the
                // updates removes the per-event WriteBatch this used to do.
                let mut current_path = Self::get_archive_path(&dir_writer, &Utc::now());
                let mut writer =
                    CompressedJsonLFile::with_frame_target(&current_path, frame_target)
                        .expect("Failed to open archive");
                let mut shard = shard_hash_of(&current_path);
                let mut batch: Vec<([u8; 32], IndexEntry)> = Vec::with_capacity(WRITER_BATCH);

                // blocking_recv sleeps the thread until work arrives (no busy-poll)
                while let Some(first) = rx.blocking_recv() {
                    let mut acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
                    let mut msg = Some(first);
                    while let Some(m) = msg.take() {
                        match m {
                            WriterMsg::Flush(ack) => acks.push(ack),
                            WriterMsg::Event(e) => {
                                // swap files if current path is different
                                let current = Self::get_archive_path(&dir_writer, &Utc::now());
                                if current != current_path {
                                    writer = CompressedJsonLFile::with_frame_target(
                                        &current,
                                        frame_target,
                                    )
                                    .expect("Failed to open archive");
                                    shard = shard_hash_of(&current);
                                    current_path = current;
                                }

                                let loc = writer
                                    .write_event(&e)
                                    .expect("Failed to write event to archive");
                                batch.push((
                                    *e.id.as_bytes(),
                                    IndexEntry::located(
                                        e.created_at.as_secs(),
                                        EventLoc { shard, ..loc },
                                    ),
                                ));
                            }
                        }

                        if batch.len() < WRITER_BATCH {
                            msg = rx.try_recv().ok();
                        }
                    }

                    // Flush before indexing: an index entry must never point
                    // at bytes a reader cannot decode yet. This is a zstd
                    // block flush, so frames still grow to frame_target.
                    if let Err(e) = writer.flush() {
                        warn!("Failed to flush archive: {e}");
                    }
                    let ids: Vec<[u8; 32]> = batch.iter().map(|(k, _)| *k).collect();
                    if let Err(e) = index_writer
                        .insert_batch(std::mem::take(&mut batch))
                        .map(|_| ())
                    {
                        warn!("Failed to apply index update: {e}");
                    }
                    for id in ids {
                        in_flight_writer.remove(&id);
                    }
                    // Acked only after the index update, so a caller that has
                    // awaited flush() sees its events in count_keys/list_ids.
                    for ack in acks {
                        let _ = ack.send(());
                    }
                }
            })
            .map_err(|e| anyhow!("failed to start archive writer: {e}"))?;
        Ok(Self {
            out_dir: dir,
            database: index,
            tx_writer: tx,
            writer: Arc::new(WriterHandle {
                join: std::sync::Mutex::new(Some(join)),
            }),
            pool: Arc::new(ShardReaderPool::new()),
            shards: Arc::new(DashMap::new()),
            frame_target,
            in_flight,
            scan_fallback: ScanFallback::default(),
        })
    }

    /// Wait until every event queued so far has been written to its archive
    /// and recorded in the index.
    ///
    /// `save_event` returns as soon as the event is queued (the writer thread
    /// assigns its offset), so callers that need read-after-write - counts,
    /// `list_ids`, lookups - await this first.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx_writer
            .send(WriterMsg::Flush(tx))
            .await
            .map_err(|e| anyhow!("writer thread is gone: {e}"))?;
        rx.await.map_err(|e| anyhow!("writer thread is gone: {e}"))
    }

    /// How to handle events whose location the index does not know
    /// (see [`ScanFallback`]). Defaults to [`ScanFallback::Day`].
    pub fn with_scan_fallback(mut self, mode: ScanFallback) -> Self {
        self.scan_fallback = mode;
        self
    }

    /// List (event id, created_at) pairs with `since <= created_at <= until`
    /// (inclusive bounds). Returns an empty list while the backend's time
    /// index is still being built — callers should treat that as "no local
    /// knowledge", not as "no events".
    pub fn list_ids(&self, since: u64, until: u64) -> Vec<(EventId, Timestamp)> {
        if !self.database.time_index_ready() {
            return Vec::new();
        }
        self.database
            .list_ids(since, until)
            .into_iter()
            .filter_map(|(k, v)| Some((EventId::from_slice(&k).ok()?, Timestamp::from_secs(v))))
            .collect()
    }

    /// Resolve a [`shard_hash`] back to a shard path.
    ///
    /// There is no persisted registry: the mapping is recomputed by hashing
    /// the names of the files in `out_dir`, so an archive dropped in by an
    /// external relay backup resolves on the next refresh, and a wiped/rebuilt
    /// index still agrees with the ids already stored.
    pub fn shard_path(&self, shard: u64) -> Option<PathBuf> {
        if let Some(p) = self.shards.get(&shard) {
            return Some(p.clone());
        }
        self.refresh_shards();
        self.shards.get(&shard).map(|p| p.clone())
    }

    /// Rebuild the shard-hash -> path map from the archive directory.
    pub fn refresh_shards(&self) {
        let entries = match std::fs::read_dir(&self.out_dir) {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to list {}: {e}", self.out_dir.display());
                return;
            }
        };
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(true) {
                continue; // skip the index-* database directories
            }
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(".frames") {
                continue; // sidecars are not shards
            }
            let h = shard_hash(name);
            if let Some(existing) = self.shards.get(&h)
                && existing.value() != &path
            {
                // 64-bit collision (~1e-13 for a few thousand shards). Refuse
                // to guess: lookups fall back to scanning.
                warn!(
                    "shard hash collision between {} and {}",
                    existing.value().display(),
                    path.display()
                );
                continue;
            }
            self.shards.insert(h, path);
        }
    }

    /// Raw JSON lines for `ids`, in the same order. `None` where the event is
    /// unknown or could not be read.
    ///
    /// Batched on purpose: one index `multi_get`, then reads grouped by
    /// `(shard, frame)` so a frame holding several wanted events is decoded
    /// once, executed in parallel on the blocking pool.
    pub async fn get_many_raw(&self, ids: &[EventId]) -> Vec<Option<Vec<u8>>> {
        let keys: Vec<[u8; 32]> = ids.iter().map(|i| *i.as_bytes()).collect();
        let entries = self.database.get_many(&keys);

        let mut requests = Vec::with_capacity(ids.len());
        // request index -> position in the output
        let mut slots = Vec::with_capacity(ids.len());
        let mut fallback = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            match entry {
                Some(IndexEntry { loc: Some(loc), .. }) => match self.shard_path(loc.shard) {
                    Some(path) => {
                        slots.push(i);
                        requests.push(ReadRequest {
                            path,
                            offset: loc.offset,
                            len: loc.len,
                        });
                    }
                    None => {
                        // Shard renamed/deleted since indexing.
                        fallback.push((i, entry.map(|e| e.created_at)));
                    }
                },
                // v0 entry: we know the event exists and when, but not where.
                Some(e) => fallback.push((i, Some(e.created_at))),
                None => {}
            }
        }

        let mut out = vec![None; ids.len()];
        if !requests.is_empty() {
            let read = self.pool.read_many_async(Arc::new(requests)).await;
            for (slot, bytes) in slots.into_iter().zip(read) {
                out[slot] = bytes;
            }
        }

        // Events whose location is unknown: scan the shard for the day we do
        // know about. Bounded, and it keeps pre-existing (v0) databases usable
        // without a rebuild.
        for (i, created_at) in fallback {
            if self.scan_fallback == ScanFallback::Off {
                continue;
            }
            if let Some(ts) = created_at {
                out[i] = self.scan_for_id(&ids[i], ts).await;
            }
        }

        // A stale or corrupt offset must never yield the wrong event.
        for (i, bytes) in out.iter_mut().enumerate() {
            if let Some(b) = bytes.as_ref()
                && !line_has_id(b, &ids[i])
            {
                warn!("index pointed at the wrong event for {}", ids[i]);
                *bytes = None;
            }
        }
        out
    }

    /// Raw JSON line for one event id.
    pub async fn get_raw(&self, id: &EventId) -> Option<Vec<u8>> {
        self.get_many_raw(std::slice::from_ref(id)).await.remove(0)
    }

    /// Scan for `id`, starting with the shard named for its `created_at` day.
    /// Used for v0 index entries and for shards that moved after indexing.
    async fn scan_for_id(&self, id: &EventId, created_at: u64) -> Option<Vec<u8>> {
        let id = *id;
        let mut candidates = Vec::new();
        if let Some(day) = DateTime::from_timestamp(created_at as i64, 0) {
            let path = Self::get_archive_path(&self.out_dir, &day);
            if path.exists() {
                candidates.push(path);
            }
        }

        if self.scan_fallback == ScanFallback::All {
            // Shard names follow write time, not event time, so an imported
            // archive can hold an event from any day.
            self.refresh_shards();
            for entry in self.shards.iter() {
                if !candidates.contains(entry.value()) {
                    candidates.push(entry.value().clone());
                }
            }
            if candidates.len() > 1 {
                debug!("scanning {} shards for {id}", candidates.len());
            }
        }

        tokio::task::spawn_blocking(move || {
            candidates
                .into_iter()
                .find_map(|path| scan_file_for_id(&path, &id))
        })
        .await
        .ok()
        .flatten()
    }

    /// Returns the number of items in the index database
    ///
    /// **WARNING:** Can take a very long time if your index is very large, this operation is O(n)
    pub fn count_keys(&self) -> u64 {
        self.database.count_keys()
    }

    /// Is the index empty
    pub fn is_index_empty(&self) -> bool {
        self.database.is_index_empty()
    }

    /// Recompute the cached event count from the actual index data and persist it.
    ///
    /// **WARNING:** This is an O(n) scan over the index. Use to repair a stale count
    /// (e.g. after concurrent writers during a rollout left the persisted count wrong).
    /// Returns the corrected count.
    pub fn repair_count(&self) -> Result<u64> {
        self.database.repair_count()
    }

    /// Wipe the index database. Exposed for integration tests and for callers that
    /// need to reset before a full `rebuild_index`.
    pub fn database_wipe_for_test(&mut self) -> Result<()> {
        self.database.wipe()
    }

    /// Where an event is stored, if the index knows.
    pub fn locate(&self, id: &EventId) -> Result<Option<EventLoc>> {
        Ok(self.database.get(id.as_bytes())?.and_then(|e| e.loc))
    }

    /// Point an id at a different location. Only for tests that need to
    /// simulate a stale/corrupt index.
    pub fn overwrite_location_for_test(&self, id: &EventId, loc: EventLoc) -> Result<()> {
        let created_at = self
            .database
            .get(id.as_bytes())?
            .map(|e| e.created_at)
            .unwrap_or_default();
        self.database
            .insert(*id.as_bytes(), IndexEntry::located(created_at, loc))
    }

    /// Index shards that are new or have changed since the last pass.
    ///
    /// This is the incremental counterpart to
    /// [`rebuild_index`](Self::rebuild_index): it skips shards whose size and
    /// mtime match what was recorded when they were indexed, so a restart over
    /// a 132 GB archive costs a `stat` per shard instead of a full re-walk.
    ///
    /// Shards that are one giant frame (the usual shape of an archive produced
    /// elsewhere) are rewritten into bounded frames first, otherwise every
    /// lookup into them would decode from the start of the file. Reframing
    /// preserves the decompressed bytes, so offsets stay valid.
    ///
    /// The shard the writer is currently appending to is left alone - those
    /// events are indexed inline as they are written.
    ///
    /// Blocking: reads and compresses whole shards. Call it from
    /// `spawn_blocking` (or a maintenance thread), not from an async task.
    #[cfg(feature = "sync")]
    pub fn index_new_shards(&self) -> Result<IndexReport> {
        let mut report = IndexReport::default();
        let live = Self::get_archive_path(&self.out_dir, &Utc::now());

        let mut todo = Vec::new();
        for entry in std::fs::read_dir(&self.out_dir)?.flatten() {
            let path = entry.path();
            if !path.is_file() || !crate::cursor::is_walkable_archive(&path) || path == live {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            report.shards += 1;

            let key = ShardState::key(name);
            let stored = self
                .database
                .get_meta(&key)?
                .and_then(|v| ShardState::decode(&v));
            let current = ShardState::of(&path, 0)?;
            if stored.map(|s| s.matches(&current)).unwrap_or(false) {
                report.unchanged += 1;
                continue;
            }
            todo.push((path, key));
        }

        for (path, key) in todo {
            if self.reframe_if_coarse(&path)? {
                report.reframed += 1;
            }

            let shard = shard_hash_of(&path);
            let new_events = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let counter = new_events.clone();
            let db = self.database.clone();
            crate::NostrCursor::new(self.out_dir.clone())
                .with_files(vec![path.clone()])
                .with_dedupe(false)
                .walk_with_chunked_sync_located(
                    move |_path, events| {
                        let mut batch = Vec::with_capacity(events.len());
                        let mut id = [0u8; 32];
                        for e in events {
                            if e.event.id.len() == 64
                                && faster_hex::hex_decode(e.event.id.as_bytes(), &mut id).is_ok()
                            {
                                batch.push((
                                    id,
                                    IndexEntry::located(
                                        e.event.created_at,
                                        EventLoc {
                                            shard,
                                            offset: e.offset,
                                            len: e.len,
                                        },
                                    ),
                                ));
                            }
                        }
                        match db.insert_batch(batch) {
                            // insert_batch reports genuinely new keys, so the
                            // count stays exact without an O(n) rescan.
                            Ok(new) => {
                                counter.fetch_add(new as u64, std::sync::atomic::Ordering::Relaxed);
                            }
                            Err(e) => warn!("Failed to apply index update: {e}"),
                        }
                    },
                    1000,
                );

            let indexed = new_events.load(std::sync::atomic::Ordering::Relaxed);
            report.indexed += 1;
            report.new_events += indexed;
            // Record state *after* any reframe, so size/mtime match the file we
            // just read - otherwise the next pass would redo the work.
            self.database
                .put_meta(&key, &ShardState::of(&path, indexed)?.encode())?;
            debug!("indexed {} ({indexed} new events)", path.display());
        }
        Ok(report)
    }

    /// Rewrite a shard into bounded frames when its frames are too coarse for
    /// seeking (typically a whole archive compressed as one frame).
    /// Returns whether it was rewritten.
    #[cfg(feature = "sync")]
    fn reframe_if_coarse(&self, path: &Path) -> Result<bool> {
        let is_zstd = matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("zst") | Some("zstd")
        );
        if !is_zstd {
            return Ok(false); // gz/bz2 cannot be reframed without renaming
        }

        let table = self.pool.frame_table(path);
        let compressed_len = std::fs::metadata(path)?.len();
        let coarse = match table.max_frame_span() {
            // Frames much bigger than the target: a lookup would decode far
            // more than it needs.
            Some(max) => max > self.frame_target.saturating_mul(4),
            // Zero or one boundary: a single frame covering the whole file.
            // Tiny files are cheap to decode whole, so leave them.
            None => compressed_len > self.frame_target,
        };
        if !coarse {
            return Ok(false);
        }

        debug!("reframing {} for seekable lookups", path.display());
        reframe_archive(path, self.frame_target)?;
        // Cached fd/frame table now describe the old file.
        self.pool.invalidate(path);
        Ok(true)
    }

    /// Generate `.frames` sidecars for any zstd shard that lacks one, so
    /// lookups into it can seek instead of decoding from the start.
    ///
    /// Costs one decompression pass per shard. Returns how many were built.
    pub fn rebuild_missing_frame_indexes(&self) -> usize {
        let entries = match std::fs::read_dir(&self.out_dir) {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to list {}: {e}", self.out_dir.display());
                return 0;
            }
        };
        let mut built = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let is_zstd = matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("zst") | Some("zstd")
            );
            if !is_zstd || crate::database::frames::sidecar_path(&path).exists() {
                continue;
            }
            match rebuild_frame_index(&path) {
                Ok(n) => {
                    debug!("built frame index for {} ({n} frames)", path.display());
                    built += 1;
                }
                Err(e) => warn!("{}: failed to build frame index: {e}", path.display()),
            }
        }
        self.pool.clear();
        built
    }

    /// Rebuilt event id index using parallel std::thread workers.
    ///
    /// This method uses OS threads for true CPU parallelism, which is significantly
    /// faster than the async version for CPU-bound workloads like JSON parsing.
    #[cfg(feature = "sync")]
    pub fn rebuild_index(&mut self) -> Result<()> {
        // Make every shard seekable first: build missing sidecars, and rewrite
        // archives that are one giant frame (imported from elsewhere) into
        // bounded frames. Reframing preserves decompressed bytes, so the
        // offsets recorded below stay valid.
        self.rebuild_missing_frame_indexes();
        for entry in std::fs::read_dir(&self.out_dir)?.flatten() {
            let path = entry.path();
            if path.is_file()
                && crate::cursor::is_walkable_archive(&path)
                && let Err(e) = self.reframe_if_coarse(&path)
            {
                warn!("{}: reframe failed: {e}", path.display());
            }
        }
        self.database.wipe()?;
        self.database.setup_for_reindex()?;
        self.shards.clear();
        self.pool.clear();
        let db = self.database.clone();
        crate::NostrCursor::new(self.out_dir.clone())
            .with_max_parallelism()
            // No in-memory dedupe during rebuild: it would buffer every event id in a
            // DashMap (millions of ids -> OOM on large archives). Re-inserting the
            // same id is a harmless idempotent overwrite in the KV index, so we just
            // insert and repair the count afterwards.
            .with_dedupe(false)
            .walk_with_chunked_sync_located(
                move |path, events| {
                    // The shard id comes from the file name alone, so an
                    // archive dropped in by an external backup indexes without
                    // any registration step.
                    let shard = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(shard_hash)
                        .unwrap_or(0);
                    let mut batch = Vec::with_capacity(events.len());
                    let mut id = [0u8; 32];
                    for e in events {
                        if e.event.id.len() == 64
                            && faster_hex::hex_decode(e.event.id.as_bytes(), &mut id).is_ok()
                        {
                            batch.push((
                                id,
                                IndexEntry::located(
                                    e.event.created_at,
                                    EventLoc {
                                        shard,
                                        offset: e.offset,
                                        len: e.len,
                                    },
                                ),
                            ));
                        }
                    }
                    if let Err(e) = db.insert_batch(batch).map(|_| ()) {
                        warn!("Failed to apply index update: {}", e);
                    }
                },
                1000,
            );

        // `insert_batch` counts only genuinely new keys, so the cached count is
        // already exact - no O(n) rescan needed here any more.

        // Record what was indexed so `index_new_shards` skips these shards.
        let live = Self::get_archive_path(&self.out_dir, &Utc::now());
        for entry in std::fs::read_dir(&self.out_dir)?.flatten() {
            let path = entry.path();
            if !path.is_file() || !crate::cursor::is_walkable_archive(&path) || path == live {
                continue;
            }
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                let state = ShardState::of(&path, 0)?;
                self.database
                    .put_meta(&ShardState::key(name), &state.encode())?;
            }
        }
        Ok(())
    }
}

impl<D> NostrDatabase for JsonFilesDatabase<D>
where
    D: IndexDb + 'static,
{
    fn backend(&self) -> Backend {
        Backend::Custom("JsonFileDatabase".to_owned())
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            let id = *event.id.as_bytes();
            if self
                .database
                .contains_key(&id)
                .map_err(|e| DatabaseError::Backend(e.into_boxed_dyn_error()))?
            {
                return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
            }

            // The index entry is written by the writer thread, once the event
            // has an offset. Claim the id here so duplicates arriving in that
            // window are still rejected synchronously.
            if self.in_flight.insert(id, ()).is_some() {
                return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
            }

            match self
                .tx_writer
                .send(WriterMsg::Event(Box::new(event.clone())))
                .await
            {
                Ok(()) => {
                    debug!("Saved event: {}", event.id);
                    Ok(SaveEventStatus::Success)
                }
                Err(e) => {
                    self.in_flight.remove(&id);
                    Err(DatabaseError::Backend(Box::new(e)))
                }
            }
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            if self.in_flight.contains_key(event_id.as_bytes())
                || self
                    .database
                    .contains_key(event_id.as_bytes())
                    .map_err(|e| DatabaseError::Backend(e.into_boxed_dyn_error()))?
            {
                Ok(DatabaseEventStatus::Saved)
            } else {
                Ok(DatabaseEventStatus::NotExistent)
            }
        })
    }

    fn event_by_id(
        &self,
        event_id: &EventId,
    ) -> BoxedFuture<'_, Result<Option<Event>, DatabaseError>> {
        let event_id = *event_id;
        Box::pin(async move {
            let Some(raw) = self.get_raw(&event_id).await else {
                return Ok(None);
            };
            match serde_json::from_slice::<Event>(&raw) {
                Ok(e) => Ok(Some(e)),
                Err(e) => {
                    warn!("Failed to parse stored event {event_id}: {e}");
                    Ok(None)
                }
            }
        })
    }

    fn count(&self, filter: Filter) -> BoxedFuture<'_, Result<usize, DatabaseError>> {
        Box::pin(async move {
            // Only id-filters are servable from the index without a scan.
            match filter.ids.as_ref() {
                Some(ids) => {
                    let mut n = 0;
                    for id in ids {
                        if self
                            .database
                            .contains_key(id.as_bytes())
                            .map_err(|e| DatabaseError::Backend(e.into_boxed_dyn_error()))?
                        {
                            n += 1;
                        }
                    }
                    Ok(n)
                }
                None => Ok(0),
            }
        })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<'_, Result<Events, DatabaseError>> {
        Box::pin(async move {
            let mut events = Events::new(&filter);
            // Fetch by id in one batch; other filter kinds still need a
            // relay-side query (the archive has no pubkey/kind index).
            let Some(ids) = filter.ids.as_ref() else {
                return Ok(events);
            };
            let ids: Vec<EventId> = ids.iter().copied().collect();
            for raw in self.get_many_raw(&ids).await.into_iter().flatten() {
                match serde_json::from_slice::<Event>(&raw) {
                    Ok(e) if filter.match_event(&e, Default::default()) => {
                        events.insert(e);
                    }
                    Ok(_) => {}
                    Err(e) => warn!("Failed to parse stored event: {e}"),
                }
            }
            Ok(events)
        })
    }

    fn delete(&self, _filter: Filter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }

    fn wipe(&self) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }
}

pub type DefaultJsonFilesDatabase = JsonFilesDatabase<rocksdb::RocksDbIndex>;

impl DefaultJsonFilesDatabase {
    pub fn new<P>(path: P) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        Self::new_with_frame_target(path, DEFAULT_FRAME_TARGET)
    }

    /// As [`new`](Self::new) with a custom zstd frame size - the lookup
    /// latency knob, see [`DEFAULT_FRAME_TARGET`].
    pub fn new_with_frame_target<P>(path: P, frame_target: u64) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        let p = PathBuf::from(&path);
        let db = rocksdb::RocksDbIndex::open(p.join("index-rocksdb"))?;
        JsonFilesDatabase::new_with_index_and_frame_target(path, db, frame_target)
    }
}
