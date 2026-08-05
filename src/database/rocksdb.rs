#![cfg(feature = "db-rocksdb")]
use crate::IndexDb;
use crate::database::value::{IndexEntry, V0_LEN};
use anyhow::Result;
use anyhow::anyhow;
use log::{debug, warn};
use rocksdb::properties::{
    BLOCK_CACHE_CAPACITY, BLOCK_CACHE_PINNED_USAGE, BLOCK_CACHE_USAGE, CUR_SIZE_ACTIVE_MEM_TABLE,
    CUR_SIZE_ALL_MEM_TABLES, ESTIMATE_LIVE_DATA_SIZE, ESTIMATE_NUM_KEYS,
    ESTIMATE_PENDING_COMPACTION_BYTES, ESTIMATE_TABLE_READERS_MEM, LIVE_SST_FILES_SIZE,
    MEM_TABLE_FLUSH_PENDING, SIZE_ALL_MEM_TABLES,
};
use rocksdb::{BlockBasedOptions, Cache, IteratorMode, Options};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Prefix for indexer bookkeeping keys (per-shard state).
///
/// Bookkeeping lives in the default column family - adding a column family
/// would stop any tool that opens the index with plain `DB::open` from working.
/// That is safe because event keys are distinguished purely by *length*
/// (32 = primary, 40 = time index), and [`meta_key`] guarantees bookkeeping
/// keys are never either length.
pub const META_PREFIX: &[u8] = b"shard/";

/// Build a bookkeeping key that can never be mistaken for an event key.
pub fn meta_key(name: &str) -> Vec<u8> {
    let mut k = META_PREFIX.to_vec();
    k.extend_from_slice(name.as_bytes());
    while k.len() == 32 || k.len() == 40 {
        k.push(b'/');
    }
    k
}

/// Meta key storing the number of indexed events (excluded from `list_ids` since
/// it is not 32 bytes). Allows O(1) startup count instead of a full keyspace scan.
const META_COUNT_KEY: &[u8] = b"__meta_count__";

/// Meta key marking the secondary time index as complete. Until it is set,
/// time-ranged queries return empty and a background thread backfills
/// `time_key` entries for all pre-existing events.
const META_TIMEIDX_KEY: &[u8] = b"__meta_timeidx__";

/// Secondary index key for time-ranged lookups: big-endian `created_at`
/// followed by the event id. Big-endian makes lexicographic key order equal
/// numeric time order, so a day is one contiguous range scan. Distinguished
/// from primary 32-byte id keys by length (40 bytes).
fn time_key(id: &[u8; 32], ts: u64) -> [u8; 40] {
    let mut k = [0u8; 40];
    k[..8].copy_from_slice(&ts.to_be_bytes());
    k[8..].copy_from_slice(id);
    k
}

#[derive(Clone)]
pub struct RocksDbIndex {
    database: Option<Arc<rocksdb::DB>>,
    /// Total number of events in the database
    item_count: Arc<AtomicUsize>,
    /// Whether the secondary time index covers all existing entries.
    time_ready: Arc<std::sync::atomic::AtomicBool>,
}

impl RocksDbIndex {
    /// Options used for normal (non-bulk-load) operation.
    ///
    /// `DB::open_default` asks for Snappy, which this build does not include,
    /// so the index ends up **uncompressed**. Event ids are random and
    /// incompressible, but the rest (v1 values, the big-endian timestamps that
    /// prefix the time index) is not, so zstd is worth having. A bloom filter
    /// keeps `contains_key`/`get` from touching disk for absent ids, which is
    /// the dominant lookup pattern.
    pub fn get_options() -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

        // Bounded descriptor and metadata use: the default (-1) keeps an fd and
        // a table reader per SST, and each reader holds that file's index and
        // filter blocks. On a multi-gigabyte index that grows without limit.
        opts.set_max_open_files(512);

        let mut table = BlockBasedOptions::default();
        table.set_bloom_filter(10.0, false);
        // Keys are 32/40 bytes with no shared prefix worth indexing, so bigger
        // blocks trade a little read amplification for much less index size.
        table.set_block_size(16 * 1024);
        // Cache index/filter blocks rather than pinning them per open file, and
        // give the cache an explicit budget -- without one, "cached" index and
        // filter blocks are simply unbounded memory.
        table.set_block_cache(&Cache::new_lru_cache(512 * 1024 * 1024));
        table.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&table);

        opts
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = rocksdb::DB::open(&Self::get_options(), path).map_err(|e| anyhow!(e))?;

        // Try to read the persisted count first; fall back to a one-time full scan
        // of 32-byte keys for legacy databases, then persist it for next time.
        let initial_count = match db.get_pinned(META_COUNT_KEY) {
            Ok(Some(val)) if val.len() >= 8 => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&val[..8]);
                u64::from_le_bytes(arr) as usize
            }
            _ => {
                let count = db
                    .iterator(IteratorMode::Start)
                    .filter(|x| x.as_ref().map(|(k, _)| k.len() == 32).unwrap_or(false))
                    .count();
                let _ = db.put(META_COUNT_KEY, (count as u64).to_le_bytes());
                count
            }
        };

        let db = Arc::new(db);
        // Time index state: complete flag present, or trivially complete when
        // the database is empty. Otherwise backfill in the background —
        // time-ranged queries return empty until it finishes.
        let time_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        match db.get_pinned(META_TIMEIDX_KEY) {
            Ok(Some(_)) => time_ready.store(true, Ordering::SeqCst),
            _ if initial_count == 0 => {
                let _ = db.put(META_TIMEIDX_KEY, [1u8]);
                time_ready.store(true, Ordering::SeqCst);
            }
            _ => {
                let db = db.clone();
                let flag = time_ready.clone();
                std::thread::Builder::new()
                    .name("rocksdb-timeidx".into())
                    .spawn(move || {
                        warn!("building time index for {initial_count} existing events");
                        let started = std::time::Instant::now();
                        let mut batch = rocksdb::WriteBatch::new();
                        let mut n = 0u64;
                        for item in db.iterator(IteratorMode::Start) {
                            let Ok((k, v)) = item else { break };
                            if k.len() != 32 || v.len() < V0_LEN {
                                continue;
                            }
                            let id: &[u8; 32] = k.as_ref().try_into().unwrap();
                            // Values may be v0 (8 bytes) or v1 (27); only the
                            // timestamp prefix matters here.
                            let Some(ts) = IndexEntry::decode_created_at(&v) else {
                                continue;
                            };
                            batch.put(time_key(id, ts), []);
                            n += 1;
                            if batch.len() >= 100_000 {
                                let b = std::mem::take(&mut batch);
                                if let Err(e) = db.write(b) {
                                    warn!("time index backfill write failed: {e}");
                                    return;
                                }
                            }
                        }
                        if let Err(e) = db.write(batch) {
                            warn!("time index backfill write failed: {e}");
                            return;
                        }
                        let _ = db.put(META_TIMEIDX_KEY, [1u8]);
                        flag.store(true, Ordering::SeqCst);
                        warn!(
                            "time index complete: {n} entries in {:.0}s",
                            started.elapsed().as_secs_f64()
                        );
                    })
                    .ok();
            }
        }

        let index = Self {
            database: Some(db),
            item_count: Arc::new(AtomicUsize::new(initial_count)),
            time_ready,
        };

        // Heal a count left catastrophically wrong -- reset to zero by a crash
        // mid-rollout while the data survived. Deliberately a ratio test and
        // not a percentage one: see `auto_repair_count_if_diverged`.
        index.auto_repair_count_if_diverged();

        Ok(index)
    }

    /// Compare the cached count with `ESTIMATE_NUM_KEYS` and run `repair_count()`
    /// when they disagree by more than the threshold. Returns true if a repair ran.
    ///

    ///
    /// `ESTIMATE_NUM_KEYS` is an approximation and also counts the meta key, so we
    /// only treat large divergences as corruption: the difference must exceed both
    /// a relative ratio of the estimate and an absolute floor (to avoid false
    /// positives on tiny databases where the estimate is unreliable).
    fn auto_repair_count_if_diverged(&self) -> bool {
        // `estimate-num-keys` sums SST table properties and does not reconcile
        // a key written more than once across levels. `rebuild_index` bulk-loads
        // with overwrites by design -- re-inserting an id is an idempotent
        // overwrite -- so on a rebuilt index the estimate sits well below the
        // true key count and never converges.
        //
        // At 10% this fired on every single start of a healthy 897M-event
        // index (estimate 1.60G against 1.79G expected, 11% low) and spent
        // ~20 minutes rescanning to confirm the cached count was already
        // correct -- before the port was even bound, and filling ~29 GB of page
        // cache immediately before the server allocates its own working set.
        // The band has to reflect what this estimator can actually promise.
        // A *ratio*, not a percentage. This exists to catch a count left
        // catastrophically wrong -- reset to zero by a crash mid-rollout while
        // the data survived -- which is off by orders of magnitude. It is not
        // here to police estimator noise, which on a bulk-loaded index runs to
        // tens of percent and drifts as compaction proceeds.
        const RATIO_THRESHOLD: f64 = 4.0;
        const ABS_THRESHOLD: u64 = 1000; // ...and at least this many keys

        let Some(database) = self.database.as_ref() else {
            return false;
        };
        let estimate = match database.property_int_value(ESTIMATE_NUM_KEYS) {
            Ok(Some(e)) => e,
            _ => return false, // can't estimate -> don't touch
        };

        // While the time index is still backfilling, the keyspace is in
        // flux between N and 2N keys; any divergence check would misfire.
        if !self.time_index_ready() {
            return false;
        }

        let cached = self.item_count.load(Ordering::SeqCst) as u64;
        // Each event has a primary id key and a time-index key, plus the two
        // meta keys.
        let expected = cached.saturating_mul(2).saturating_add(2);
        let diff = estimate.abs_diff(expected);

        let ratio = expected.max(estimate) as f64 / expected.min(estimate).max(1) as f64;
        let diverged = diff > ABS_THRESHOLD && ratio > RATIO_THRESHOLD;

        if diverged {
            warn!(
                "Event index count looks stale (cached={cached} events = {expected} keys, \
                 estimate={estimate} keys); running repair_count() to rescan."
            );
            match self.repair_count() {
                Ok(n) => {
                    warn!("Event index count repaired: {cached} -> {n}");
                    return true;
                }
                Err(e) => {
                    warn!("auto repair_count() failed: {e}");
                }
            }
        }
        false
    }

    pub fn get_bulk_load_options() -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // FIRST, because `prepare_for_bulk_load` rewrites the very settings we
        // care about: it sets all three level-0 triggers to 1<<30 and disables
        // auto-compaction. Calling it after the triggers below silently undid
        // them, so a rebuild accumulated level-0 files without ever compacting
        // -- and with them, the per-file index and filter blocks that made the
        // process grow by gigabytes until it was OOM-killed.
        opts.prepare_for_bulk_load();

        // Moderate memtable sizes to control memory usage
        // With N threads writing, memory can spike quickly
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MiB per memtable
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);

        // Trigger compaction/flush sooner to prevent memory buildup. These come
        // after `prepare_for_bulk_load` so they win: a rebuild that never
        // compacts trades a bounded amount of write amplification for unbounded
        // memory, which is the wrong way round.
        opts.set_disable_auto_compactions(false);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(8);
        opts.set_level_zero_stop_writes_trigger(12);

        // More background threads for compaction
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4);
        opts.increase_parallelism(parallelism);
        opts.set_max_background_jobs(parallelism.min(8));

        // Bounded descriptor and metadata use. The default (-1) keeps an fd and
        // a table reader per SST, and a table reader holds that file's index and
        // filter blocks resident. Across the thousands of files a full rebuild
        // produces that is gigabytes of memory nothing ever caps.
        opts.set_max_open_files(512);

        // Same on-disk layout as steady state, or a reindex would rewrite the
        // whole index uncompressed.
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
        let mut table_opts = BlockBasedOptions::default();
        table_opts.set_bloom_filter(10.0, false);
        table_opts.set_block_size(16 * 1024);
        // Index and filter blocks go in the block cache, so they are bounded by
        // its capacity instead of growing with the number of open files. At 10
        // bits per key a few hundred million keys is otherwise gigabytes of
        // filters held outside any budget.
        table_opts.set_block_cache(&Cache::new_lru_cache(512 * 1024 * 1024));
        table_opts.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&table_opts);

        opts
    }

    /// Rewrite every SST with the current options.
    ///
    /// Existing databases were written **uncompressed** (the previous options
    /// asked for Snappy, which this build does not include). Opening with the
    /// new options does not rewrite anything: old files keep their old format
    /// and are converted only as normal compaction touches them. This forces
    /// that conversion now, at the cost of one full rewrite of the index.
    pub fn compact(&self) {
        let Some(db) = self.database.as_ref() else {
            return;
        };
        // `compact_range` alone will not do it: manual compaction skips the
        // bottommost level unless forced, and in a mostly-static index that is
        // where all the old data lives - so it would silently rewrite nothing.
        let mut opts = rocksdb::CompactOptions::default();
        opts.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &opts);
    }

    pub fn print_memory_usage(&self) {
        let database = self.database.as_ref().expect("Database not open");
        fn format_bytes(s: &str) -> String {
            let bytes: u64 = s.parse().unwrap_or(0);
            const GIB: u64 = 1024 * 1024 * 1024;
            const MIB: u64 = 1024 * 1024;
            if bytes >= GIB {
                format!("{:.2} GiB", bytes as f64 / GIB as f64)
            } else {
                format!("{:.2} MiB", bytes as f64 / MIB as f64)
            }
        }

        debug!("=== RocksDB Memory Usage ===");

        // Block cache statistics
        if let Ok(Some(capacity)) = database.property_value(BLOCK_CACHE_CAPACITY) {
            debug!("Block Cache Capacity: {}", format_bytes(&capacity));
        }
        if let Ok(Some(usage)) = database.property_value(BLOCK_CACHE_USAGE) {
            debug!("Block Cache Usage: {}", format_bytes(&usage));
        }
        if let Ok(Some(pinned)) = database.property_value(BLOCK_CACHE_PINNED_USAGE) {
            debug!("Block Cache Pinned: {}", format_bytes(&pinned));
        }

        // Memtable statistics
        if let Ok(Some(active)) = database.property_value(CUR_SIZE_ACTIVE_MEM_TABLE) {
            debug!("Active Memtable Size: {}", format_bytes(&active));
        }
        if let Ok(Some(all_unflushed)) = database.property_value(CUR_SIZE_ALL_MEM_TABLES) {
            debug!(
                "All Unflushed Memtables Size: {}",
                format_bytes(&all_unflushed)
            );
        }
        if let Ok(Some(all_mem)) = database.property_value(SIZE_ALL_MEM_TABLES) {
            debug!("All Memtables (incl. pinned): {}", format_bytes(&all_mem));
        }

        // Table readers (index/filter blocks outside block cache)
        if let Ok(Some(table_readers)) = database.property_value(ESTIMATE_TABLE_READERS_MEM) {
            debug!(
                "Estimated Table Readers Memory: {}",
                format_bytes(&table_readers)
            );
        }

        // Live data and SST files
        if let Ok(Some(live_data)) = database.property_value(ESTIMATE_LIVE_DATA_SIZE) {
            debug!("Estimated Live Data Size: {}", format_bytes(&live_data));
        }
        if let Ok(Some(sst_size)) = database.property_value(LIVE_SST_FILES_SIZE) {
            debug!("Live SST Files Size: {}", format_bytes(&sst_size));
        }

        // Key count estimate
        if let Ok(Some(num_keys)) = database.property_value(ESTIMATE_NUM_KEYS) {
            debug!("Estimated Number of Keys: {}", num_keys);
        }

        // Pending operations
        if let Ok(Some(pending_compact)) =
            database.property_value(ESTIMATE_PENDING_COMPACTION_BYTES)
        {
            debug!(
                "Pending Compaction Bytes: {}",
                format_bytes(&pending_compact)
            );
        }
        if let Ok(Some(flush_pending)) = database.property_value(MEM_TABLE_FLUSH_PENDING) {
            debug!("Memtable Flush Pending: {}", flush_pending);
        }

        debug!("============================");
    }
}

impl IndexDb for RocksDbIndex {
    fn list_ids(&self, min: u64, max: u64) -> Vec<([u8; 32], u64)> {
        if !self.time_index_ready() {
            return Vec::new();
        }
        let database = self.database.as_ref().expect("Database not open");
        let start = min.to_be_bytes();
        let mut out = Vec::new();
        for item in database.iterator(IteratorMode::From(&start, rocksdb::Direction::Forward)) {
            let Ok((k, _)) = item else { break };
            // Key order is prefix order: once the 8-byte time prefix exceeds
            // `max` there is nothing further in range.
            if k.len() < 8 {
                continue;
            }
            let ts = u64::from_be_bytes(k[..8].try_into().unwrap());
            if ts > max {
                break;
            }
            // Skip interleaved primary (32-byte) id keys whose random prefix
            // happens to fall inside the range.
            if k.len() != 40 {
                continue;
            }
            let id: [u8; 32] = k[8..].try_into().unwrap();
            out.push((id, ts));
        }
        out
    }

    fn time_index_ready(&self) -> bool {
        self.time_ready.load(Ordering::Relaxed)
    }

    fn count_keys(&self) -> u64 {
        let ret = self.item_count.load(Ordering::Relaxed);
        ret as u64
    }

    fn contains_key(&self, id: &[u8; 32]) -> Result<bool> {
        let database = self.database.as_ref().expect("Database not open");
        match database.get_pinned(id) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => {
                warn!("Failed to check key exist: {:?}", e);
                Ok(false)
            }
        }
    }

    fn is_index_empty(&self) -> bool {
        self.item_count.load(Ordering::SeqCst) == 0
    }

    fn repair_count(&self) -> Result<u64> {
        let database = self.database.as_ref().expect("Database not open");
        // Full scan of real (32-byte) event keys; ignores the meta key.
        let count = database
            .iterator(IteratorMode::Start)
            .filter(|x| x.as_ref().map(|(k, _)| k.len() == 32).unwrap_or(false))
            .count();
        self.item_count.store(count, Ordering::SeqCst);
        database
            .put(META_COUNT_KEY, (count as u64).to_le_bytes())
            .map_err(|e| anyhow!(e))?;
        Ok(count as u64)
    }

    fn setup_for_reindex(&mut self) -> Result<()> {
        // Reopening with bulk-load options is a performance optimization for large
        // reindexes. It requires exclusive access to the DB LOCK, which fails when
        // another clone of this handle is still alive in the process. In that case
        // fall back to the existing (already-open) handle - reindex still works
        // correctly, just without the bulk-load tuning.
        let Some(current) = self.database.as_ref() else {
            return Err(anyhow!("Database not open"));
        };

        // Only one strong reference -> we hold the handle exclusively and can reopen.
        if Arc::strong_count(current) == 1 {
            let path = {
                let db = self.database.take().expect("Database not open");
                let path = db.path().to_owned();
                drop(db);
                path
            };
            let opts = Self::get_bulk_load_options();
            let db = rocksdb::DB::open(&opts, path).map_err(|e| anyhow!(e))?;
            self.database.replace(Arc::new(db));
        } else {
            warn!(
                "setup_for_reindex: {} handle clones alive; keeping existing handle (bulk-load tuning skipped)",
                Arc::strong_count(current) - 1
            );
        }
        Ok(())
    }

    fn insert(&self, k: [u8; 32], v: IndexEntry) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        let new_count = self.item_count.fetch_add(1, Ordering::Relaxed) + 1;
        let mut batch = rocksdb::WriteBatch::new();
        batch.put(time_key(&k, v.created_at), []);
        batch.put(k, v.encode().as_slice());
        batch.put(META_COUNT_KEY, (new_count as u64).to_le_bytes());
        database.write(batch).map_err(|e| anyhow!(e))?;
        Ok(())
    }

    fn insert_batch(&self, items: Vec<([u8; 32], IndexEntry)>) -> Result<usize> {
        let database = self.database.as_ref().expect("Database not open");

        // Re-inserting an id is an idempotent overwrite, so only count keys
        // that are actually new. One batched read with bloom filters costs
        // almost nothing when the keys are absent (the bulk-load case), and it
        // keeps the cached count exact instead of needing an O(n) rescan.
        let keys: Vec<[u8; 32]> = items.iter().map(|(k, _)| *k).collect();
        let existing = database.multi_get(&keys);

        let mut batch = rocksdb::WriteBatch::new();
        let mut new_keys = 0usize;
        for ((k, v), present) in items.into_iter().zip(existing) {
            if !matches!(present, Ok(Some(_))) {
                new_keys += 1;
            }
            batch.put(time_key(&k, v.created_at), []);
            batch.put(k, v.encode().as_slice());
        }
        let new_count = self.item_count.fetch_add(new_keys, Ordering::Relaxed) + new_keys;
        batch.put(META_COUNT_KEY, (new_count as u64).to_le_bytes());
        database.write(batch)?;
        Ok(new_keys)
    }

    fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let database = self.database.as_ref().expect("Database not open");
        database.get(key).map_err(|e| anyhow!(e))
    }

    fn put_meta(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        database.put(key, value).map_err(|e| anyhow!(e))
    }

    fn clear_meta(&self) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        let mut batch = rocksdb::WriteBatch::new();
        for item in database.prefix_iterator(META_PREFIX) {
            let (k, _) = item.map_err(|e| anyhow!(e))?;
            if !k.starts_with(META_PREFIX) {
                break;
            }
            batch.delete(&k);
        }
        database.write(batch).map_err(|e| anyhow!(e))
    }

    fn get(&self, id: &[u8; 32]) -> Result<Option<IndexEntry>> {
        let database = self.database.as_ref().expect("Database not open");
        match database.get_pinned(id).map_err(|e| anyhow!(e))? {
            Some(v) => Ok(Some(IndexEntry::decode(&v)?)),
            None => Ok(None),
        }
    }

    fn get_many(&self, ids: &[[u8; 32]]) -> Vec<Option<IndexEntry>> {
        let database = self.database.as_ref().expect("Database not open");
        // One batched read: better block locality than N point lookups.
        database
            .multi_get(ids)
            .into_iter()
            .map(|r| match r {
                Ok(Some(v)) => match IndexEntry::decode(&v) {
                    Ok(e) => Some(e),
                    Err(e) => {
                        warn!("corrupt index value: {e}");
                        None
                    }
                },
                Ok(None) => None,
                Err(e) => {
                    warn!("index multi_get failed: {e}");
                    None
                }
            })
            .collect()
    }

    fn wipe(&mut self) -> Result<()> {
        // Delete all keys in place using the open handle. This avoids the
        // destroy+reopen approach, which fails when another clone of the DB handle
        // in the same process still holds the file lock (RocksDB allows only one
        // process/handle to own the LOCK at a time).
        let database = self.database.as_ref().expect("Database not open");

        let mut batch = rocksdb::WriteBatch::new();
        for item in database.iterator(IteratorMode::Start) {
            let (k, _) = item.map_err(|e| anyhow!(e))?;
            batch.delete(&k);
        }
        // Reset the persisted count to zero in the same batch; an empty
        // database trivially has a complete time index.
        batch.put(META_COUNT_KEY, 0u64.to_le_bytes());
        batch.put(META_TIMEIDX_KEY, [1u8]);
        database.write(batch).map_err(|e| anyhow!(e))?;

        // Indexer bookkeeping describes data that no longer exists.
        self.clear_meta()?;

        self.item_count.store(0, Ordering::SeqCst);
        self.time_ready.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[0] = n;
        k[31] = n;
        k
    }

    #[test]
    fn time_range_queries() {
        let dir = tempfile::tempdir().unwrap();
        let idx = RocksDbIndex::open(dir.path()).unwrap();
        assert!(idx.time_index_ready());
        idx.insert(id(1), IndexEntry::new(100)).unwrap();
        idx.insert_batch(vec![
            (id(2), IndexEntry::new(200)),
            (id(3), IndexEntry::new(300)),
            (id(4), IndexEntry::new(65_000_000_000)),
        ])
        .unwrap();
        let got = idx.list_ids(150, 300);
        assert_eq!(
            got,
            vec![(id(2), 200), (id(3), 300)],
            "inclusive numeric range in time order"
        );
        assert_eq!(idx.list_ids(0, u64::MAX).len(), 4);
        assert_eq!(idx.count_keys(), 4, "time keys must not inflate the count");
        assert!(idx.contains_key(&id(2)).unwrap());
    }

    #[test]
    fn migration_backfills_legacy_entries() {
        let dir = tempfile::tempdir().unwrap();
        // Simulate a legacy database: primary keys only, no time index flag.
        {
            let db = rocksdb::DB::open_default(dir.path()).unwrap();
            db.put(id(7), 700u64.to_le_bytes()).unwrap();
            db.put(id(8), 800u64.to_le_bytes()).unwrap();
            db.put(META_COUNT_KEY, 2u64.to_le_bytes()).unwrap();
        }
        let idx = RocksDbIndex::open(dir.path()).unwrap();
        // Backfill runs on a background thread.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !idx.time_index_ready() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(idx.time_index_ready(), "migration did not complete");
        assert_eq!(idx.list_ids(700, 800), vec![(id(7), 700), (id(8), 800)]);
        assert_eq!(idx.count_keys(), 2);
        // Legacy 8-byte values decode as v0: known timestamp, unknown location.
        let got = idx.get(&id(7)).unwrap().unwrap();
        assert_eq!(got.created_at, 700);
        assert!(got.loc.is_none());
    }

    #[test]
    fn stores_and_returns_locations() {
        use crate::database::value::{EventLoc, shard_hash};
        let dir = tempfile::tempdir().unwrap();
        let idx = RocksDbIndex::open(dir.path()).unwrap();
        let loc = EventLoc {
            shard: shard_hash("events_20250801.jsonl.zst"),
            offset: 4096,
            len: 512,
        };
        idx.insert(id(1), IndexEntry::located(100, loc)).unwrap();
        idx.insert_batch(vec![(id(2), IndexEntry::new(200))])
            .unwrap();

        assert_eq!(idx.get(&id(1)).unwrap().unwrap().loc, Some(loc));
        assert_eq!(idx.get(&id(2)).unwrap().unwrap().loc, None);
        assert!(idx.get(&id(9)).unwrap().is_none());

        // Located entries must not disturb the time index or the count.
        assert_eq!(idx.list_ids(0, u64::MAX).len(), 2);
        assert_eq!(idx.count_keys(), 2);

        let many = idx.get_many(&[id(1), id(9), id(2)]);
        assert_eq!(many[0].unwrap().loc, Some(loc));
        assert!(many[1].is_none());
        assert_eq!(many[2].unwrap().created_at, 200);
    }
}
