#![cfg(feature = "db-rocksdb")]
use crate::IndexDb;
use anyhow::anyhow;
use anyhow::Result;
use log::{debug, warn};
use rocksdb::properties::{
    BLOCK_CACHE_CAPACITY, BLOCK_CACHE_PINNED_USAGE, BLOCK_CACHE_USAGE, CUR_SIZE_ACTIVE_MEM_TABLE,
    CUR_SIZE_ALL_MEM_TABLES, ESTIMATE_LIVE_DATA_SIZE, ESTIMATE_NUM_KEYS,
    ESTIMATE_PENDING_COMPACTION_BYTES, ESTIMATE_TABLE_READERS_MEM, LIVE_SST_FILES_SIZE,
    MEM_TABLE_FLUSH_PENDING, SIZE_ALL_MEM_TABLES,
};
use rocksdb::{BlockBasedOptions, IteratorMode, Options};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = rocksdb::DB::open_default(path).map_err(|e| anyhow!(e))?;

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
                            if k.len() != 32 || v.len() != 8 {
                                continue;
                            }
                            let id: &[u8; 32] = k.as_ref().try_into().unwrap();
                            let ts = u64::from_le_bytes(v.as_ref().try_into().unwrap());
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

        // Sanity-check the cached count against RocksDB's key estimate. If a
        // concurrent writer / crash left the persisted count badly wrong (e.g. reset
        // to 0 during a k8s rollout while data remained), the two will diverge far
        // beyond normal estimate noise. Auto-repair in that case so a restart
        // recovers instead of trusting the stale count forever.
        index.auto_repair_count_if_diverged();

        Ok(index)
    }

    /// Compare the cached count with `ESTIMATE_NUM_KEYS` and run `repair_count()`
    /// when they disagree by more than the threshold. Returns true if a repair ran.
    ///
    /// `ESTIMATE_NUM_KEYS` is an approximation and also counts the meta key, so we
    /// only treat large divergences as corruption: the difference must exceed both
    /// a relative ratio of the estimate and an absolute floor (to avoid false
    /// positives on tiny databases where the estimate is unreliable).
    fn auto_repair_count_if_diverged(&self) -> bool {
        const REL_THRESHOLD: f64 = 0.10; // >10% divergence
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

        let diverged = diff > ABS_THRESHOLD && (diff as f64) > REL_THRESHOLD * (estimate.max(1) as f64);

        if diverged {
            warn!(
                "Event index count looks stale (cached={cached}, estimate={estimate}); \
                 running repair_count() to rescan."
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

        // Moderate memtable sizes to control memory usage
        // With N threads writing, memory can spike quickly
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MiB per memtable
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);

        // Trigger compaction/flush sooner to prevent memory buildup
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(8);
        opts.set_level_zero_stop_writes_trigger(12);

        // More background threads for compaction
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4);
        opts.increase_parallelism(parallelism);
        opts.set_max_background_jobs(parallelism.min(8));

        // Optimize for bulk loading
        opts.prepare_for_bulk_load();

        let table_opts = BlockBasedOptions::default();
        opts.set_block_based_table_factory(&table_opts);

        opts
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

    fn insert(&self, k: [u8; 32], v: [u8; 8]) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        let new_count = self.item_count.fetch_add(1, Ordering::Relaxed) + 1;
        let mut batch = rocksdb::WriteBatch::new();
        batch.put(time_key(&k, u64::from_le_bytes(v)), []);
        batch.put(k, v);
        batch.put(META_COUNT_KEY, (new_count as u64).to_le_bytes());
        database.write(batch).map_err(|e| anyhow!(e))?;
        Ok(())
    }

    fn insert_batch(&self, items: Vec<([u8; 32], [u8; 8])>) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        let mut batch = rocksdb::WriteBatch::new();
        let n = items.len();
        for (k, v) in items {
            batch.put(time_key(&k, u64::from_le_bytes(v)), []);
            batch.put(k, v);
        }
        let new_count = self.item_count.fetch_add(n, Ordering::Relaxed) + n;
        batch.put(META_COUNT_KEY, (new_count as u64).to_le_bytes());
        database.write(batch)?;
        Ok(())
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
        idx.insert(id(1), 100u64.to_le_bytes()).unwrap();
        idx.insert_batch(vec![
            (id(2), 200u64.to_le_bytes()),
            (id(3), 300u64.to_le_bytes()),
            (id(4), 65_000_000_000u64.to_le_bytes()),
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
    }
}
