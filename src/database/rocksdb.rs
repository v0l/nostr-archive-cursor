use anyhow::Result;
use anyhow::anyhow;
use log::{debug, trace};
use nostr_sdk::prelude::DatabaseError;
use nostr_sdk::{EventId, Timestamp};
use rocksdb::{BlockBasedOptions, IteratorMode, Options};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct RocksDbIndex {
    database: Option<Arc<rocksdb::DB>>,
    /// Total number of events in the database
    item_count: Arc<AtomicUsize>,
}

impl RocksDbIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let table_opts = BlockBasedOptions::default();
        opts.set_block_based_table_factory(&table_opts);

        let db = rocksdb::DB::open(&opts, path).map_err(|e| anyhow!(e))?;
        Ok(Self {
            database: Some(Arc::new(db)),
            item_count: Arc::new(AtomicUsize::new(0)),
        })
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

    pub fn list_ids(&self, since: u64, until: u64) -> Vec<(EventId, Timestamp)> {
        let database = self.database.as_ref().expect("Database not open");
        database
            .iterator(IteratorMode::Start)
            .filter_map(|x| {
                if let Ok((k, v)) = x {
                    let v_slice = v.iter().as_slice();
                    let timestamp = if v_slice.len() != 8 {
                        0
                    } else {
                        u64::from_le_bytes(v_slice.try_into().ok()?)
                    };
                    if timestamp >= since && timestamp <= until {
                        Some((EventId::from_slice(&k).ok()?, Timestamp::from(timestamp)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the number of items in the index database
    ///
    /// **WARNING:** Can take a very long time if your index is very large, this operation is O(n)
    pub fn count_keys(&self) -> u64 {
        let database = self.database.as_ref().expect("Database not open");
        let ret = self.item_count.load(Ordering::SeqCst);
        if ret == 0 {
            trace!("Internal count was 0, using index db count (WARNING! O(n))");
            let db_len = database.iterator(IteratorMode::Start).count();
            self.item_count.store(db_len, Ordering::SeqCst);
            db_len as u64
        } else {
            ret as u64
        }
    }

    /// Is the index empty
    pub fn is_index_empty(&self) -> bool {
        let database = self.database.as_ref().expect("Database not open");
        database.iterator(IteratorMode::Start).next().is_none()
    }

    pub fn setup_for_reindex(&mut self) -> Result<()> {
        let path = {
            let db = self.database.take().expect("Database not open");
            let path = db.path().to_owned();
            drop(db);
            path
        };
        let opts = Self::get_bulk_load_options();
        let db = rocksdb::DB::open(&opts, path).map_err(|e| anyhow!(e))?;
        self.database.replace(Arc::new(db));
        Ok(())
    }

    pub fn insert(&self, id: EventId, timestamp: Timestamp) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        database
            .put(id, &timestamp.as_secs().to_le_bytes())
            .map_err(|e| DatabaseError::Backend(Box::new(e)))?;
        self.item_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn insert_batch(&self, items: Vec<(EventId, Timestamp)>) -> Result<()> {
        let database = self.database.as_ref().expect("Database not open");
        let mut batch = rocksdb::WriteBatch::new();
        for (k, v) in items {
            batch.put(k.as_bytes(), &v.as_secs().to_le_bytes());
        }
        let batch_size = batch.len();
        database.write(batch)?;
        self.item_count.fetch_add(batch_size, Ordering::Relaxed);
        Ok(())
    }

    pub fn contains_key(&self, id: &EventId) -> Result<bool> {
        let database = self.database.as_ref().expect("Database not open");
        Ok(database.key_may_exist(id.as_bytes()))
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
        if let Ok(Some(capacity)) = database.property_value("rocksdb.block-cache-capacity") {
            debug!("Block Cache Capacity: {}", format_bytes(&capacity));
        }
        if let Ok(Some(usage)) = database.property_value("rocksdb.block-cache-usage") {
            debug!("Block Cache Usage: {}", format_bytes(&usage));
        }
        if let Ok(Some(pinned)) = database.property_value("rocksdb.block-cache-pinned-usage") {
            debug!("Block Cache Pinned: {}", format_bytes(&pinned));
        }

        // Memtable statistics
        if let Ok(Some(active)) = database.property_value("rocksdb.cur-size-active-mem-table") {
            debug!("Active Memtable Size: {}", format_bytes(&active));
        }
        if let Ok(Some(all_unflushed)) = database.property_value("rocksdb.cur-size-all-mem-tables")
        {
            debug!(
                "All Unflushed Memtables Size: {}",
                format_bytes(&all_unflushed)
            );
        }
        if let Ok(Some(all_mem)) = database.property_value("rocksdb.size-all-mem-tables") {
            debug!("All Memtables (incl. pinned): {}", format_bytes(&all_mem));
        }

        // Table readers (index/filter blocks outside block cache)
        if let Ok(Some(table_readers)) =
            database.property_value("rocksdb.estimate-table-readers-mem")
        {
            debug!(
                "Estimated Table Readers Memory: {}",
                format_bytes(&table_readers)
            );
        }

        // Live data and SST files
        if let Ok(Some(live_data)) = database.property_value("rocksdb.estimate-live-data-size") {
            debug!("Estimated Live Data Size: {}", format_bytes(&live_data));
        }
        if let Ok(Some(sst_size)) = database.property_value("rocksdb.live-sst-files-size") {
            debug!("Live SST Files Size: {}", format_bytes(&sst_size));
        }

        // Key count estimate
        if let Ok(Some(num_keys)) = database.property_value("rocksdb.estimate-num-keys") {
            debug!("Estimated Number of Keys: {}", num_keys);
        }

        // Pending operations
        if let Ok(Some(pending_compact)) =
            database.property_value("rocksdb.estimate-pending-compaction-bytes")
        {
            debug!(
                "Pending Compaction Bytes: {}",
                format_bytes(&pending_compact)
            );
        }
        if let Ok(Some(flush_pending)) = database.property_value("rocksdb.mem-table-flush-pending")
        {
            debug!("Memtable Flush Pending: {}", flush_pending);
        }

        debug!("============================");
    }
}
