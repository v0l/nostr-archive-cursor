#![cfg(feature = "db-sled")]
use crate::IndexDb;
use anyhow::Result;
use anyhow::anyhow;
use log::warn;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use zstd::zstd_safe::WriteBuf;

#[derive(Clone)]
pub struct SledIndex {
    database: sled::Db,
    /// Total number of events in the database
    item_count: Arc<AtomicUsize>,
}

impl SledIndex {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let db = sled::open(path).map_err(|e| anyhow!(e))?;
        let db_len = db.len();
        Ok(Self {
            database: db,
            item_count: Arc::new(AtomicUsize::new(db_len)),
        })
    }
}

impl IndexDb for SledIndex {
    fn list_ids(&self, min: u64, max: u64) -> Vec<([u8; 32], u64)> {
        // Full scan with numeric comparison. Sled has no secondary time
        // index; this backend is not used for large archives.
        self.database
            .iter()
            .filter_map(|x| {
                let (k, v) = x.ok()?;
                if k.len() != 32 || v.len() != 8 {
                    return None;
                }
                let ts = u64::from_le_bytes(v.as_ref().try_into().ok()?);
                if ts >= min && ts <= max {
                    Some((k.as_ref().try_into().ok()?, ts))
                } else {
                    None
                }
            })
            .collect()
    }

    fn count_keys(&self) -> u64 {
        let ret = self.item_count.load(Ordering::Relaxed);
        ret as u64
    }

    fn contains_key(&self, id: &[u8; 32]) -> Result<bool> {
        self.database.contains_key(id).map_err(|e| anyhow!(e))
    }

    fn is_index_empty(&self) -> bool {
        self.database.is_empty()
    }

    fn setup_for_reindex(&mut self) -> Result<()> {
        Ok(())
    }

    fn insert(&self, k: [u8; 32], v: [u8; 8]) -> Result<()> {
        self.database.insert(k.as_slice(), v.as_slice())?;
        self.item_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn insert_batch(&self, items: Vec<([u8; 32], [u8; 8])>) -> Result<()> {
        let mut batch = sled::Batch::default();
        let len = items.len();
        for (k, v) in items {
            batch.insert(k.as_slice(), v.as_slice());
        }
        self.database.apply_batch(batch)?;
        self.item_count.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    fn wipe(&mut self) -> Result<()> {
        self.database.clear().map_err(|e| anyhow!(e))
    }

    fn repair_count(&self) -> Result<u64> {
        // sled has no separate meta count; rescan real (32-byte) keys.
        let count = self
            .database
            .iter()
            .filter(|x| x.as_ref().map(|(k, _)| k.len() == 32).unwrap_or(false))
            .count();
        self.item_count.store(count, Ordering::SeqCst);
        Ok(count as u64)
    }
}
