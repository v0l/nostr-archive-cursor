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
    fn list_ids<'a>(&'a self, min: &[u8; 8], max: &[u8; 8]) -> Vec<(&'a [u8; 32], &'a [u8; 8])> {
        self.database
            .iter()
            .filter_map(|x| {
                if let Ok((k, v)) = x {
                    // skip invalid data
                    if k.len() != 32 || v.len() != 8 {
                        warn!("Invalid KV entry in rocksdb: {:?} => {:?}", k, v);
                        return None;
                    }
                    let k = unsafe { &*(k.as_slice().as_ptr() as *const [u8; 32]) };
                    let v = unsafe { &*(v.as_slice().as_ptr() as *const [u8; 8]) };
                    if v > min && v < max {
                        Some((k, v))
                    } else {
                        None
                    }
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
}
