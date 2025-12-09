use anyhow::Result;
use anyhow::anyhow;
use log::trace;
use nostr_sdk::prelude::DatabaseError;
use nostr_sdk::{EventId, Timestamp};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct SledIndex {
    database: sled::Db,
    /// Total number of events in the database
    item_count: Arc<AtomicUsize>,
}

impl SledIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let db = sled::open(path).map_err(|e| anyhow!(e))?;
        let db_len = db.len();
        Ok(Self {
            database: db,
            item_count: Arc::new(AtomicUsize::new(db_len)),
        })
    }

    pub fn list_ids(&self, since: u64, until: u64) -> Vec<(EventId, Timestamp)> {
        self.database
            .iter()
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
        let ret = self.item_count.load(Ordering::SeqCst);
        ret as u64
    }

    /// Is the index empty
    pub fn is_index_empty(&self) -> bool {
        self.database.is_empty()
    }

    pub fn clear(&self) -> Result<()> {
        self.database.clear().map_err(|e| anyhow!(e))
    }

    pub fn insert(&self, id: EventId, timestamp: Timestamp) -> Result<()> {
        self.database
            .insert(id, &timestamp.as_secs().to_le_bytes())
            .map_err(|e| DatabaseError::Backend(Box::new(e)))?;
        self.item_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn insert_batch(&self, items: Vec<(EventId, Timestamp)>) -> Result<()> {
        let mut batch = sled::Batch::default();
        let len = items.len();
        for (k, v) in items {
            batch.insert(k.as_bytes(), &v.as_secs().to_le_bytes());
        }
        self.database.apply_batch(batch)?;
        self.item_count.fetch_add(len, Ordering::SeqCst);
        Ok(())
    }

    pub fn contains_key(&self, id: &EventId) -> Result<bool> {
        self.database.contains_key(id).map_err(|e| anyhow!(e))
    }
}
