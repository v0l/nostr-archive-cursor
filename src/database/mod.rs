use anyhow::{Result, anyhow};
use chrono::{DateTime, NaiveDate, Utc};
use log::{debug, warn};
use nostr_sdk::prelude::{
    Backend, BoxedFuture, DatabaseError, DatabaseEventStatus, Events, NostrDatabase,
    RejectedReason, SaveEventStatus,
};
use nostr_sdk::{Event, EventId, Filter, Timestamp};
use std::fmt::{Debug, Formatter};
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::error::TryRecvError;

mod file;
pub use file::*;
mod rocksdb;
#[cfg(feature = "db-rocksdb")]
pub use crate::database::rocksdb::*;
mod sled;
#[cfg(feature = "db-sled")]
pub use crate::database::sled::*;

/// KV index database for tracking event ids + timestamps
pub trait IndexDb: Clone + Send + Sync {
    /// List entries by V range
    fn list_ids<'a>(&'a self, min: &[u8; 8], max: &[u8; 8]) -> Vec<(&'a [u8; 32], &'a [u8; 8])>;
    fn count_keys(&self) -> u64;
    fn contains_key(&self, id: &[u8; 32]) -> Result<bool>;
    fn is_index_empty(&self) -> bool;
    /// Reconfigure the database for faster bulk loading
    fn setup_for_reindex(&mut self) -> Result<()>;
    fn insert(&self, k: [u8; 32], v: [u8; 8]) -> Result<()>;
    fn insert_batch(&self, items: Vec<([u8; 32], [u8; 8])>) -> Result<()>;
    fn wipe(&mut self) -> Result<()>;
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
    tx_writer: tokio::sync::mpsc::UnboundedSender<Event>,
}

impl<D> Debug for JsonFilesDatabase<D> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl<D> JsonFilesDatabase<D> {
    pub const EVENT_FORMAT: &'static str = "%Y%m%d";

    pub fn new_with_index<P>(dir: P, index: D) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        let dir = PathBuf::from(&dir);
        create_dir_all(&dir)?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dir_writer = dir.clone();
        let _ = std::thread::Builder::new()
            .name("JsonFilesDatabase::writer".into())
            .spawn(move || {
                let mut current_path = Self::get_archive_path(&dir_writer, &Utc::now());
                let mut writer =
                    CompressedJsonLFile::new(&current_path).expect("Failed to open archive");
                loop {
                    match rx.try_recv() {
                        Ok(e) => {
                            // swap files if current path is different
                            let current = Self::get_archive_path(&dir_writer, &Utc::now());
                            if current != current_path {
                                writer = CompressedJsonLFile::new(&current)
                                    .expect("Failed to open archive");
                                current_path = current;
                            }

                            writer
                                .write_event(&e)
                                .expect("Failed to write event to archive");
                        }
                        Err(p) => match p {
                            TryRecvError::Empty => {
                                // sleep if no data
                                std::thread::sleep(std::time::Duration::from_millis(100));
                            }
                            TryRecvError::Disconnected => {
                                break;
                            }
                        },
                    }
                }
            });
        Ok(Self {
            out_dir: dir,
            database: index,
            tx_writer: tx,
        })
    }

    pub fn get_archive_path(base: &Path, time: &DateTime<Utc>) -> PathBuf {
        PathBuf::from(base.join(format!(
            "events_{}.jsonl.zst",
            time.format(Self::EVENT_FORMAT)
        )))
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
    /// List key/value pairs from the index database
    pub fn list_ids(&self, since: u64, until: u64) -> Vec<(EventId, Timestamp)> {
        self.database
            .list_ids(&since.to_le_bytes(), &until.to_le_bytes())
            .into_iter()
            .filter_map(|(k, v)| {
                Some((
                    EventId::from_slice(k).ok()?,
                    Timestamp::from_secs(u64::from_le_bytes(v.as_slice().try_into().ok()?)),
                ))
            })
            .collect()
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

    /// Rebuilt event id index using parallel std::thread workers.
    ///
    /// This method uses OS threads for true CPU parallelism, which is significantly
    /// faster than the async version for CPU-bound workloads like JSON parsing.
    #[cfg(feature = "sync")]
    pub fn rebuild_index(&mut self) -> Result<()> {
        self.database.wipe()?;
        self.database.setup_for_reindex()?;
        let db = self.database.clone();
        crate::NostrCursor::new(self.out_dir.clone())
            .with_max_parallelism()
            .walk_with_chunked_sync(
                move |events| {
                    let mut batch = Vec::with_capacity(events.len());
                    let mut id = [0u8; 32];
                    for event in events {
                        if event.id.len() == 64
                            && faster_hex::hex_decode(event.id.as_bytes(), &mut id).is_ok()
                        {
                            batch.push((id, event.created_at.to_le_bytes()));
                        }
                    }
                    if let Err(e) = db.insert_batch(batch) {
                        warn!("Failed to apply index update: {}", e);
                    }
                },
                1000,
            );

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
            match self
                .database
                .contains_key(event.id.as_bytes())
                .map_err(|e| DatabaseError::Backend(e.into_boxed_dyn_error()))?
            {
                false => {
                    self.database
                        .insert(
                            event.id.as_bytes().clone(),
                            event.created_at.as_secs().to_le_bytes(),
                        )
                        .map_err(|e| DatabaseError::Backend(e.into_boxed_dyn_error()))?;

                    self.tx_writer
                        .send(event.clone())
                        .map_err(|e| DatabaseError::Backend(Box::new(e)))?;

                    debug!("Saved event: {}", event.id);
                    Ok(SaveEventStatus::Success)
                }
                _ => Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate)),
            }
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            if self
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
        _event_id: &EventId,
    ) -> BoxedFuture<'_, Result<Option<Event>, DatabaseError>> {
        Box::pin(async move { Ok(None) })
    }

    fn count(&self, _filters: Filter) -> BoxedFuture<'_, Result<usize, DatabaseError>> {
        Box::pin(async move { Ok(0) })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<'_, Result<Events, DatabaseError>> {
        Box::pin(async move { Ok(Events::new(&filter)) })
    }

    fn delete(&self, _filter: Filter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }

    fn wipe(&self) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(feature = "db-rocksdb")]
pub type DefaultJsonFilesDatabase = JsonFilesDatabase<rocksdb::RocksDbIndex>;
#[cfg(feature = "db-rocksdb")]
impl DefaultJsonFilesDatabase {
    pub fn new<P>(path: P) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        let p = PathBuf::from(&path);
        let db = rocksdb::RocksDbIndex::open(p.join("index-rocksdb"))?;
        JsonFilesDatabase::new_with_index(path, db)
    }
}

#[cfg(not(feature = "db-rocksdb"))]
pub type DefaultJsonFilesDatabase = JsonFilesDatabase<sled::SledIndex>;
#[cfg(not(feature = "db-rocksdb"))]
impl DefaultJsonFilesDatabase {
    pub fn new<P>(path: P) -> Result<Self>
    where
        for<'a> PathBuf: From<&'a P>,
    {
        let p = PathBuf::from(&path);
        let db = sled::SledIndex::open(p.join("index-sled"))?;
        JsonFilesDatabase::new_with_index(path, db)
    }
}
