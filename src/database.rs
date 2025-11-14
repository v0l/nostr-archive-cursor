use crate::NostrCursor;
use anyhow::{Result, anyhow};
use async_compression::tokio::write::ZstdEncoder;
use chrono::{DateTime, NaiveDate, Utc};
use log::{debug, error, info, trace, warn};
use nostr_sdk::prelude::{
    Backend, BoxedFuture, DatabaseError, DatabaseEventStatus, Events, NostrDatabase,
    RejectedReason, SaveEventStatus,
};
use nostr_sdk::{Event, EventId, Filter, JsonUtil, Timestamp};
use std::fmt::{Debug, Formatter};
use std::fs::create_dir_all;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Flat JSON-L file database for nostr_sdk
#[derive(Clone)]
pub struct JsonFilesDatabase {
    /// Directory where flat files are contained
    out_dir: PathBuf,
    /// Event id index database
    database: sled::Db,
    /// Current file being written to
    file: Arc<Mutex<FlatFileWriter>>,
    /// Total number of events in the database
    item_count: Arc<AtomicUsize>,
}

impl Debug for JsonFilesDatabase {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveFile {
    pub path: PathBuf,
    pub size: u64,
    pub created: DateTime<Utc>,
    /// The actual date of the file
    pub timestamp: DateTime<Utc>,
}

impl JsonFilesDatabase {
    pub fn new(dir: PathBuf) -> Result<Self> {
        create_dir_all(&dir)?;
        let db = sled::open(dir.join("index"))?;
        Ok(Self {
            out_dir: dir.clone(),
            item_count: Arc::new(AtomicUsize::new(0)),
            database: db,
            file: Arc::new(Mutex::new(FlatFileWriter {
                dir,
                current_date: Utc::now(),
                current_handle: None,
            })),
        })
    }

    pub async fn list_files(&self) -> Result<Vec<ArchiveFile>> {
        let mut list = tokio::fs::read_dir(&self.out_dir).await?;
        let mut files = Vec::new();
        while let Ok(Some(entry)) = list.next_entry().await {
            if entry.file_type().await?.is_dir() {
                continue;
            }

            let parsed_date = if let Some(d) = FlatFileWriter::parse_timestamp(&entry.path()) {
                d
            } else {
                continue;
            };

            let meta = entry.metadata().await?;
            files.push(ArchiveFile {
                path: entry.path(),
                size: meta.len(),
                created: meta.created()?.into(),
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
            let parsed_date =
                FlatFileWriter::parse_timestamp(&p).ok_or(anyhow!("Filename invalid"))?;
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

    /// List key/value pairs from the index database
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
        if ret == 0 {
            trace!("Internal count was 0, using index db count (WARNING! O(n))");
            let db_len = self.database.len();
            self.item_count.store(db_len, Ordering::SeqCst);
            db_len as u64
        } else {
            ret as u64
        }
    }

    /// Is the index empty
    pub fn is_index_empty(&self) -> bool {
        self.database.is_empty()
    }

    /// Rebuilt event id index
    pub async fn rebuild_index(&mut self) -> Result<()> {
        self.database.clear()?;

        let db = self.database.clone();
        NostrCursor::new(self.out_dir.clone())
            .with_parallelism(4)
            .with_dedupe(false)
            .walk_with(move |event| {
                let db = db.clone();
                Box::pin(async move {
                    if let Ok(id) = hex::decode(&event.id)
                        && let Err(e) = db.insert(id, &event.created_at.to_le_bytes())
                    {
                        warn!(
                            "Failed to insert event into index {} {}",
                            serde_json::to_string(&event).unwrap_or_default(),
                            e
                        );
                    }
                })
            })
            .await;

        Ok(())
    }
}

impl NostrDatabase for JsonFilesDatabase {
    fn backend(&self) -> Backend {
        Backend::Custom("JsonFileDatabase".to_owned())
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            match self.check_id(&event.id).await? {
                DatabaseEventStatus::NotExistent => {
                    self.database
                        .insert(event.id, &event.created_at.as_secs().to_le_bytes())
                        .map_err(|e| DatabaseError::Backend(Box::new(e)))?;

                    let mut fl = self.file.lock().await;
                    fl.write_event(event).await.map_err(|e| {
                        DatabaseError::Backend(Box::new(Error::new(ErrorKind::Other, e)))
                    })?;
                    self.item_count.fetch_add(1, Ordering::SeqCst);
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
                .contains_key(event_id)
                .map_err(|e| DatabaseError::Backend(Box::new(e)))?
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

pub struct FlatFileWriter {
    pub dir: PathBuf,
    pub current_date: DateTime<Utc>,
    pub current_handle: Option<(PathBuf, File)>,
}

impl FlatFileWriter {
    pub const EVENT_FORMAT: &'static str = "%Y%m%d";

    /// Spawn a task to compress a file
    async fn compress_file(file: PathBuf) -> Result<()> {
        let out_path = file.with_extension("jsonl.zst");
        let mut in_file = File::open(file.clone()).await?;
        {
            let out_file = File::create(out_path.clone()).await?;
            let mut enc = ZstdEncoder::new(out_file);
            let mut buf: [u8; 1024] = [0; 1024];
            while let Ok(n) = in_file.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                enc.write_all(&buf[..n]).await?;
            }
            enc.shutdown().await?;
        }

        let in_size = in_file.metadata().await?.len();
        let out_size = File::open(out_path).await?.metadata().await?.len();
        drop(in_file);
        tokio::fs::remove_file(file).await?;
        info!(
            "Compressed file ratio={:.2}x, size={}M",
            in_size as f32 / out_size as f32,
            out_size as f32 / 1024.0 / 1024.0
        );

        Ok(())
    }

    /// Write event to the current file handle, or move to the next file handle
    pub(crate) async fn write_event(&mut self, ev: &Event) -> Result<()> {
        let now = Utc::now();
        if self.current_date.format(Self::EVENT_FORMAT).to_string()
            != now.format(Self::EVENT_FORMAT).to_string()
        {
            if let Some((path, ref mut handle)) = self.current_handle.take() {
                handle.flush().await?;
                info!("Closing file {:?}", &path);
                tokio::spawn(async move {
                    if let Err(e) = Self::compress_file(path).await {
                        error!("Failed to compress file: {}", e);
                    }
                });
            }

            // open new file
            self.current_date = now;
        }

        if self.current_handle.is_none() {
            let path = self.dir.join(format!(
                "events_{}.jsonl",
                self.current_date.format(Self::EVENT_FORMAT)
            ));
            info!("Creating file {:?}", &path);
            self.current_handle = Some((
                path.clone(),
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .await?,
            ));
        }

        if let Some((_path, handle)) = self.current_handle.as_mut() {
            handle.write_all(ev.as_json().as_bytes()).await?;
            handle.write(b"\n").await?;
        }
        Ok(())
    }

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
}
