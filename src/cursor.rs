use crate::event::NostrEvent;
use anyhow::{Result, bail};
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, ZstdDecoder};
use async_stream::stream;
use futures::stream::{Stream, StreamExt};
use log::{debug, error, info};
use std::collections::HashSet;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
struct EventId([u8; 32]);

/// A directory cursor over 1 or more JSON-L files
///
/// Skips duplicate events
///
/// Works with compressed files too
pub struct NostrCursor {
    ids: HashSet<EventId>,
    dir: PathBuf,
    parallelism: usize,
}

impl NostrCursor {
    /// Creates a new cursor for reading Nostr events from a directory.
    ///
    /// # Arguments
    ///
    /// * `dir` - Path to the directory containing JSON-L files
    ///
    /// # Default Behavior
    ///
    /// - Files are read sequentially (parallelism = 1)
    /// - Duplicate events are automatically filtered out
    /// - Supports compressed files (.gz, .zst, .bz2)
    ///
    /// # Example
    ///
    /// ```rust
    /// let cursor = NostrCursor::new("./backups".into());
    /// ```
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            ids: HashSet::new(),
            parallelism: 1,
        }
    }

    /// Sets the number of files to read in parallel.
    ///
    /// # Arguments
    ///
    /// * `parallelism` - Number of files to process concurrently
    ///
    /// # Performance Notes
    ///
    /// - Higher parallelism = more memory usage (one buffer per file)
    /// - Recommended: 2-8 for most workloads
    /// - Default is 1 (sequential processing)
    ///
    /// # Example
    ///
    /// ```rust
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_parallelism(4);
    /// ```
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Returns a stream of deduplicated Nostr events from all files in the directory.
    ///
    /// # Behavior
    ///
    /// - Reads files in parallel (up to `parallelism` limit)
    /// - Yields events one at a time (no buffering of entire files)
    /// - Automatically deduplicates events based on event ID
    /// - Skips directories and invalid JSON lines
    /// - Supports compressed formats: .gz, .zst, .bz2, .json, .jsonl
    ///
    /// # Memory Usage
    ///
    /// Memory-efficient for large datasets (300M+ events):
    /// - Events are streamed, not buffered
    /// - Only stores event IDs for deduplication
    /// - One read buffer per parallel file
    ///
    /// # Example
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    ///
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_parallelism(4);
    ///
    /// let mut stream = cursor.walk();
    /// while let Some(event) = stream.next().await {
    ///     // Process event
    /// }
    /// ```
    pub fn walk(mut self) -> impl Stream<Item = NostrEvent> {
        let parallelism = self.parallelism;
        let dir = self.dir.clone();

        stream! {
            let mut dir_reader = match tokio::fs::read_dir(&dir).await {
                Ok(reader) => reader,
                Err(e) => {
                    error!("Failed to read directory: {}", e);
                    return;
                }
            };

            let mut files = Vec::new();
            while let Ok(Some(path)) = dir_reader.next_entry().await {
                if path.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                files.push(path.path());
            }

            // Create a stream of file streams and flatten them with parallelism
            let file_streams = futures::stream::iter(files.into_iter().map(|path| {
                info!("Reading: {}", path.to_str().unwrap());
                Box::pin(Self::read_file_stream(path)) as Pin<Box<dyn Stream<Item = NostrEvent> + Send>>
            }))
            .flatten_unordered(parallelism);

            tokio::pin!(file_streams);

            while let Some(event) = file_streams.next().await {
                let ev_id = match hex::decode(&event.id) {
                    Ok(bytes) => match bytes.as_slice().try_into() {
                        Ok(array) => EventId(array),
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                };

                if self.ids.insert(ev_id) {
                    yield event;
                }
            }
        }
    }

    /// Creates a stream of events from a single file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the JSON-L file (can be compressed)
    ///
    /// # Returns
    ///
    /// A stream that yields events one at a time as they're read from the file.
    /// Invalid JSON lines are logged and skipped.
    fn read_file_stream(path: PathBuf) -> impl Stream<Item = NostrEvent> {
        stream! {
            match Self::open_file_static(path.clone()).await {
                Ok(f) => {
                    let mut file = BufReader::new(f);
                    let mut line = Vec::new();
                    let mut lines = 0u64;
                    let mut events = 0u64;

                    loop {
                        match file.read_until(10, &mut line).await {
                            Ok(size) => {
                                if size == 0 {
                                    info!("EOF. lines={lines}, events={events}");
                                    break;
                                }
                                lines += 1;

                                let line_json = &line[..size];
                                match serde_json::from_slice::<NostrEvent>(line_json) {
                                    Ok(event) => {
                                        events += 1;
                                        yield event;
                                    }
                                    Err(e) => {
                                        debug!(
                                            "Invalid json on {} {}",
                                            String::from_utf8_lossy(line_json),
                                            e
                                        )
                                    }
                                }

                                line.clear();
                            }
                            Err(e) => {
                                error!("Error reading file: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to open file {:?}: {}", path, e);
                }
            }
        }
    }

    /// Opens a file and returns an async reader, automatically handling compression.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file
    ///
    /// # Supported Formats
    ///
    /// - `.json` / `.jsonl` - Uncompressed JSON-L
    /// - `.gz` - Gzip compressed
    /// - `.zst` - Zstandard compressed
    /// - `.bz2` - Bzip2 compressed
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File cannot be opened
    /// - File extension is not recognized
    /// - File has no extension
    async fn open_file_static(path: PathBuf) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        let f = BufReader::new(File::open(path.clone()).await?);
        match path.extension() {
            Some(ext) => match ext.to_str().unwrap() {
                "json" => Ok(Box::pin(f)),
                "jsonl" => Ok(Box::pin(f)),
                "gz" => Ok(Box::pin(GzipDecoder::new(f))),
                "zst" | "zstd" => Ok(Box::pin(ZstdDecoder::new(f))),
                "bz2" => Ok(Box::pin(BzDecoder::new(f))),
                _ => bail!("Unknown extension"),
            },
            None => bail!("Could not determine archive format"),
        }
    }
}
