use std::path::PathBuf;

#[cfg(feature = "stream")]
use crate::NostrEvent;
#[cfg(feature = "stream")]
use crate::reader::not_sync::ChunkedJsonReader;
#[cfg(feature = "stream")]
use futures::{Stream, StreamExt};
#[cfg(feature = "stream")]
use log::{debug, error, info};
#[cfg(feature = "stream")]
use std::collections::HashSet;
#[cfg(feature = "stream")]
use std::pin::Pin;

#[cfg(any(feature = "sync", feature = "async"))]
#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId(pub [u8; 32]);

/// Fast hex decoding of event IDs using faster-hex
#[cfg(any(feature = "sync", feature = "async"))]
#[inline]
pub fn decode_event_id(hex_str: &str) -> Result<EventId, ()> {
    if hex_str.len() != 64 {
        return Err(());
    }
    let mut bytes = [0u8; 32];
    faster_hex::hex_decode(hex_str.as_bytes(), &mut bytes).map_err(|_| ())?;
    Ok(EventId(bytes))
}

/// A directory cursor over 1 or more JSON-L files
///
/// Skips duplicate events
///
/// Works with compressed files too
/// Largest buffer capacity a pooled read buffer may retain between chunks.
/// Buffers that grew beyond this (rare oversized events) are reallocated small.
const MAX_RETAINED_BUFFER: usize = 64 * 1024;

/// An event plus where its JSON object starts in the decompressed stream.
#[cfg(feature = "sync")]
pub struct LocatedEvent<'a> {
    pub event: crate::NostrEventBorrowed<'a>,
    /// Byte offset of the opening `{` in the decompressed file.
    pub offset: u64,
    /// Length of the JSON object in bytes.
    pub len: u32,
}

/// Is this read error just "the stream ends here"?
///
/// The live shard's last zstd frame is deliberately left open (the writer
/// block-flushes so readers see events immediately without cutting a frame per
/// batch), and archives can also be truncated mid-write. Both surface as a
/// decode error at EOF and mean "no more events", not "corrupt archive".
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn is_end_of_stream(e: &std::io::Error) -> bool {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        return true;
    }
    let msg = e.to_string();
    msg.contains("incomplete frame") || msg.contains("Src size is incorrect")
}

/// Files an archive walk must ignore: our own frame-index sidecars, and empty
/// files (a freshly rotated shard whose writer has not flushed yet).
#[cfg(any(feature = "sync", feature = "async"))]
pub(crate) fn is_walkable_archive(path: &std::path::Path) -> bool {
    if path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.ends_with(".frames") || n.ends_with(".frames.tmp"))
        .unwrap_or(false)
    {
        return false;
    }
    match std::fs::metadata(path) {
        Ok(m) => m.len() > 0,
        Err(_) => false,
    }
}

pub struct NostrCursor {
    /// Directory to read archives from
    dir: PathBuf,
    /// When set, only these files are walked instead of everything in `dir`.
    /// Used by incremental indexing to visit just the shards that changed.
    only: Option<Vec<PathBuf>>,
    /// Number of files to process in parallel
    parallelism: usize,
    /// If deduplication should be performed
    dedupe: bool,
}

/// Smallest archive worth splitting: a segment costs one extra frame of
/// decode, and the work queue already balances across files.
const MIN_SPLIT_BYTES: u64 = 1 << 30;

/// One unit of reading work: a whole file, or a slice of one.
///
/// A single archive can hold a fifth of a corpus -- one shard here is
/// 149 GB -- and parallelising across files leaves every worker but one
/// idle for hours at the end of a pass. A framed zstd archive can be split
/// on its frame boundaries, which is exactly what the frame sidecar records
/// and why it exists.
#[derive(Clone, Debug)]
pub(crate) struct Segment {
    pub path: PathBuf,
    /// Where to start decoding, as a compressed byte offset. Deliberately
    /// one frame *before* `start`, so the reader meets a line break and
    /// resynchronises before it reaches the bytes this segment owns.
    pub seek_to: u64,
    /// Decompressed offset of the first byte this segment owns.
    pub decode_from: u64,
    /// First decompressed offset this segment does *not* own.
    pub start: u64,
    pub end: Option<u64>,
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
    /// ```rust,no_run
    /// use nostr_archive_cursor::NostrCursor;
    ///
    /// let cursor = NostrCursor::new("./backups".into());
    /// ```
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            only: None,
            parallelism: 1,
            dedupe: true,
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
    /// ```rust,no_run
    /// use nostr_archive_cursor::NostrCursor;
    ///
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_parallelism(4);
    /// ```
    /// Walk only these files (they must live in the cursor's directory).
    ///
    /// Lets an incremental indexer re-read just the shards whose size or mtime
    /// changed, instead of the whole archive.
    pub fn with_files(mut self, files: Vec<PathBuf>) -> Self {
        self.only = Some(files);
        self
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Sets parallelism to the number of available CPU cores.
    ///
    /// This provides a convenient way to maximize parallel processing
    /// without manually specifying the core count.
    ///
    /// # Performance Notes
    ///
    /// - Uses `std::thread::available_parallelism()` to detect CPU cores
    /// - Falls back to 1 if CPU count cannot be determined
    /// - May not be optimal for I/O-bound workloads (consider manual tuning)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use nostr_archive_cursor::NostrCursor;
    ///
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_max_parallelism();
    /// ```
    pub fn with_max_parallelism(mut self) -> Self {
        self.parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        self
    }

    /// Controls whether duplicate events should be filtered.
    ///
    /// # Arguments
    ///
    /// * `dedupe` - Set to `true` to enable deduplication, `false` to disable
    ///
    /// # Default Behavior
    ///
    /// Deduplication is enabled by default. When enabled, events are deduplicated
    /// based on their event ID, ensuring each unique event is only yielded once.
    ///
    /// # Performance Notes
    ///
    /// - **Enabled**: Event IDs are stored in memory (32 bytes per unique event)
    /// - **Disabled**: No memory overhead, but duplicate events may be processed
    /// - Disable deduplication if you're certain your data has no duplicates or if
    ///   you want to handle deduplication yourself
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use nostr_archive_cursor::NostrCursor;
    ///
    /// // Disable deduplication for faster processing when duplicates aren't a concern
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_dedupe(false);
    /// ```
    pub fn with_dedupe(mut self, dedupe: bool) -> Self {
        self.dedupe = dedupe;
        self
    }
}

#[cfg(feature = "stream")]
impl NostrCursor {
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
    pub fn walk(self) -> impl Stream<Item = NostrEvent> {
        let parallelism = self.parallelism;
        let dir = self.dir.clone();

        use async_stream::stream;
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
                if !is_walkable_archive(&path.path()) {
                    continue;
                }
                files.push(path.path());
            }

            // Create a stream of file streams and flatten them with parallelism
            let total_files = files.len();
            let file_streams = futures::stream::iter(files.into_iter().enumerate().map(|(idx, path)| {
                info!("Reading [{}/{}]: {}", idx + 1, total_files, path.display());
                Box::pin(Self::read_file_stream(path)) as Pin<Box<dyn Stream<Item = NostrEvent> + Send>>
            }))
            .flatten_unordered(parallelism);

            tokio::pin!(file_streams);

            let mut ids = HashSet::new();
            while let Some(event) = file_streams.next().await {
                let ev_id = match decode_event_id(&event.id) {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                if !self.dedupe || ids.insert(ev_id) {
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
        async_stream::stream! {
            match Self::open_file_static(path.clone()).await {
                Ok(f) => {
                    let mut reader = ChunkedJsonReader::new(f);
                    let mut buffer = Vec::new();
                    let mut objects = 0u64;
                    let mut events = 0u64;

                    loop {
                        match reader.read_json_object(&mut buffer).await {
                            Ok(size) => {
                                if size == 0 {
                                    info!("EOF. objects={objects}, events={events}");
                                    break;
                                }
                                objects += 1;

                                match serde_json::from_slice::<NostrEvent>(&buffer) {
                                    Ok(event) => {
                                        events += 1;
                                        yield event;
                                    }
                                    Err(e) => {
                                        debug!(
                                            "Invalid json on {} {}",
                                            String::from_utf8_lossy(&buffer),
                                            e
                                        )
                                    }
                                }
                            }
                            Err(e) => {
                                if is_end_of_stream(&e) {
                                    info!("EOF (open/truncated frame). objects={objects}, events={events}");
                                } else {
                                    error!("Error reading file: {}", e);
                                }
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
}

#[cfg(feature = "async")]
impl NostrCursor {
    /// Processes all files in parallel, calling the provided async callback for batches of events.
    ///
    /// This chunked version reads multiple lines at once and passes them as a batch to the callback,
    /// providing better performance than processing one event at a time.
    ///
    /// # Arguments
    ///
    /// * `callback` - An async function called for each batch of events with zero-copy borrowed data.
    ///   Must be `Fn` (not `FnMut`) to allow concurrent calls from multiple file readers.
    ///   Use interior mutability (e.g., `Mutex`) if you need to mutate shared state.
    /// * `chunk_size` - Number of lines to read per chunk (default: 1000)
    ///
    /// # Behavior
    ///
    /// - Reads up to `parallelism` files concurrently
    /// - Reads `chunk_size` lines per batch
    /// - Automatically deduplicates events based on event ID
    /// - Callback is invoked in parallel from multiple file readers
    /// - Events are passed as borrowed data (zero-copy) - convert with `.to_owned()` if needed
    /// - Waits for all files to complete before returning
    ///
    /// # Performance
    ///
    /// Significantly faster than `walk_with()` because:
    /// - Reduces I/O overhead by reading multiple lines at once
    /// - Allows batch processing in the callback
    /// - All events in a batch borrow from the same buffer
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::{Arc, Mutex};
    ///
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_parallelism(4);
    ///
    /// let counter = Arc::new(Mutex::new(0));
    /// let counter_clone = counter.clone();
    ///
    /// cursor.walk_with_chunked(move |events| {
    ///     let counter = counter_clone.clone();
    ///     async move {
    ///         // Process batch of borrowed events in parallel (async)
    ///         let mut count = counter.lock().unwrap();
    ///         *count += events.len();
    ///     }
    /// }, 1000).await;
    /// ```
    pub async fn walk_with_chunked<F>(self, callback: F, chunk_size: usize)
    where
        F: for<'a> Fn(
                Vec<crate::event::NostrEventBorrowed<'a>>,
            ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync
            + Clone,
    {
        use dashmap::DashMap;
        use futures::StreamExt;
        use futures::stream::FuturesUnordered;
        use std::pin::Pin;
        use std::sync::Arc;

        let dir = self.dir.clone();
        let parallelism = self.parallelism;

        let mut dir_reader = match tokio::fs::read_dir(&dir).await {
            Ok(reader) => reader,
            Err(e) => {
                log::error!("Failed to read directory: {}", e);
                return;
            }
        };

        let mut files = Vec::new();
        while let Ok(Some(path)) = dir_reader.next_entry().await {
            if path.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            if !is_walkable_archive(&path.path()) {
                continue;
            }
            files.push(path.path());
        }

        // Shared deduplication state - only allocate if dedupe is enabled
        let ids: Option<Arc<DashMap<EventId, ()>>> = if self.dedupe {
            Some(Arc::new(DashMap::new()))
        } else {
            None
        };

        // Use FuturesUnordered for dynamic work distribution
        let mut tasks: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> =
            FuturesUnordered::new();
        let total_files = files.len();
        let mut file_iter = files.into_iter().enumerate();

        let mut get_next_task = || {
            if let Some((idx, path)) = file_iter.next() {
                let callback = callback.clone();
                let ids = ids.clone();

                Some(Box::pin(async move {
                    log::info!("Reading [{}/{}]: {}", idx + 1, total_files, path.display());
                    Self::read_file_with_callback_chunked(path, callback, ids, chunk_size).await;
                }))
            } else {
                None
            }
        };

        // Start initial batch of tasks up to parallelism limit
        for _ in 0..parallelism {
            if let Some(t) = get_next_task() {
                tasks.push(t);
            }
        }

        // As each task completes, immediately start a new one from the remaining files
        while tasks.next().await.is_some() {
            if let Some(t) = get_next_task() {
                tasks.push(t);
            }
        }
    }

    /// Reads a single file in chunks and invokes the callback with batches of borrowed events.
    ///
    /// Reads multiple lines into a single buffer, collects slices pointing to each line,
    /// then parses all events (which borrow from the buffer) and passes them as a batch.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to read
    /// * `callback` - Async function receiving a slice of borrowed events per chunk
    /// * `ids` - Optional shared deduplication map
    /// * `chunk_size` - Number of lines to read per chunk
    async fn read_file_with_callback_chunked<F>(
        path: PathBuf,
        callback: F,
        mut ids: Option<std::sync::Arc<dashmap::DashMap<EventId, ()>>>,
        chunk_size: usize,
    ) where
        F: for<'a> Fn(
                Vec<crate::NostrEventBorrowed<'a>>,
            ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync,
    {
        use crate::NostrEventBorrowed;
        use std::ops::Deref;

        let f = match Self::open_file_static(path.clone()).await {
            Ok(f) => f,
            Err(e) => {
                log::error!("Failed to open file {}: {}", path.display(), e);
                return;
            }
        };
        let mut reader = crate::reader::not_sync::ChunkedJsonReader::new(f);
        let mut objects = 0u64;
        let mut events = 0u64;

        // Pre-allocate reusable buffers to avoid allocation churn
        let mut buffer_pool: Vec<Vec<u8>> =
            (0..chunk_size).map(|_| Vec::with_capacity(2048)).collect();

        loop {
            let mut buffer_count = 0;

            // Read chunk_size JSON objects, reusing buffers from pool
            for buffer in buffer_pool.iter_mut() {
                // Reuse the allocation for typical events, but don't let one
                // giant event (long-form posts, base64 blobs go to ~512KB)
                // permanently ratchet this slot up: the pool is chunk_size
                // buffers per worker thread, so with default settings a few
                // thousand oversized events grow the process by
                // workers x chunk_size x max_event_size — gigabytes that are
                // never returned.
                if buffer.capacity() > MAX_RETAINED_BUFFER {
                    *buffer = Vec::with_capacity(2048);
                } else {
                    buffer.clear();
                }
                match reader.read_json_object(buffer).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        objects += 1;
                        buffer_count += 1;
                    }
                    Err(e) => {
                        if is_end_of_stream(&e) {
                            log::debug!("EOF (open or truncated final frame)");
                        } else {
                            log::error!("Error reading file: {}", e);
                        }
                        break;
                    }
                }
            }

            if buffer_count == 0 {
                log::info!("EOF. objects={objects}, events={events}");
                break;
            }

            // Parse all JSON objects - they all borrow from buffer_pool
            let mut parsed_events: Vec<NostrEventBorrowed> = Vec::with_capacity(buffer_count);

            for json_bytes in &buffer_pool[..buffer_count] {
                match serde_json::from_slice::<NostrEventBorrowed>(json_bytes) {
                    Ok(event) => {
                        // Apply deduplication if enabled
                        if let Some(ids_map) = ids.as_mut() {
                            let ev_id = match decode_event_id(event.id.deref()) {
                                Ok(id) => id,
                                Err(_) => continue,
                            };

                            if ids_map.insert(ev_id, ()).is_none() {
                                events += 1;
                                parsed_events.push(event);
                            }
                        } else {
                            events += 1;
                            parsed_events.push(event);
                        }
                    }
                    Err(e) => {
                        log::warn!("Invalid json: {} (bytes: {})", e, json_bytes.len())
                    }
                }
            }

            // Invoke callback with the entire batch of borrowed events
            if !parsed_events.is_empty() {
                callback(parsed_events).await;
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
    async fn open_file_static(
        path: PathBuf,
    ) -> anyhow::Result<std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send>>> {
        use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, ZstdDecoder};
        let f = tokio::fs::File::open(path.clone()).await?;
        match path.extension() {
            Some(ext) => match ext.to_str().unwrap() {
                "json" | "jsonl" => Ok(Box::pin(tokio::io::BufReader::new(f))),
                "gz" => Ok(Box::pin(tokio::io::BufReader::new(GzipDecoder::new(
                    tokio::io::BufReader::new(f),
                )))),
                "zst" | "zstd" => Ok(Box::pin(tokio::io::BufReader::new(ZstdDecoder::new(
                    tokio::io::BufReader::new(f),
                )))),
                "bz2" => Ok(Box::pin(tokio::io::BufReader::new(BzDecoder::new(
                    tokio::io::BufReader::new(f),
                )))),
                _ => anyhow::bail!("Unknown extension"),
            },
            None => anyhow::bail!("Could not determine archive format"),
        }
    }
}

#[cfg(feature = "sync")]
impl NostrCursor {
    /// Processes all files using OS threads for true CPU parallelism.
    ///
    /// Unlike the async `walk_with_chunked`, this method spawns actual OS threads,
    /// ensuring that CPU-bound work (JSON parsing, hex decoding) runs truly in parallel
    /// across multiple cores.
    ///
    /// # Arguments
    ///
    /// * `callback` - A sync function called for each batch of events.
    ///   Must be `Fn` (not `FnMut`) + `Send` + `Sync` to allow concurrent calls from multiple threads.
    /// * `chunk_size` - Number of events to read per batch
    ///
    /// # Performance
    ///
    /// This is the fastest option for CPU-bound workloads because:
    /// - Each file is processed on a dedicated OS thread
    /// - JSON parsing and hex decoding run truly in parallel
    /// - No async runtime overhead
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use nostr_archive_cursor::{NostrCursor, NostrEventBorrowed};
    /// use std::sync::atomic::{AtomicU64, Ordering};
    /// use std::sync::Arc;
    ///
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_max_parallelism()
    ///     .with_dedupe(false);
    ///
    /// let counter = Arc::new(AtomicU64::new(0));
    /// let counter_clone = counter.clone();
    ///
    /// cursor.walk_with_chunked_sync(move |events: Vec<NostrEventBorrowed>| {
    ///     counter_clone.fetch_add(events.len() as u64, Ordering::Relaxed);
    /// }, 1000);
    ///
    /// println!("Processed {} events", counter.load(Ordering::Relaxed));
    /// ```
    pub fn walk_with_chunked_sync<F>(self, callback: F, chunk_size: usize)
    where
        F: Fn(Vec<crate::NostrEventBorrowed<'_>>) + Send + Sync + 'static,
    {
        self.walk_with_chunked_sync_located(
            move |_path, events| callback(events.into_iter().map(|e| e.event).collect()),
            chunk_size,
        )
    }

    /// Like [`walk_with_chunked_sync`](Self::walk_with_chunked_sync), but the
    /// callback also receives the file each batch came from and the byte
    /// offset/length of every event within it.
    ///
    /// This is what lets an index record `(shard, offset, len)` so events can
    /// later be fetched by id without scanning.
    pub fn walk_with_chunked_sync_located<F>(self, callback: F, chunk_size: usize)
    where
        F: Fn(&std::path::Path, Vec<LocatedEvent<'_>>) + Send + Sync + 'static,
    {
        use std::sync::Arc;
        use std::sync::Mutex;

        let dir = self.dir.clone();
        let parallelism = self.parallelism;

        // Explicit file list, or everything in the directory.
        let files: Vec<PathBuf> = match self.only {
            Some(files) => files
                .into_iter()
                .filter(|p| is_walkable_archive(p))
                .collect(),
            None => match std::fs::read_dir(&dir) {
                Ok(reader) => reader
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                    .map(|e| e.path())
                    .filter(|p| is_walkable_archive(p))
                    .collect(),
                Err(e) => {
                    log::error!("Failed to read directory: {}", e);
                    return;
                }
            },
        };

        // Shared deduplication state
        let ids: Option<Arc<dashmap::DashMap<EventId, ()>>> = if self.dedupe {
            Some(Arc::new(dashmap::DashMap::new()))
        } else {
            None
        };

        // Wrap callback in Arc for sharing across threads
        let callback = Arc::new(callback);

        // Split large framed archives so one huge shard cannot serialise the
        // tail of a pass onto a single worker. Small and unseekable archives
        // stay whole, so this is a no-op for a directory of ordinary dumps.
        let segments: Vec<Segment> = files
            .iter()
            .flat_map(|p| Self::plan_segments(p, parallelism))
            .collect();
        if segments.len() > files.len() {
            log::info!(
                "{} archive(s) split into {} segments for {parallelism} worker(s)",
                files.len(),
                segments.len()
            );
        }

        // Track progress
        let total_files = segments.len();
        let processed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Use a work-stealing approach with a shared queue
        let file_queue = Arc::new(Mutex::new(segments.into_iter()));

        // Spawn worker threads
        let handles: Vec<_> = (0..parallelism)
            .map(|n| {
                let file_queue = file_queue.clone();
                let callback = callback.clone();
                let ids = ids.clone();
                let processed_count = processed_count.clone();

                std::thread::Builder::new()
                    .name(format!("nostr-cursor:{}", n))
                    .spawn(move || {
                        loop {
                            // Get next file from queue
                            let path = {
                                let mut queue = file_queue.lock().unwrap();
                                queue.next()
                            };

                            let seg = match path {
                                Some(p) => p,
                                None => break, // No more work
                            };

                            let current = processed_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                + 1;
                            log::info!(
                                "Reading [{}/{}]: {}{}",
                                current,
                                total_files,
                                seg.path.display(),
                                match seg.end {
                                    Some(e) => format!(" [{}..{}]", seg.start, e),
                                    None if seg.start > 0 => format!(" [{}..]", seg.start),
                                    None => String::new(),
                                }
                            );
                            Self::read_segment_sync_chunked(
                                &seg,
                                &*callback,
                                ids.clone(),
                                chunk_size,
                            );
                        }
                    })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            match handle {
                Ok(j) => {
                    if let Err(_) = j.join() {
                        log::error!("Failed to join thread");
                    }
                }
                Err(e) => {
                    log::error!("Failed to start thread: {}", e);
                }
            }
        }
    }

    /// Opens a file synchronously, handling compression based on extension.
    /// Returns a BufRead - decompressors are wrapped in BufReader.
    /// Split `path` into at most `want` segments on frame boundaries.
    ///
    /// Returns a single whole-file segment when the archive cannot be seeked
    /// into: gzip and bzip2 have no frame structure, and a zstd archive with no
    /// (or a one-entry) sidecar is a single frame that must be decoded from the
    /// start regardless.
    pub(crate) fn plan_segments(path: &PathBuf, want: usize) -> Vec<Segment> {
        Self::plan_segments_with_min(path, want, MIN_SPLIT_BYTES)
    }

    /// [`plan_segments`](Self::plan_segments) with an explicit size threshold,
    /// so tests can split archives smaller than the production minimum.
    #[cfg(test)]
    pub(crate) fn plan_segments_for_test(
        path: &PathBuf,
        want: usize,
        min_bytes: u64,
    ) -> Vec<Segment> {
        Self::plan_segments_with_min(path, want, min_bytes)
    }

    fn plan_segments_with_min(path: &PathBuf, want: usize, min_bytes: u64) -> Vec<Segment> {
        let whole = |p: &PathBuf| {
            vec![Segment {
                path: p.clone(),
                seek_to: 0,
                decode_from: 0,
                start: 0,
                end: None,
            }]
        };
        if want <= 1 {
            return whole(path);
        }
        let is_zstd = matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("zst") | Some("zstd")
        );
        if !is_zstd {
            return whole(path);
        }
        match std::fs::metadata(path) {
            Ok(m) if m.len() >= min_bytes => {}
            _ => return whole(path),
        }
        let sidecar = crate::database::frames::sidecar_path(path);
        let table = match crate::database::frames::FrameTable::load(&sidecar) {
            Ok(Some(t)) if t.len() > 2 => t,
            _ => return whole(path),
        };

        // Cut on frame boundaries, spaced evenly through the frame list.
        let frames = table.len();
        let cuts = want.min(frames);
        let mut segments = Vec::with_capacity(cuts);
        for i in 0..cuts {
            let idx = i * frames / cuts;
            let here = match table.get(idx) {
                Some(f) => f,
                None => continue,
            };
            // Start decoding a frame early so the reader sees a newline and
            // resyncs before reaching `start`; objects before `start` are then
            // discarded by offset. Without the overlap an event beginning
            // exactly on the boundary is dropped by both neighbours.
            let back = table.get(idx.saturating_sub(1)).unwrap_or(here);
            let next = (i + 1 < cuts)
                .then(|| (i + 1) * frames / cuts)
                .and_then(|n| table.get(n));
            segments.push(Segment {
                path: path.clone(),
                seek_to: if idx == 0 { 0 } else { back.compressed },
                decode_from: if idx == 0 { 0 } else { back.uncompressed },
                start: here.uncompressed,
                end: next.map(|f| f.uncompressed),
            });
        }
        if segments.is_empty() {
            return whole(path);
        }
        log::debug!(
            "{}: split into {} segment(s) across {frames} frames",
            path.display(),
            segments.len()
        );
        segments
    }

    /// Advance a freshly-seeked reader past the partial line it starts in,
    /// returning it with the decompressed offset it now sits at.
    ///
    /// A no-op for a segment that starts at byte zero, which is already on a
    /// line boundary by definition.
    fn skip_partial_line(
        mut reader: Box<dyn std::io::BufRead + Send>,
        seg: &Segment,
    ) -> std::io::Result<(Box<dyn std::io::BufRead + Send>, u64)> {
        if seg.seek_to == 0 {
            return Ok((reader, seg.decode_from));
        }
        let mut skipped = 0u64;
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break; // no newline in the rest of the file
            }
            match buf.iter().position(|&b| b == b'\n') {
                Some(i) => {
                    reader.consume(i + 1);
                    skipped += i as u64 + 1;
                    break;
                }
                None => {
                    let n = buf.len();
                    reader.consume(n);
                    skipped += n as u64;
                }
            }
        }
        Ok((reader, seg.decode_from + skipped))
    }

    /// Open an archive positioned at a compressed byte offset.
    fn open_segment_sync(
        path: &PathBuf,
        seek_to: u64,
    ) -> anyhow::Result<Box<dyn std::io::BufRead + Send>> {
        if seek_to == 0 {
            return Self::open_file_sync(path);
        }
        use std::io::Seek;
        let mut f = std::fs::File::open(path)?;
        f.seek(std::io::SeekFrom::Start(seek_to))?;
        Ok(Box::new(std::io::BufReader::new(
            zstd::stream::Decoder::new(f)?,
        )))
    }

    fn open_file_sync(path: &PathBuf) -> anyhow::Result<Box<dyn std::io::BufRead + Send>> {
        let f = std::fs::File::open(path)?;
        match path.extension() {
            Some(ext) => match ext.to_str().unwrap() {
                "json" | "jsonl" => Ok(Box::new(std::io::BufReader::new(f))),
                "gz" => Ok(Box::new(std::io::BufReader::new(
                    flate2::read::GzDecoder::new(f),
                ))),
                "zst" | "zstd" => Ok(Box::new(std::io::BufReader::new(
                    zstd::stream::Decoder::new(f)?,
                ))),
                "bz2" => Ok(Box::new(std::io::BufReader::new(
                    bzip2::read::BzDecoder::new(f),
                ))),
                _ => anyhow::bail!("Unknown extension"),
            },
            None => anyhow::bail!("Could not determine archive format"),
        }
    }

    /// Reads a file synchronously in chunks, invoking the callback for each batch.
    fn read_file_sync_chunked<F>(
        path: &PathBuf,
        callback: &F,
        ids: Option<std::sync::Arc<dashmap::DashMap<EventId, ()>>>,
        chunk_size: usize,
    ) where
        F: Fn(&std::path::Path, Vec<LocatedEvent<'_>>),
    {
        Self::read_segment_sync_chunked(
            &Segment {
                path: path.clone(),
                seek_to: 0,
                decode_from: 0,
                start: 0,
                end: None,
            },
            callback,
            ids,
            chunk_size,
        )
    }

    /// Read one segment of an archive, reporting absolute offsets.
    fn read_segment_sync_chunked<F>(
        seg: &Segment,
        callback: &F,
        ids: Option<std::sync::Arc<dashmap::DashMap<EventId, ()>>>,
        chunk_size: usize,
    ) where
        F: Fn(&std::path::Path, Vec<LocatedEvent<'_>>),
    {
        let path = &seg.path;
        let reader = match Self::open_segment_sync(path, seg.seek_to) {
            Ok(r) => r,
            Err(e) => {
                log::error!("Failed to open file {}: {}", path.display(), e);
                return;
            }
        };

        // A frame boundary is not a line boundary. The writer rolls frames
        // after a complete event, but `write_framed` -- which produced every
        // reframed archive -- cuts on byte count, so a segment usually starts
        // in the middle of an event. Discard that partial line here: the JSON
        // reader treats a fragment as a hard error rather than resynchronising,
        // which ended the segment at its first byte.
        let (reader, base) = match Self::skip_partial_line(reader, seg) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}: seeking to a line boundary: {e}", path.display());
                return;
            }
        };
        let mut reader = crate::reader::sync::SyncChunkedJsonReader::with_base(reader, base);
        let mut objects = 0u64;
        let mut events = 0u64;
        // Set once this segment has read past the range it owns.
        let mut done = false;

        // Pre-allocate reusable buffers
        let mut buffer_pool: Vec<Vec<u8>> =
            (0..chunk_size).map(|_| Vec::with_capacity(2048)).collect();
        // Offset of each buffered object in the decompressed stream.
        let mut offsets: Vec<u64> = vec![0; chunk_size];

        loop {
            let mut buffer_count = 0;

            // Read chunk_size JSON objects
            for buffer in buffer_pool.iter_mut() {
                // Reuse the allocation for typical events, but don't let one
                // giant event (long-form posts, base64 blobs go to ~512KB)
                // permanently ratchet this slot up: the pool is chunk_size
                // buffers per worker thread, so with default settings a few
                // thousand oversized events grow the process by
                // workers x chunk_size x max_event_size — gigabytes that are
                // never returned.
                if buffer.capacity() > MAX_RETAINED_BUFFER {
                    *buffer = Vec::with_capacity(2048);
                } else {
                    buffer.clear();
                }
                // Keep reading into *this* buffer until it holds an object the
                // segment owns. Skipping by advancing the loop instead would
                // leave `buffer_count` pointing at a slot the parse step never
                // filled, and every offset after it would describe the wrong
                // event.
                let mut kept = false;
                loop {
                    match reader.read_json_object(buffer) {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            let at = reader.last_object_offset();
                            // Bytes before `start` belong to the previous
                            // segment; we only decoded them to resynchronise on
                            // a line break.
                            if at < seg.start {
                                buffer.clear();
                                continue;
                            }
                            // An event beginning at or past `end` is the next
                            // segment's. One that merely *extends* past it is
                            // ours, so ownership is decided by where it starts.
                            if seg.end.is_some_and(|e| at >= e) {
                                done = true;
                                break;
                            }
                            offsets[buffer_count] = at;
                            objects += 1;
                            buffer_count += 1;
                            kept = true;
                            break;
                        }
                        Err(e) => {
                            if is_end_of_stream(&e) {
                                log::debug!("EOF (open or truncated final frame)");
                            } else {
                                log::error!("Error reading file: {}", e);
                            }
                            break;
                        }
                    }
                }
                if !kept {
                    break;
                }
            }

            if buffer_count == 0 {
                log::info!("EOF. objects={objects}, events={events}");
                break;
            }

            // Parse all JSON objects
            let mut parsed_events: Vec<LocatedEvent> = Vec::with_capacity(buffer_count);

            for (i, json_bytes) in buffer_pool[..buffer_count].iter().enumerate() {
                let located = |event| LocatedEvent {
                    event,
                    offset: offsets[i],
                    len: json_bytes.len() as u32,
                };
                match serde_json::from_slice::<crate::NostrEventBorrowed>(json_bytes) {
                    Ok(event) => {
                        if let Some(ref ids_map) = ids {
                            use std::ops::Deref;

                            let ev_id = match decode_event_id(event.id.deref()) {
                                Ok(id) => id,
                                Err(_) => continue,
                            };

                            if ids_map.insert(ev_id, ()).is_none() {
                                events += 1;
                                parsed_events.push(located(event));
                            }
                        } else {
                            events += 1;
                            parsed_events.push(located(event));
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Invalid json: {} {}",
                            e,
                            std::str::from_utf8(json_bytes).unwrap_or("<invalid json>")
                        )
                    }
                }
            }

            if !parsed_events.is_empty() {
                callback(path, parsed_events);
            }

            if done {
                log::info!("segment end. objects={objects}, events={events}");
                break;
            }
        }
    }
}

#[cfg(all(test, feature = "sync"))]
mod segment_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Write a framed archive of `n` events, returning its path.
    fn write_archive(dir: &std::path::Path, n: u64, frame_target: u64) -> PathBuf {
        #[derive(serde::Serialize)]
        struct Ev {
            id: String,
            pubkey: String,
            created_at: u64,
            kind: u16,
            tags: Vec<Vec<String>>,
            content: String,
            sig: String,
        }
        let path = dir.join("events_20240101.jsonl.zst");
        let mut w =
            crate::database::file::CompressedJsonLFile::with_frame_target(&path, frame_target)
                .unwrap();
        for i in 0..n {
            w.write_event(&Ev {
                id: format!("{i:064x}"),
                pubkey: "b".repeat(64),
                created_at: 1_700_000_000 + i,
                kind: 1,
                tags: vec![],
                // Vary the length so events straddle frame boundaries.
                content: "x".repeat((i % 97) as usize + 1),
                sig: "c".repeat(128),
            })
            .unwrap();
        }
        w.finish().unwrap();
        path
    }

    /// Read every event via the public walk, at a given parallelism.
    fn walk(dir: &std::path::Path, parallelism: usize) -> Vec<(String, u64, u32)> {
        let seen: Arc<Mutex<Vec<(String, u64, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let out = seen.clone();
        NostrCursor::new(dir.to_path_buf())
            .with_parallelism(parallelism)
            .with_dedupe(false)
            .walk_with_chunked_sync_located(
                move |_p, events| {
                    let mut g = out.lock().unwrap();
                    for e in events {
                        g.push((e.event.id.to_string(), e.offset, e.len));
                    }
                },
                64,
            );
        let mut v = Arc::try_unwrap(seen).unwrap().into_inner().unwrap();
        v.sort();
        v
    }

    /// Splitting an archive across workers must not change what is read.
    ///
    /// Every event, exactly once, with the same offset and length as a
    /// single-threaded pass -- offsets are what the index stores, so a segment
    /// that reported them relative to its own slice would make every lookup
    /// into that shard read the wrong bytes.
    #[test]
    fn segmented_reads_match_a_whole_file_read() {
        let dir = tempfile::tempdir().unwrap();
        // Small frames so a 1 GiB-threshold-exempt file still yields many.
        let path = write_archive(dir.path(), 20_000, 4096);
        let table =
            crate::database::frames::FrameTable::load(&crate::database::frames::sidecar_path(&path))
                .unwrap()
                .unwrap();
        assert!(table.len() > 8, "need several frames to split across");

        let whole = walk(dir.path(), 1);
        assert_eq!(whole.len(), 20_000, "baseline must see every event");

        // Force splitting regardless of the size threshold by planning directly.
        for workers in [2usize, 4, 8] {
            let segs = NostrCursor::plan_segments_for_test(&path, workers, 0);
            assert!(segs.len() > 1, "expected a split at {workers} workers");

            let seen: Arc<Mutex<Vec<(String, u64, u32)>>> = Arc::new(Mutex::new(Vec::new()));
            for seg in &segs {
                let out = seen.clone();
                NostrCursor::read_segment_sync_chunked(
                    seg,
                    &move |_p: &std::path::Path, events: Vec<LocatedEvent<'_>>| {
                        let mut g = out.lock().unwrap();
                        for e in events {
                            g.push((e.event.id.to_string(), e.offset, e.len));
                        }
                    },
                    None,
                    64,
                );
            }
            let mut got = Arc::try_unwrap(seen).unwrap().into_inner().unwrap();
            got.sort();
            assert_eq!(
                got.len(),
                whole.len(),
                "{workers} segments changed the event count"
            );
            assert_eq!(got, whole, "{workers} segments changed events or offsets");
        }
    }

    /// The same, for an archive whose frames were cut on byte counts.
    ///
    /// `CompressedJsonLFile` rolls a frame only after a complete event, so its
    /// frames happen to land on line boundaries and a segment starting at one
    /// parses cleanly by luck. `write_framed` -- which produced every reframed
    /// archive, including the 149 GB shard this feature exists for -- cuts on
    /// byte count, so segments start mid-event. The JSON reader treats a
    /// fragment as a hard error rather than resynchronising, which ended each
    /// segment at its first byte and silently dropped the rest of its range.
    #[test]
    fn segments_of_a_byte_framed_archive_read_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_archive(dir.path(), 20_000, 1 << 20);
        let whole = walk(dir.path(), 1);
        assert_eq!(whole.len(), 20_000);

        // Reframe it the way an imported archive gets reframed: frames cut by
        // size, with no regard for where events begin or end.
        crate::database::file::reframe_archive(&src, 4096).unwrap();

        let after = walk(dir.path(), 1);
        assert_eq!(after.len(), 20_000, "reframe must preserve every event");

        for workers in [2usize, 4, 8] {
            let segs = NostrCursor::plan_segments_for_test(&src, workers, 0);
            assert!(segs.len() > 1, "expected a split at {workers} workers");

            let seen: Arc<Mutex<Vec<(String, u64, u32)>>> = Arc::new(Mutex::new(Vec::new()));
            for seg in &segs {
                let out = seen.clone();
                NostrCursor::read_segment_sync_chunked(
                    seg,
                    &move |_p: &std::path::Path, events: Vec<LocatedEvent<'_>>| {
                        let mut g = out.lock().unwrap();
                        for e in events {
                            g.push((e.event.id.to_string(), e.offset, e.len));
                        }
                    },
                    None,
                    64,
                );
            }
            let mut got = Arc::try_unwrap(seen).unwrap().into_inner().unwrap();
            got.sort();
            assert_eq!(got, after, "{workers} segments lost or duplicated events");
        }
    }

    /// Offsets from a segmented read must still point at the right bytes.
    #[test]
    fn segment_offsets_locate_the_original_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_archive(dir.path(), 5_000, 4096);
        let segs = NostrCursor::plan_segments_for_test(&path, 4, 0);
        assert!(segs.len() > 1);

        let pool = crate::ShardReaderPool::new();
        for seg in &segs {
            let seen: Arc<Mutex<Vec<(String, u64, u32)>>> = Arc::new(Mutex::new(Vec::new()));
            let out = seen.clone();
            NostrCursor::read_segment_sync_chunked(
                seg,
                &move |_p: &std::path::Path, events: Vec<LocatedEvent<'_>>| {
                    let mut g = out.lock().unwrap();
                    for e in events {
                        g.push((e.event.id.to_string(), e.offset, e.len));
                    }
                },
                None,
                64,
            );
            let got = Arc::try_unwrap(seen).unwrap().into_inner().unwrap();
            for (id, offset, len) in got.iter().take(20) {
                let bytes = pool.read_zstd_range(&path, *offset, *len).unwrap();
                let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(v["id"].as_str().unwrap(), id, "offset {offset} misreads");
            }
        }
    }
}
