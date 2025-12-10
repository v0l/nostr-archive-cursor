use std::path::PathBuf;

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
pub struct NostrCursor {
    /// Directory to read archives from
    dir: PathBuf,
    /// Number of files to process in parallel
    parallelism: usize,
    /// If deduplication should be performed
    dedupe: bool,
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
    /// ```rust
    /// let cursor = NostrCursor::new("./backups".into())
    ///     .with_parallelism(4);
    /// ```
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
    /// ```rust
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
    /// ```rust
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
                        match read_json_object(&mut reader, &mut buffer).await {
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
                    info!("Reading [{}/{}]: {}", idx + 1, total_files, path.display());
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
                buffer.clear();
                match reader.read_json_object(buffer).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        objects += 1;
                        buffer_count += 1;
                    }
                    Err(e) => {
                        log::error!("Error reading file: {}", e);
                        break;
                    }
                }
            }

            if buffer_count == 0 {
                info!("EOF. objects={objects}, events={events}");
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
    /// ```rust
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
    /// cursor.walk_with_chunked_sync(move |events| {
    ///     counter_clone.fetch_add(events.len() as u64, Ordering::Relaxed);
    /// }, 1000);
    ///
    /// println!("Processed {} events", counter.load(Ordering::Relaxed));
    /// ```
    pub fn walk_with_chunked_sync<F>(self, callback: F, chunk_size: usize)
    where
        F: Fn(Vec<crate::NostrEventBorrowed<'_>>) + Send + Sync + 'static,
    {
        use std::sync::Arc;
        use std::sync::Mutex;

        let dir = self.dir.clone();
        let parallelism = self.parallelism;

        // Read directory (sync)
        let files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
            Ok(reader) => reader
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .map(|e| e.path())
                .collect(),
            Err(e) => {
                log::error!("Failed to read directory: {}", e);
                return;
            }
        };

        // Shared deduplication state
        let ids: Option<Arc<dashmap::DashMap<EventId, ()>>> = if self.dedupe {
            Some(Arc::new(dashmap::DashMap::new()))
        } else {
            None
        };

        // Wrap callback in Arc for sharing across threads
        let callback = Arc::new(callback);

        // Track file progress
        let total_files = files.len();
        let processed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Use a work-stealing approach with a shared file queue
        let file_queue = Arc::new(Mutex::new(files.into_iter()));

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

                            let path = match path {
                                Some(p) => p,
                                None => break, // No more files
                            };

                            let current = processed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                            log::info!("Reading [{}/{}]: {}", current, total_files, path.display());
                            Self::read_file_sync_chunked(
                                &path,
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
        F: Fn(Vec<crate::NostrEventBorrowed<'_>>),
    {
        let reader = match Self::open_file_sync(path) {
            Ok(r) => r,
            Err(e) => {
                log::error!("Failed to open file {}: {}", path.display(), e);
                return;
            }
        };

        let mut reader = crate::reader::sync::SyncChunkedJsonReader::new(reader);
        let mut objects = 0u64;
        let mut events = 0u64;

        // Pre-allocate reusable buffers
        let mut buffer_pool: Vec<Vec<u8>> =
            (0..chunk_size).map(|_| Vec::with_capacity(2048)).collect();

        loop {
            let mut buffer_count = 0;

            // Read chunk_size JSON objects
            for buffer in buffer_pool.iter_mut() {
                buffer.clear();
                match reader.read_json_object(buffer) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        objects += 1;
                        buffer_count += 1;
                    }
                    Err(e) => {
                        log::error!("Error reading file: {}", e);
                        break;
                    }
                }
            }

            if buffer_count == 0 {
                log::info!("EOF. objects={objects}, events={events}");
                break;
            }

            // Parse all JSON objects
            let mut parsed_events: Vec<crate::NostrEventBorrowed> =
                Vec::with_capacity(buffer_count);

            for json_bytes in &buffer_pool[..buffer_count] {
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
                                parsed_events.push(event);
                            }
                        } else {
                            events += 1;
                            parsed_events.push(event);
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
                callback(parsed_events);
            }
        }
    }
}
