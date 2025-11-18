use crate::event::{NostrEvent, NostrEventBorrowed};
use anyhow::{Result, bail};
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, ZstdDecoder};
use async_stream::stream;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::future::Future;
use std::ops::Deref;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
struct EventId([u8; 32]);

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

/// Reads from a BufReader until a complete JSON object is found.
///
/// This function properly handles JSON objects by:
/// - Finding the closing `}` that matches the opening `{`
/// - Ignoring `}` characters inside strings
/// - Handling escaped characters like `\"`
/// - Supporting nested objects
///
/// # Arguments
///
/// * `reader` - The async reader to read from
/// * `buffer` - A reusable buffer that will contain the complete JSON object
///
/// # Returns
///
/// The number of bytes read, or 0 on EOF
async fn read_json_object<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> std::io::Result<usize> {
    buffer.clear();

    let mut depth = 0;
    let mut in_string = false;
    let mut escaped = false;
    let mut found_opening = false;
    let mut byte_buf = [0u8; 1];

    loop {
        let n = reader.read(&mut byte_buf).await?;
        if n == 0 {
            // EOF
            return Ok(buffer.len());
        }

        let byte = byte_buf[0];
        buffer.push(byte);

        // Skip whitespace before finding the opening brace
        if !found_opening {
            if byte == b'{' {
                found_opening = true;
                depth = 1;
            } else if !byte.is_ascii_whitespace() {
                // Non-whitespace, non-brace character before object starts
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected JSON object to start with '{'",
                ));
            }
            continue;
        }

        if escaped {
            escaped = false;
            continue;
        }

        match byte {
            b'\\' if in_string => {
                escaped = true;
            }
            b'"' => {
                in_string = !in_string;
            }
            b'{' if !in_string => {
                depth += 1;
            }
            b'}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    // Found the complete JSON object
                    return Ok(buffer.len());
                }
            }
            _ => {}
        }
    }
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

            let mut ids = HashSet::new();
            while let Some(event) = file_streams.next().await {
                let ev_id = match hex::decode(&event.id) {
                    Ok(bytes) => match bytes.as_slice().try_into() {
                        Ok(array) => EventId(array),
                        Err(_) => continue,
                    },
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
        stream! {
            match Self::open_file_static(path.clone()).await {
                Ok(f) => {
                    let mut file = BufReader::new(f);
                    let mut buffer = Vec::new();
                    let mut objects = 0u64;
                    let mut events = 0u64;

                    loop {
                        match read_json_object(&mut file, &mut buffer).await {
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
                &'a [NostrEventBorrowed<'a>],
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync
            + Clone,
    {
        let dir = self.dir.clone();
        let parallelism = self.parallelism;

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

        // Shared deduplication state using lock-free concurrent hashmap
        let ids = Arc::new(DashMap::<EventId, ()>::new());
        let dedupe = self.dedupe;

        // Use FuturesUnordered for dynamic work distribution
        let mut tasks: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> =
            FuturesUnordered::new();
        let mut file_iter = files.into_iter();

        // Start initial batch of tasks up to parallelism limit
        for _ in 0..parallelism {
            if let Some(path) = file_iter.next() {
                let callback = callback.clone();
                let ids = ids.clone();

                tasks.push(Box::pin(async move {
                    info!("Reading: {}", path.to_str().unwrap());
                    Self::read_file_with_callback_chunked(
                        path,
                        callback,
                        if dedupe { Some(ids) } else { None },
                        chunk_size,
                    )
                    .await;
                }));
            }
        }

        // As each task completes, immediately start a new one from the remaining files
        while (tasks.next().await).is_some() {
            if let Some(path) = file_iter.next() {
                let callback = callback.clone();
                let ids = ids.clone();

                tasks.push(Box::pin(async move {
                    info!("Reading: {}", path.to_str().unwrap());
                    Self::read_file_with_callback_chunked(
                        path,
                        callback,
                        if dedupe { Some(ids) } else { None },
                        chunk_size,
                    )
                    .await;
                }));
            }
        }
    }

    /// Processes all files in parallel, calling the provided async callback for each event.
    ///
    /// This method allows true parallel processing - each file reader calls the callback
    /// independently, enabling concurrent event processing across multiple threads.
    ///
    /// # Arguments
    ///
    /// * `callback` - An async function called for each event with zero-copy borrowed data.
    ///   Must be `Fn` (not `FnMut`) to allow concurrent calls from multiple file readers.
    ///   Use interior mutability (e.g., `Mutex`) if you need to mutate shared state.
    ///
    /// # Behavior
    ///
    /// - Reads up to `parallelism` files concurrently
    /// - Automatically deduplicates events based on event ID
    /// - Callback is invoked in parallel from multiple file readers
    /// - Events are passed as borrowed data (zero-copy) - convert with `.to_owned()` if needed
    /// - Waits for all files to complete before returning
    ///
    /// # Performance
    ///
    /// This approach enables true parallel processing since each file reader can invoke
    /// the callback independently, unlike `walk()` which returns a single sequential stream.
    /// Events are parsed with zero-copy deserialization for better performance.
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
    /// cursor.walk_with(move |event| {
    ///     let counter = counter_clone.clone();
    ///     async move {
    ///         // Process borrowed event in parallel (async)
    ///         let mut count = counter.lock().unwrap();
    ///         *count += 1;
    ///         // Convert to owned if needed: let owned = event.to_owned();
    ///     }
    /// }).await;
    /// ```
    pub async fn walk_with<F>(self, callback: F)
    where
        F: for<'a> Fn(NostrEventBorrowed<'a>) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync
            + Clone,
    {
        let dir = self.dir.clone();
        let parallelism = self.parallelism;

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

        // Shared deduplication state using lock-free concurrent hashmap
        let ids = Arc::new(DashMap::<EventId, ()>::new());
        let dedupe = self.dedupe;

        // Use FuturesUnordered for dynamic work distribution
        // This ensures all threads stay busy by starting new tasks as soon as previous ones complete
        let mut tasks: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> =
            FuturesUnordered::new();
        let mut file_iter = files.into_iter();

        // Start initial batch of tasks up to parallelism limit
        for _ in 0..parallelism {
            if let Some(path) = file_iter.next() {
                let callback = callback.clone();
                let ids = ids.clone();

                tasks.push(Box::pin(async move {
                    info!("Reading: {}", path.to_str().unwrap());
                    Self::read_file_with_callback(
                        path,
                        callback,
                        if dedupe { Some(ids) } else { None },
                    )
                    .await;
                }));
            }
        }

        // As each task completes, immediately start a new one from the remaining files
        // This keeps all threads busy instead of waiting for chunks to complete
        while (tasks.next().await).is_some() {
            if let Some(path) = file_iter.next() {
                let callback = callback.clone();
                let ids = ids.clone();

                tasks.push(Box::pin(async move {
                    info!("Reading: {}", path.to_str().unwrap());
                    Self::read_file_with_callback(
                        path,
                        callback,
                        if dedupe { Some(ids) } else { None },
                    )
                    .await;
                }));
            }
        }
    }

    /// Reads a single file and invokes the async callback for each deduplicated event.
    async fn read_file_with_callback<F>(
        path: PathBuf,
        callback: F,
        mut ids: Option<Arc<DashMap<EventId, ()>>>,
    ) where
        F: for<'a> Fn(NostrEventBorrowed<'a>) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync,
    {
        match Self::open_file_static(path.clone()).await {
            Ok(f) => {
                let mut file = BufReader::new(f);
                let mut buffer = Vec::with_capacity(1024 * 1024 * 64);
                let mut objects = 0u64;
                let mut events = 0u64;

                loop {
                    match read_json_object(&mut file, &mut buffer).await {
                        Ok(size) => {
                            if size == 0 {
                                info!("EOF. objects={objects}, events={events}");
                                break;
                            }
                            objects += 1;

                            match serde_json::from_slice::<NostrEventBorrowed>(&buffer) {
                                Ok(event) => {
                                    // Only decode ID if deduplication is enabled
                                    if let Some(ids_map) = ids.as_mut() {
                                        let ev_id = match hex::decode(event.id.deref()) {
                                            Ok(bytes) => match bytes.as_slice().try_into() {
                                                Ok(array) => EventId(array),
                                                Err(_) => continue,
                                            },
                                            Err(_) => continue,
                                        };

                                        // Check and insert into shared deduplication set (lock-free)
                                        // insert() returns None if the key was not present
                                        if ids_map.insert(ev_id, ()).is_none() {
                                            events += 1;
                                            callback(event).await;
                                        }
                                    } else {
                                        // No deduplication - process directly
                                        events += 1;
                                        callback(event).await;
                                    }
                                }
                                Err(e) => {
                                    warn!(
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
        mut ids: Option<Arc<DashMap<EventId, ()>>>,
        chunk_size: usize,
    ) where
        F: for<'a> Fn(
                &'a [NostrEventBorrowed<'a>],
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync,
    {
        match Self::open_file_static(path.clone()).await {
            Ok(f) => {
                let mut file = BufReader::with_capacity(1024 * 1024, f);
                let mut objects = 0u64;
                let mut events = 0u64;

                loop {
                    let mut buffer_objects: Vec<Vec<u8>> = Vec::with_capacity(chunk_size);

                    // Read chunk_size JSON objects into buffer
                    for _ in 0..chunk_size {
                        let mut buffer = Vec::new();
                        match read_json_object(&mut file, &mut buffer).await {
                            Ok(0) => break, // EOF
                            Ok(_) => {
                                objects += 1;
                                buffer_objects.push(buffer);
                            }
                            Err(e) => {
                                error!("Error reading file: {}", e);
                                break;
                            }
                        }
                    }

                    if buffer_objects.is_empty() {
                        info!("EOF. objects={objects}, events={events}");
                        break;
                    }

                    // Parse all JSON objects - they all borrow from the same buffer
                    let mut parsed_events: Vec<NostrEventBorrowed> =
                        Vec::with_capacity(buffer_objects.len());

                    for json_bytes in buffer_objects {
                        match serde_json::from_slice::<NostrEventBorrowed>(&json_bytes) {
                            Ok(event) => {
                                // Apply deduplication if enabled
                                if let Some(ids_map) = ids.as_mut() {
                                    let ev_id = match hex::decode(event.id.deref()) {
                                        Ok(bytes) => match bytes.as_slice().try_into() {
                                            Ok(array) => EventId(array),
                                            Err(_) => continue,
                                        },
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
                                warn!("Invalid json: {} (bytes: {})", e, json_bytes.len())
                            }
                        }
                    }

                    // Invoke callback with the entire batch of borrowed events
                    if !parsed_events.is_empty() {
                        callback(&parsed_events).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to open file {:?}: {}", path, e);
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
