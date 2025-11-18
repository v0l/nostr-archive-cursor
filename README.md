# Nostr backup processor

Process JSON-L backups and compute some stats

A memory-efficient streaming processor for Nostr event archives that supports:
- Parallel file reading
- Automatic deduplication
- Compressed formats (.gz, .zst, .bz2)
- Streaming processing of 300M+ events

## Features

- **Memory Efficient**: Events are streamed one at a time, not buffered in memory
- **Zero-Copy Parsing**: `walk_with()` uses borrowed data with no string allocations during parsing
- **Parallel Processing**: Read multiple files concurrently with configurable parallelism
- **Automatic Deduplication**: Filters duplicate events based on event ID
- **Compression Support**: Handles gzip, zstandard, and bzip2 compressed files
- **No Memory Explosion**: Designed to process hundreds of millions of events

## Example

### Stream-based Processing

```rust
use futures::stream::StreamExt;

// Sequential processing (default)
let cursor = NostrCursor::new("./backups".into());
let mut stream = cursor.walk();
while let Some(event) = stream.next().await {
    // Process event sequentially
}

// Parallel file reading (4 files at once)
// Note: Events are still consumed sequentially from the stream
let cursor = NostrCursor::new("./backups".into())
    .with_parallelism(4);

let mut stream = cursor.walk();
while let Some(event) = stream.next().await {
    // Process event
}

// Use all available CPU cores for parallel processing
let cursor = NostrCursor::new("./backups".into())
    .with_max_parallelism();

// Disable deduplication if you're certain there are no duplicates
let cursor = NostrCursor::new("./backups".into())
    .with_dedupe(false);
```

### Callback-based Parallel Processing

For true parallel event processing, use `walk_with` which invokes a callback from multiple file readers concurrently. Events are parsed with **zero-copy deserialization** for maximum performance:

```rust
use std::sync::{Arc, Mutex};

let cursor = NostrCursor::new("./backups".into())
    .with_parallelism(4);

let counter = Arc::new(Mutex::new(0));
let counter_clone = counter.clone();

cursor.walk_with(move |event| {
    let counter = counter_clone.clone();
    async move {
        // This async callback is invoked in parallel by multiple file readers
        // Event is borrowed (zero-copy) - no string allocations during parsing

        // Use Arc/Mutex for shared state
        let mut count = counter.lock().unwrap();
        *count += 1;

        // Access borrowed fields directly (zero-copy)
        println!("Event ID: {}", event.id);

        // Convert to owned if you need to store the event
        // let owned = event.to_owned();
    }
}).await;

println!("Processed {} events", *counter.lock().unwrap());
```

### Chunked Parallel Processing

For maximum performance, use `walk_with_chunked` which processes events in batches. This is significantly faster than processing one event at a time:

```rust
use std::sync::{Arc, Mutex};

let cursor = NostrCursor::new("./backups".into())
    .with_parallelism(4);

let counter = Arc::new(Mutex::new(0));
let counter_clone = counter.clone();

cursor.walk_with_chunked(move |events| {
    let counter = counter_clone.clone();
    Box::pin(async move {
        // Process batch of borrowed events in parallel
        let mut count = counter.lock().unwrap();
        *count += events.len();

        // All events in the batch borrow from the same buffer
        for event in events {
            println!("Processing event: {}", event.id);
        }
    })
}, 1000).await;

println!("Processed {} events", *counter.lock().unwrap());
```

## Performance Notes

- **Parallelism**: Set to 2-8 for optimal performance on most systems, or use `.with_max_parallelism()` to use all CPU cores
- **Memory**: Each parallel file reader uses one buffer (~8KB)
- **Deduplication**: Event IDs are stored in a concurrent HashMap (32 bytes per unique event). Disable with `.with_dedupe(false)` if not needed
- **Zero-Copy**: `walk_with()` and `walk_with_chunked()` use borrowed strings during parsing - no allocations until you call `.to_owned()`
- **Stream vs Callback**: Use `walk()` for sequential processing, `walk_with()` for parallel event-by-event processing, `walk_with_chunked()` for parallel batch processing (fastest)

## Supported File Formats

- `.json` - Uncompressed JSON-L
- `.jsonl` - Uncompressed JSON-L
- `.gz` - Gzip compressed
- `.zst` - Zstandard compressed
- `.bz2` - Bzip2 compressed

## JsonFilesDatabase - Nostr SDK Backend

A `nostr_sdk` database backend that writes events to daily flat JSON-L files with automatic deduplication and compression.

### Features

- **Daily File Rotation**: Events are organized into files by date (`events_YYYYMMDD.jsonl`)
- **Automatic Compression**: Files are compressed with Zstandard when rotated to the next day
- **Deduplication Index**: Uses `sled` for fast event ID lookups to prevent duplicates
- **NostrDatabase Trait**: Drop-in replacement for other `nostr_sdk` database backends
- **Write-Only Design**: Optimized for archiving, not for querying (queries return empty results)

### Usage

```rust
use nostr_archive_cursor::JsonFilesDatabase;
use nostr_sdk::prelude::*;

// Create database instance
let db = JsonFilesDatabase::new("./archive".into())?;

// Use with nostr_sdk client
let client = ClientBuilder::new()
    .database(db)
    .build();

// Events are automatically saved to daily files
client.add_relay("wss://relay.example.com").await?;
client.connect().await;

// Events received from relays are saved to:
// - ./archive/events_20250112.jsonl (current day)
// - ./archive/events_20250111.jsonl.zst (previous days, compressed)
// - ./archive/index/ (sled database for deduplication)
```

### API Methods

```rust
// Create new database
let db = JsonFilesDatabase::new(dir)?;

// List all archive files
let files: Vec<ArchiveFile> = db.list_files().await?;
for file in files {
    println!("{}: {} bytes, created {}",
        file.path.display(),
        file.size,
        file.timestamp
    );
}

// Get specific archive file
let file = db.get_file("/events_20250112.jsonl")?;

// List event IDs in index with time range filter (for sync)
let since = 0; // Unix timestamp
let until = u64::MAX; // Unix timestamp
let ids: Vec<(EventId, Timestamp)> = db.list_ids(since, until);

// Get total event count
let count = db.count_keys();

// Check if index is empty
let is_empty = db.is_index_empty();

// Rebuild the event ID index from archive files
db.rebuild_index().await?;
```

### File Structure

```
archive/
├── index/              # sled database (event ID → timestamp)
├── events_20250110.jsonl.zst  # Compressed past files
├── events_20250111.jsonl.zst
└── events_20250112.jsonl       # Current day (uncompressed)
```

### Implementation Notes

- **Write-Only**: This is an archival database. Query methods (`event_by_id`, `query`, `count`) return empty results by default.
- **Compression**: Previous day's files are automatically compressed with Zstandard in the background when rotating to a new day.
- **Thread-Safe**: Uses `Arc<Mutex<FlatFileWriter>>` for concurrent event writes.
- **Atomic Operations**: Uses `sled` for crash-safe deduplication index.

### Performance

- **Deduplication**: O(1) lookup via sled index (~100ns per check)
- **Write Speed**: Limited by disk I/O, typically 10K-50K events/sec
- **Memory**: Minimal (only current file buffer + sled cache)
- **Compression Ratio**: Typically 5-10x with Zstandard