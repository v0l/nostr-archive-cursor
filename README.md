# Nostr backup processor

Process JSON-L backups and compute some stats

A memory-efficient streaming processor for Nostr event archives that supports:
- Parallel file reading
- Automatic deduplication
- Compressed formats (.gz, .zst, .bz2)
- Streaming processing of 300M+ events

## Features

- **Memory Efficient**: Events are streamed one at a time, not buffered in memory
- **Parallel Processing**: Read multiple files concurrently with configurable parallelism
- **Automatic Deduplication**: Filters duplicate events based on event ID
- **Compression Support**: Handles gzip, zstandard, and bzip2 compressed files
- **No Memory Explosion**: Designed to process hundreds of millions of events

## Example

```rust
use futures::stream::StreamExt;

// Sequential processing (default)
let cursor = NostrCursor::new("./backups".into());
let mut stream = cursor.walk();
while let Some(event) = stream.next().await {
    // Process event
}

// Parallel processing (4 files at once)
let cursor = NostrCursor::new("./backups".into())
    .with_parallelism(4);

let mut stream = cursor.walk();
while let Some(event) = stream.next().await {
    // Process event
}
```

## Performance Notes

- **Parallelism**: Set to 2-8 for optimal performance on most systems
- **Memory**: Each parallel file reader uses one buffer (~8KB)
- **Deduplication**: Event IDs are stored in a HashSet (32 bytes per event)

## Supported File Formats

- `.json` - Uncompressed JSON-L
- `.jsonl` - Uncompressed JSON-L
- `.gz` - Gzip compressed
- `.zst` - Zstandard compressed
- `.bz2` - Bzip2 compressed