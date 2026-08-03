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
- **Random Access**: Fetch any event by id without scanning the archive
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
- **Deduplication Index**: Uses RocksDB for fast event ID lookups to prevent duplicates
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
// - ./archive/index-rocksdb/ (event id index)
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

### Event lookup by id

Index values record **where** each event lives, so an event can be read back
without scanning the archive:

```rust
// Single event
let event = db.event_by_id(&id).await?;

// Raw JSON line, no parsing
let raw = db.get_raw(&id).await;

// Batched: one index multi_get, reads grouped by (shard, frame),
// decoded in parallel on the blocking pool
let events = db.get_many_raw(&ids).await;

// Where is it?
let loc = db.locate(&id)?;   // Option<EventLoc { shard, offset, len }>
```

`save_event` returns as soon as the event is queued; call `db.flush().await`
first if you need read-after-write.

How it works:

* Archives are written as **bounded zstd frames** (512 KiB of uncompressed data
  by default, tunable via `new_with_frame_target`) rather than one huge frame. Concatenated frames are still a
  normal `.zst` file - `zstd -d` and every existing reader path are unaffected.
* Each frame boundary is appended to a `<shard>.frames` sidecar, so a lookup
  binary-searches to the containing frame and decodes only that frame.
* The index value stores `shard_hash + offset + len`, where the shard id is a
  hash of the **file name** - no registry, so archives dropped into the
  directory by an external relay backup index and resolve with no registration
  step, and a rebuilt index produces identical ids.
* A `ShardReaderPool` caches file descriptors, zstd decode contexts and frame
  tables, and reads with `pread` so one handle serves concurrent lookups.
* After decoding, the event id is verified against the requested id: a stale or
  corrupt offset can never return the *wrong* event.

Measured on 200k **real** events from a nostr.band archive
(`cargo run --release --example real_archive_check --features db-rocksdb,sync -- <archive>`):

| frame target | archive size | vs single frame | single lookup p50 | batched |
| ------------ | ------------ | --------------- | ----------------- | ------- |
| 8 KiB        | 57.7 MiB     | +46%            | 22 us             | 4.2 us/event |
| 32 KiB       | 52.0 MiB     | +32%            | 35 us             | 4.4 us/event |
| 128 KiB      | 48.5 MiB     | +23%            | 85 us             | 5.7 us/event |
| **512 KiB** (default) | **44.6 MiB** | **+13%** | **472 us**   | **7.1 us/event** |
| 4 MiB        | 40.0 MiB     | +1.5%           | 2131 us           | 10.9 us/event |
| single frame | 39.4 MiB     | -               | (decodes the whole shard) | |

Small frames are not free on real data - events share pubkeys, tags and
phrasing, so a larger zstd window compresses better. The default favours
storage, because batching hides most of the latency (each frame is decoded once
per batch). Lower `frame_target` if interactive single-id fetches dominate.

### Validation against real archives

`examples/real_archive_check.rs` indexes a real archive, then fetches every
event back by id (with scanning disabled, so hits must come from the index) and
asserts the bytes are identical to the source lines:

```sh
cargo run --release --example real_archive_check --features db-rocksdb,sync -- \
    /path/to/events.jsonl.zst 500000
```

Run against three real archives (nostr.band snapshot, strfry dump, wellorder
early-1m): **0 mismatched, 0 missing**, ~265k events/s indexing, ~8-11us/event
batched retrieval. Real data exercises cases synthetic tests miss - events up
to 100 KB, truncated/corrupt lines (~0.5% of the nostr.band snapshot), and
duplicate ids across a truncated fragment and its complete copy.

### Indexing

New or changed shards are picked up incrementally - a restart costs one `stat`
per shard instead of re-walking the archive:

```rust
let report = db.index_new_shards()?;   // blocking; use spawn_blocking
// IndexReport { shards: 5, unchanged: 4, indexed: 1, reframed: 1, new_events: 198983 }
```

* Shards are skipped when their size and mtime match what was recorded when
  they were indexed (bookkeeping lives in the index under a `shard/` prefix).
* Archives that are **one giant zstd frame** - the usual shape of a backup
  produced elsewhere - are rewritten into bounded frames first, otherwise every
  lookup into them decodes from the start of the file. Reframing preserves the
  decompressed bytes, so offsets stay valid.
* The shard the writer is currently appending to is excluded; those events are
  indexed inline as they are written.
* `insert_batch` reports how many ids were genuinely new, so the cached event
  count stays exact without the O(n) `repair_count()` rescan that reindexing
  used to require.

Measured on real imports (4 x 200k events, single-frame `.zst`):

```
first pass : shards: 4, indexed: 4, reframed: 4, new_events: 795810   4.41s
second pass: shards: 4, unchanged: 4, indexed: 0                      0.000s
after drop : shards: 5, unchanged: 4, indexed: 1, new_events: 198983  1.11s
```

`rebuild_index()` remains for a full reindex; it records the same state, so an
incremental pass right after it is a no-op.

### Converting gzip/bzip2 imports

`.gz` and `.bz2` archives cannot be seeked at all - reading one event from a
550 GB `.gz` means decompressing 550 GB. `convert_archive_to_zst` rewrites them
as framed zstd, preserving the JSON-L stream byte for byte:

```rust
use nostr_archive_cursor::{convert_archive_to_zst, DEFAULT_FRAME_TARGET};

let zst = convert_archive_to_zst(Path::new("archive/events.jsonl.gz"), DEFAULT_FRAME_TARGET)?;
// -> archive/events.jsonl.zst (+ .frames sidecar); the original is left in place
```

Measured on real archives (286 MiB of JSON-L each):

| source | as imported | as framed `.zst` | convert time |
| ------ | ----------- | ---------------- | ------------ |
| `events.jsonl.gz` | 91.6 MiB | **81.7 MiB** | 0.9s |
| `nostr-wellorder-early-1m-v1.jsonl.bz2` | 93.8 MiB | **90.2 MiB** | 5.3s |

So conversion is smaller *and* seekable. The shard id comes from the file name,
which changes, so convert **before** indexing - or re-index after, which
self-heals since the same ids are simply re-recorded against the new shard.
Delete the original afterwards so it is not indexed twice.

### Index size

Measured on 5M real events, extrapolated (RocksDB with zstd + bloom filters):

| index contents | per event | at 900M events |
| -------------- | --------- | -------------- |
| `created_at` only | 74.5 B | 62.5 GiB |
| `created_at` + location | 81.9 B | 68.6 GiB |

Event ids are random and incompressible - the two 32-byte keys per event (the
primary key and the time-index key) are ~54 GiB of that. The location field
costs +7.4 B/event because shard hashes repeat and offsets increase
monotonically, so zstd squashes them.

### Opening an index written by an older version

An existing index opens unchanged - no migration, no error:

* **v0 values keep working.** Old 8-byte entries are read as "created_at known,
  location unknown", so dedupe (`check_id`), counts and `list_ids` are
  unaffected. `event_by_id` falls back to scanning; new events written after
  the upgrade are stored as v1 and are located immediately, so both layouts
  coexist in the same index.
* **Old SSTs stay uncompressed.** Previous versions opened the index with
  `DB::open_default`, which asks for Snappy - not included in this build - so
  existing data is uncompressed. New options only apply to newly written files,
  and RocksDB will *not* rewrite the rest on its own: manual compaction skips
  the bottommost level by default, and in a mostly-static index that is where
  all the data lives. `RocksDbIndex::compact()` forces it:

  ```rust
  db.compact();   // one full rewrite of the index; 3M events: 271 -> 206 MiB (-24%)
  ```

* **The `Day` scan fallback will not help a historical import.** Shards are
  named after the day they were *written*, so events imported from an old dump
  live in a shard whose name has nothing to do with their `created_at`. Use
  `ScanFallback::All` (O(archive) per miss) or, better, reindex.

The migration that actually buys O(1) lookups:

```rust
db.rebuild_index()?;   // reframes shards, records locations, records shard state
db.compact();          // optional: recompress the index in one pass
```

### Migrating an existing archive

Old index values (8 bytes: just `created_at`) are read as **version 0** and
keep working - no rebuild required. They carry no location, so lookups fall
back to scanning (`ScanFallback::Day` by default, `::All` for archives imported
out of order, `::Off` for strictly O(1) lookups).

To get O(1) lookups for existing data:

```rust
db.rebuild_missing_frame_indexes();  // build .frames sidecars (one pass per shard)
db.rebuild_index()?;                 // re-index with locations
```

Archives imported from elsewhere are usually a **single huge zstd frame**, so a
lookup would decode from the start of the file. `reframe_archive` rewrites the
framing while leaving the decompressed bytes byte-identical (so any offsets
already indexed stay valid):

```rust
use nostr_archive_cursor::{reframe_archive, DEFAULT_FRAME_TARGET};

reframe_archive(Path::new("archive/relay-backup-2019.jsonl.zst"), DEFAULT_FRAME_TARGET)?;
```

### File Structure

```
archive/
├── index-rocksdb/                    # event id → (created_at, shard, offset, len)
├── events_20250110.jsonl.zst         # compressed archive (bounded zstd frames)
├── events_20250110.jsonl.zst.frames  # frame index sidecar (16 B per frame)
├── events_20250111.jsonl.zst
└── events_20250111.jsonl.zst.frames
```

### Implementation Notes

- **Lookups**: `event_by_id`, `get_raw`, `get_many_raw` and id-filtered `query`/`count` are served from the index. Filters on other fields (pubkey, kind, tags) still return empty - the archive has no secondary index for them.
- **Index writes are batched by the writer thread** (that is where an event's offset becomes known). `save_event` dedupes synchronously via an in-flight set; use `flush()` for read-after-write.
- **Compression**: Previous day's files are automatically compressed with Zstandard in the background when rotating to a new day.
- **Thread-Safe**: Uses `Arc<Mutex<FlatFileWriter>>` for concurrent event writes.
- **Atomic Operations**: Uses RocksDB write batches for a crash-safe index.

### Performance

- **Deduplication**: O(1) lookup via the RocksDB index (~100ns per check)
- **Event lookup**: ~470us single (512 KiB frames), ~7us/event batched; verified byte-exact against 200k real archived events
- **Write Speed**: Limited by disk I/O, typically 10K-50K events/sec
- **Memory**: Minimal (current file buffer + RocksDB block cache + pooled readers)
- **Compression Ratio**: Typically 5-10x with Zstandard