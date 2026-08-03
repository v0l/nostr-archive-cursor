# Plan: `event_by_id` via `shard_name + offset_bytes`

> **Status: implemented.** See `src/database/{value,frames,file,pool}.rs`,
> `tests/event_lookup.rs` and `examples/real_archive_check.rs`. Two things
> changed once measured against real data:
>
> * `DEFAULT_FRAME_TARGET` is **512 KiB**, not 32 KiB. On real events (which
>   share pubkeys/tags/phrasing) small frames cost real disk: 32 KiB is +32%
>   vs a single frame, 512 KiB is +13%. Batching hides most of the latency
>   difference (4.4 vs 7.1 us/event), so the default favours storage.
> * The writer **block-flushes** (`ZSTD_e_flush`) per batch instead of ending a
>   frame, otherwise a quiet relay writes one frame per event.
>
> Real archives also exposed four bugs, now fixed and regression-tested:
> `.frames` sidecars being walked as archives; the open tail frame logged as
> corruption; events straddling a frame boundary being unreadable; and a
> truncation-recovery off-by-one that silently dropped the valid event
> following a truncated line when their separating newline landed on a buffer
> boundary (9 of 500k real events).

## Current state

- `src/database/mod.rs` — `JsonFilesDatabase` writes events into daily shards
  `events_YYYYMMDD.jsonl.zst` via a dedicated writer thread
  (`CompressedJsonLFile`, `src/database/file.rs`).
- `src/database/rocksdb.rs` — `RocksDbIndex` stores:
  - primary key: `id[32]` -> `created_at u64 le` (8 bytes)
  - secondary key: `be(created_at)[8] || id[32]` (40 bytes) -> `[]`
  - meta keys `__meta_count__`, `__meta_timeidx__`
- `NostrDatabase::event_by_id` returns `Ok(None)` — there is no way to fetch the
  event body. `query()`/`count()` are stubs too.

Goal: `event_by_id(id)` -> read exactly one line out of one shard.

## Core problem: shards are zstd streams

An `offset_bytes` into the *compressed* file is not a valid start position:
zstd frames must be decoded from a frame boundary. An offset into the
*uncompressed* stream is stable but requires decoding the whole shard from the
start (O(shard), hundreds of MB) for one lookup.

Fix: make the writer produce **bounded zstd frames** and record where each
frame starts. zstd concatenated frames are still a valid `.zst` file, so
`zstd -d` and every existing reader path (`open_file_sync`,
`open_file_static`) keep working unchanged.

Then a lookup is: seek the file to `frame_compressed_offset`, decode that one
frame (≤ target frame size, e.g. 4 MiB uncompressed), skip
`offset_in_frame` bytes, read one JSON line.

## Data model

### 1. Shard key = deterministic hash of the file name

The archive directory is not exclusively ours: files arrive from external relay
backups, rsync, or manual drops, and get indexed by whichever process notices
them first. So the shard key **must be derivable from the file name alone**,
with no allocation step and no shared counter:

```rust
/// Stable 64-bit id for a shard. Input is the path relative to `out_dir`
/// (so subdirectories work), with `/` separators, as bytes.
fn shard_hash(rel_name: &str) -> u64   // xxh3_64 (or blake3 truncated), fixed seed
```

Properties:
- Any process that sees `relay-backup-2019.jsonl.gz` computes the same id
  without touching the index — two indexers running concurrently agree, and a
  file dropped in by an external tool needs no registration.
- Nothing to migrate/renumber; a wiped-and-rebuilt index produces identical
  ids, so a partially-rebuilt index stays consistent with the old one.
- Reverse mapping (id -> name) needs **no persisted registry**: list the
  archive dir, hash each entry, keep an in-memory `DashMap<u64, PathBuf>`
  cache, refresh on miss. Files that appear after startup resolve on the next
  refresh.
- Optionally persist that map as a *cache* under `b"s\x00" || u64_be(hash)` ->
  rel name to skip the directory walk on first lookup; it is always rebuildable
  and never authoritative. Such keys are distinguishable from the 32-byte and
  40-byte index keys by length/prefix, so `list_ids`, `repair_count` and the
  time-index backfill filters stay correct (they already filter by `k.len()`;
  add explicit prefix guards to be safe).
- Collisions: with a few thousand shards, 64-bit collision probability is ~1e-13.
  Detect anyway — if the dir walk finds two names hashing equal, log an error
  and refuse the id (fall back to scan). The post-decode `event.id == id` check
  catches any mismatch regardless.

Renames/deletes are handled like a missing file: id not found in the dir walk
-> `created_at`-bounded scan (see "Fallback & migration").

Alternatives considered:
- **Sequential `u32` registry.** Rejected: needs a counter and a write before
  the first insert, breaks for externally-dropped files and concurrent
  indexers, and diverges if the index is wiped while files remain.
- **`shard_id` from the date (days since epoch).** Rejected: imported archives
  have arbitrary names and arbitrary date coverage.
- **Inline the file name in every value.** Self-describing and registry-free,
  but adds ~20-40 bytes per event (primary keys are random ids, so adjacent
  values hold unrelated names and block compression helps less than you'd
  hope). Keep as a fallback if hashing proves awkward.

### 2. Extended primary value (versioned, backwards compatible)

Current value is exactly 8 bytes. The new layout keeps `created_at` as a fixed
prefix and puts an explicit **version byte** immediately after it, so future
format changes are a one-line match instead of a length heuristic:

```
offset  size  field
0       8     created_at        u64 le      (always present, all versions)
8       1     version           u8          (>= 1; absent == v0)
---- v1 payload ----
9       8     shard_hash        u64 le      (xxh3_64 of the relative file name)
17      6     offset_bytes      u48 le      (uncompressed offset in shard, 256 TiB)
23      4     length_bytes      u32 le      (JSON line length, excl. newline)
                                            total: 27 bytes
```

Version dispatch:

```rust
const VALUE_V1: u8 = 1;

fn decode(v: &[u8]) -> Result<IndexEntry> {
    if v.len() < 8 { bail!("index value too short") }
    let created_at = u64::from_le_bytes(v[..8].try_into()?);
    match v.get(8).copied() {
        // v0: legacy timestamp-only entry, no location
        None => Ok(IndexEntry { created_at, loc: None }),
        Some(VALUE_V1) if v.len() >= 27 => Ok(IndexEntry { created_at, loc: Some(EventLoc {
            shard:  u64::from_le_bytes(v[9..17].try_into()?),
            offset: u48_le(&v[17..23]),
            len:    u32::from_le_bytes(v[23..27].try_into()?),
        })}),
        Some(VALUE_V1) => bail!("truncated v1 index value"),
        // Unknown/newer version written by a newer binary: degrade gracefully,
        // we can still serve created_at, check_id, count and the time index.
        Some(other) => { warn!("unknown index value version {other}"); Ok(IndexEntry { created_at, loc: None }) }
    }
}
```

Rules this buys us:
- `len == 8` is version 0 (every existing database) — no rebuild needed.
- A newer binary can add v2 (e.g. wider offsets, a kind/pubkey column) without
  touching v0/v1 readers; old binaries downgrade to "timestamp only" rather
  than misparsing.
- Never reuse a version number; keep the constants in one module
  (`src/database/value.rs`) with encode/decode + round-trip tests per version.

This keeps `contains_key`, `count_keys` and the time index untouched, and old
databases keep working without a rebuild.

`IndexDb` trait changes:

```rust
pub struct EventLoc { pub shard: u64 /* shard_hash */, pub offset: u64, pub len: u32 }
pub struct IndexEntry { pub created_at: u64, pub loc: Option<EventLoc> }
/// encodes to 8 bytes when `loc` is None (v0), 27 bytes when Some (v1)
pub struct IndexValue(IndexEntry);

fn insert(&self, k: [u8;32], v: IndexValue) -> Result<()>;          // was [u8;8]
fn insert_batch(&self, items: Vec<([u8;32], IndexValue)>) -> Result<()>;
fn get(&self, id: &[u8;32]) -> Result<Option<IndexEntry>>;          // new
```

No shard-registry methods on `IndexDb`: `shard_hash(name)` is a pure function
and id -> name resolution lives in `JsonFilesDatabase` (directory walk +
`DashMap` cache), so the index backend stays a dumb KV store and externally
added files need no registration.

`IndexValue` encodes to a v0 (8-byte) or v1 (27-byte) buffer, so the existing
reindex path can keep writing timestamp-only entries if it wants.
Use `([u8; 27], usize)` to avoid a heap alloc per insert.

### 3. Frame table sidecar (per shard)

`events_YYYYMMDD.jsonl.zst.frames`, appended by the writer whenever it closes
a frame. Fixed-size records, so it can be binary-searched with `pread`:

```
header: b"NAFR" || u32_le version(1)
record: u64_le uncompressed_start || u64_le compressed_start   (16 bytes)
```

Lookup: binary search for the greatest `uncompressed_start <= offset_bytes`,
seek the `.zst` to the matching `compressed_start`, decode forward.

If the sidecar is missing (legacy shard, or file copied without it), fall back
to decoding the shard from byte 0 — correct, just slow — and log a warning.
A `rebuild_frame_index()` helper can regenerate sidecars by streaming each
shard and recording frame boundaries.

## Writer changes (`src/database/file.rs`)

`CompressedJsonLFile` becomes offset-aware:

- Own the `File` handle and track `compressed_pos` (from
  `stream_position()`) and `uncompressed_pos`.
- Use `zstd::Encoder` explicitly (not `auto_finish`): after writing a line, if
  `uncompressed_pos - frame_start >= FRAME_TARGET` (default 4 MiB, tunable),
  call `encoder.finish()` -> get the `File` back -> record
  `(frame_uncompressed_start, frame_compressed_start)` in the `.frames`
  sidecar -> start a fresh `Encoder`.
- `write_event` returns `EventLoc { offset: uncompressed_pos_before, len }`.
- On shard rotation (day change) or drop: finish the frame, flush sidecar.

Compression-ratio cost of 4 MiB frames vs one giant frame is small (a few %)
and can be tuned; make `FRAME_TARGET` a field with a builder setter.

## Wiring the offset back into the index

`save_event` currently inserts into the index *before* handing the event to the
writer thread, so it does not know the offset. Two options:

**Option A (recommended) — writer owns the index write.**
Channel payload becomes `Event` only; the writer thread holds a clone of the
`IndexDb` and, after `write_event` returns the `EventLoc`, does the
`insert`/`insert_batch`. `save_event` keeps a cheap `contains_key` check plus
an in-flight `DashSet<[u8;32]>` guard so duplicates sent back-to-back before
the writer drains are still rejected.
- Pros: batchable index writes (one `insert_batch` per drained chunk — much
  faster than today's per-event `WriteBatch` with a count key), single writer.
- Cons: index becomes eventually-consistent w.r.t. `check_id`; the in-flight
  set covers the window. `tests/writer_thread.rs` asserts synchronous dedupe
  and count, so it needs the in-flight guard to keep passing (it will, the
  guard is checked in `save_event`).

**Option B — round-trip.** Send `(Event, oneshot::Sender<EventLoc>)`, await the
location in `save_event`, then insert. Keeps strict ordering/semantics, but
serialises every save on the writer thread and adds latency.

Start with A; it also fixes the current per-event write amplification.

## Performance: frame size + reader pool

Everything except the decode is cheap; the decode is `~0.5 x frame_size` on
average (you must decode from the frame start to reach the event), at roughly
1.5 GB/s per core:

| step                                   | cost |
| -------------------------------------- | ---- |
| RocksDB point get (bloom + cache hit)  | 2-10 us |
| ...cold (1-2 SSD reads)                | 100-300 us |
| shard hash -> path (`DashMap`)         | ~50 ns |
| frame table binary search (in memory)  | ~100 ns |
| `open()` + `seek()` per lookup         | 20-60 us  (pool this away) |
| `zstd::Decoder::new()` (1-2 MB window) | 20-50 us  (pool this away) |
| decode to the event                    | 4 MiB frames: ~1.3 ms / 1 MiB: ~330 us / 256 KiB: ~85 us |

Decisions:

- **Default `FRAME_TARGET` = 512 KiB** (~170 us average decode), tunable via
  `with_frame_target()`. Smaller frames cost compression ratio (each frame
  restarts the zstd window); at 512 KiB with level 3 the loss over a single
  giant frame is a few percent on JSON-L, which is a good trade for a ~8x
  latency win. Benchmark on a real shard before locking the default in.
- **Positioned reads, not `seek`**: read the frame's exact compressed byte
  range with `read_at`/`pread` (both the compressed *and* uncompressed extents
  of a frame are known from the frame table), so one `File` handle is shared
  concurrently with no seek races and no per-lookup `open()`.
- **Early exit**: stream-decode and stop as soon as `offset + len` bytes have
  been produced, so the average decode is half a frame rather than a whole one.

### Measured (100 MB nostr JSON-L, zstd level 3, this box)

| frame size | ratio | decompress |
| ---------- | ----- | ---------- |
| whole file | x2.50 | 2.08 GB/s |
| 4 MiB      | x2.50 | 2.09 GB/s |
| 512 KiB    | x2.53 | 2.13 GB/s |
| **64 KiB** | **x2.59** | **2.31 GB/s** |
| 32 KiB     | x2.56 | 2.21 GB/s |
| 16 KiB     | x2.41 | 1.51 GB/s |

Framing is essentially free: ids, pubkeys and signatures are random hex, so a
bigger zstd window buys nothing and 64 KiB frames compress *better* than one
giant frame. Hence **`DEFAULT_FRAME_TARGET` = 64 KiB** (~15 us average decode).
Below ~32 KiB both ratio and speed degrade.

Cost of small frames is sidecar size: 16 bytes per frame = 256 KB per GB of
shard. So `FrameTable` keeps the records as raw bytes and binary-searches them
in place rather than parsing into a `Vec<FrameStart>` on first touch.

### Batch lookups

`get_many(ids)` exists because per-id costs amortise well:

1. one RocksDB `multi_get` instead of N gets (better block locality);
2. group the resulting locations by `(shard, frame)` and sort by compressed
   offset, so each frame is read+decoded once and IO is near-sequential;
3. optionally fan out across shards on the blocking pool.

For 100 scattered ids this is ~25 ms cold / ~3 ms warm single-threaded; for 100
ids clustered in a day (the common "fetch a thread/profile's events" shape) the
frame grouping collapses most of the decode work.

### `ShardReaderPool`

```rust
pub struct ShardReaderPool {
    /// shard_hash -> open fd, capped (default 256) with LRU eviction so we
    /// never blow the process fd limit on an archive with thousands of shards.
    files: DashMap<u64, Arc<File>>,
    /// Reusable decode slots; checked out per lookup, returned on drop.
    slots: ArrayQueue<DecodeSlot>,      // cap = 2 x available_parallelism
    /// Parsed `.frames` tables, ~16 bytes per frame (a 1 GiB shard at
    /// 512 KiB frames is 2k records = 32 KiB). Cheap to keep forever.
    frames: DashMap<u64, Arc<FrameTable>>,
}

struct DecodeSlot {
    dctx: zstd_safe::DCtx<'static>,  // reset+reused, no realloc per lookup
    cin:  Vec<u8>,                   // compressed frame bytes (pread target)
    out:  Vec<u8>,                   // decoded bytes up to the event
}
```

Checkout is wait-free (`ArrayQueue::pop`, falling back to allocating a
temporary slot when empty, which is dropped instead of returned). All blocking
IO runs under `spawn_blocking`.

Optional, behind a builder flag: a byte-capped LRU of fully decoded frames
(`(shard, frame_start) -> Arc<Vec<u8>>`, default off / 0 bytes). It turns
repeat lookups within a hot shard into a `memcpy`, but it is pure win only for
clustered access patterns, so it stays opt-in.

Expected end-to-end: **~200-400 us cold, ~30-60 us warm** (page cache + index
block cache + pooled decoder), i.e. thousands of lookups/sec/core, and it
parallelises linearly since there is no shared mutable state on the read path.

## Read path

New API on `JsonFilesDatabase`:

```rust
pub fn locate(&self, id: &EventId) -> Result<Option<(String /*shard*/, u64 /*offset*/, u32 /*len*/)>>;
pub async fn read_event_raw(&self, id: &EventId) -> Result<Option<String>>;  // raw JSON line
```

and implement `NostrDatabase::event_by_id`:

1. `database.get(id)` -> `IndexEntry`; `None` -> `Ok(None)`.
2. No `loc` (legacy 8-byte value) -> fallback (below).
3. Resolve `shard_hash` -> path via the cache; on miss, re-walk `out_dir` (this
   is what picks up externally-dropped files) and retry; still missing -> fall
   back to the `created_at`-bounded scan, else `Ok(None)` + warn.
4. Consult the `.frames` sidecar via the pool's `DashMap<u64, Arc<FrameTable>>`
   -> `(compressed_start, compressed_end, uncompressed_start)`.
5. Check out a `DecodeSlot`, `read_at` the compressed frame range into `cin`,
   stream-decode into `out` until `offset - uncompressed_start + len` bytes are
   available (early exit), then slice `len` bytes (or up to `\n`).
6. `serde_json::from_slice::<Event>` -> sanity check `event.id == id`; on
   mismatch log + fall back to a linear scan of the shard (guards against a
   stale/corrupt index).

Blocking file IO goes through `tokio::task::spawn_blocking` since the trait
method is async.

Also worth doing once this lands: implement `query()` for
`Filter { ids: [...] }` on top of the same path, and `count()`.

## Fallback & migration

- **Legacy entries (8-byte values):** `event_by_id` returns `None` by default;
  opt-in `with_scan_fallback(true)` scans shards for the id (bounded by the
  `created_at` we *do* have -> only the shard(s) for that day need scanning).
  This makes migration a non-event.
- **Externally added files** (relay backups dropped into the archive dir) need
  no special handling: `rebuild_index()` / the incremental indexer hashes
  whatever file name it is reading and stores that in the value; lookup
  resolves it via the directory walk. They do need a `.frames` sidecar (or they
  fall back to decoding from offset 0) — see `rebuild_frame_index()`. Note the
  offset is in the *decompressed* stream, so this works for `.gz`/`.bz2`
  imports too, just without frame-level seeking.
- **Backfill:** extend `rebuild_index()` to record locations. This requires the
  reader to report byte offsets, so `SyncChunkedJsonReader` needs to track the
  number of bytes consumed from the decompressed stream and expose the start
  offset + length of each returned object (it already knows `start`/`end`
  within `fill_buf`, so this is a running counter). While rebuilding, also
  regenerate the `.frames` sidecar for each shard by recording zstd frame
  boundaries — or simply write "single frame at 0" for legacy shards, so
  lookups decode from the start of the shard.
- Ship behind no feature flag but bump the minor version; format is
  additive/back-compatible in both directions (an old binary reading a new
  23-byte value only reads the first 8 bytes — it must use `v[..8]`, so audit
  those `try_into()` calls, they currently assume `len == 8`; that assumption
  must be relaxed *before* new values are written).
- Upgrade path for later formats: bump `version`, write v2 for new events, and
  let `decode` keep handling v0/v1 in place. A full rewrite of old values is
  only needed if a field's *meaning* changes — then do it in `repair_count()`'s
  existing full-scan pass rather than adding another one.

## Implementation order

1. Add `src/database/value.rs` with `IndexEntry`/`IndexValue`, the version
   constants and encode/decode, and route every existing read through it —
   relaxing all `value.len() == 8` assumptions (rocksdb + time-index
   backfill). Ship this first so a rollback is safe; at this point everything
   still writes v0.
2. `shard_hash()` + `get()` on `IndexDb`, implemented for rocksdb;
   `JsonFilesDatabase` shard-name cache (directory walk, refresh-on-miss,
   collision detection); start writing v1 values.
3. Offset-aware `CompressedJsonLFile` + `.frames` sidecar + frame-target knob
   (default 512 KiB).
3b. `ShardReaderPool`: fd cache, `DecodeSlot` pool, frame-table cache.
4. Rewire the writer thread to own index inserts (Option A) with the in-flight
   dedupe guard.
5. `locate()` / `read_event_raw()` / `event_by_id()` + frame-table cache.
6. Offset-reporting in `SyncChunkedJsonReader` and location-aware
   `rebuild_index()` + `rebuild_frame_index()`.
7. Tests:
   - round-trip: save N events, read each back by id, compare;
   - lookup across a frame boundary (small `FRAME_TARGET` in the test);
   - lookup across a day rotation / multiple shards;
   - a file dropped into the archive dir *after* startup (external relay
     backup) is indexed and then looked up by id without a restart;
   - shard renamed/deleted after indexing -> scan fallback, never a wrong event;
   - `shard_hash` is stable across processes/rebuilds (golden-value test);
   - v0 (8-byte) value -> `None`, and -> found with scan fallback;
   - missing `.frames` sidecar -> still correct;
   - corrupt offset -> id mismatch detected, no wrong event returned;
   - pool: concurrent lookups from N threads return correct distinct events
     (catches fd/seek/slot-reuse races);
   - fd cache eviction under a tiny cap still serves lookups.

## Benchmarks to add (`benches/` or an example)

- cold/warm `event_by_id` latency percentiles at 256 KiB / 512 KiB / 4 MiB
  frame targets;
- resulting archive size at each frame target (the ratio cost);
- lookups/sec at 1/2/4/8 threads to confirm the pool scales.

## Sizing

Index value grows 8 -> 27 bytes per event. At 300M events that is ~8 GB of
value bytes before compression (RocksDB zstd squashes the high bytes of the
offset and the small set of distinct shard hashes well). Frame sidecars: one
16-byte record per 4 MiB of uncompressed data — negligible.

If those 8 bytes/event matter more than registry-freedom, the shard field can
be narrowed to a truncated `u32` hash (~1e-4 collision probability over a few
thousand shards) — still safe, because the post-decode `event.id == id` check
falls back to a scan on mismatch. That is a v2 format bump, not a redesign.
