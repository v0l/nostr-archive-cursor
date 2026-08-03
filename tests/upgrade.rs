#![cfg(all(feature = "db-rocksdb", feature = "sync"))]
//! Opening an archive written by the *previous* version: v0 (8-byte) index
//! values, uncompressed SSTs, single-frame shards and no frame sidecars.

use nostr_archive_cursor::{DefaultJsonFilesDatabase, FrameTable, ScanFallback, sidecar_path};
use nostr_sdk::prelude::*;
use std::path::PathBuf;

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nac-upgrade-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Big-endian time-index key, as the old version wrote it.
fn time_key(id: &[u8; 32], ts: u64) -> [u8; 40] {
    let mut k = [0u8; 40];
    k[..8].copy_from_slice(&ts.to_be_bytes());
    k[8..].copy_from_slice(id);
    k
}

/// Build an archive exactly as the previous version left it on disk.
///
/// `created_now` picks the realistic live-relay shape (events written on the
/// day they arrive) versus a historical import (old events in a shard named
/// after the day it was imported).
fn make_old_archive_at(dir: &std::path::Path, n: u64, created_now: bool) -> Vec<Event> {
    let keys = Keys::generate();
    let now = chrono::Utc::now();
    let day = now.format("%Y%m%d").to_string();
    let base = if created_now {
        now.timestamp() as u64 - n // same day as the shard name
    } else {
        1_700_000_000 // an import: events far older than the shard's day
    };

    // Old writer: one zstd frame for the whole shard, no `.frames` sidecar.
    let mut events = Vec::new();
    let mut raw = Vec::new();
    for i in 0..n {
        let ev = EventBuilder::new(Kind::Custom(30078), format!("old {i} {}", "x".repeat(100)))
            .custom_created_at(Timestamp::from_secs(base + i))
            .sign_with_keys(&keys)
            .unwrap();
        raw.extend_from_slice(ev.as_json().as_bytes());
        raw.push(b'\n');
        events.push(ev);
    }
    std::fs::write(
        dir.join(format!("events_{day}.jsonl.zst")),
        zstd::encode_all(raw.as_slice(), 3).unwrap(),
    )
    .unwrap();

    // Old index: uncompressed (open_default without snappy compiled in),
    // 8-byte values, plus the time index and meta keys.
    let db = rocksdb::DB::open_default(dir.join("index-rocksdb")).unwrap();
    for ev in &events {
        let id = *ev.id.as_bytes();
        let ts = ev.created_at.as_secs();
        db.put(id, ts.to_le_bytes()).unwrap();
        db.put(time_key(&id, ts), []).unwrap();
    }
    db.put(b"__meta_count__", (n).to_le_bytes()).unwrap();
    db.put(b"__meta_timeidx__", [1u8]).unwrap();
    drop(db);

    events
}

/// Live-relay shape: events written on the day they arrived.
fn make_old_archive(dir: &std::path::Path, n: u64) -> Vec<Event> {
    make_old_archive_at(dir, n, true)
}

#[tokio::test(flavor = "multi_thread")]
async fn old_index_opens_and_keeps_working() {
    let dir = tmp_dir("open");
    let events = make_old_archive(&dir, 500);

    // Must open without error or migration.
    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(db.count_keys(), 500, "existing count must be trusted as-is");
    assert!(!db.is_index_empty());

    // Dedupe and time queries work untouched.
    for ev in events.iter().take(20) {
        assert_eq!(
            db.check_id(&ev.id).await.unwrap(),
            DatabaseEventStatus::Saved
        );
    }
    assert_eq!(db.list_ids(0, u64::MAX).len(), 500);

    // v0 entries carry no location...
    assert!(db.locate(&events[0].id).unwrap().is_none());
    // ...so lookups fall back to scanning that day's shard, and still work.
    let got = db.event_by_id(&events[42].id).await.unwrap();
    assert_eq!(got.map(|e| e.id), Some(events[42].id));

    // With scanning off, an un-migrated entry is simply "not found" - never wrong.
    let strict = db.clone().with_scan_fallback(ScanFallback::Off);
    assert!(strict.event_by_id(&events[42].id).await.unwrap().is_none());
}

/// Shards are named after the day they were *written*, so for an archive of
/// imported history the cheap `Day` guess cannot find anything: those events'
/// `created_at` points at a shard that never existed. This is the case that
/// makes reindexing (not scanning) the right migration for old archives.
#[tokio::test(flavor = "multi_thread")]
async fn historical_import_needs_full_scan_or_a_reindex() {
    let dir = tmp_dir("historical");
    let events = make_old_archive_at(&dir, 300, false);

    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(
        db.event_by_id(&events[7].id).await.unwrap(),
        None,
        "Day fallback cannot find events whose created_at day has no shard"
    );

    let full = db.clone().with_scan_fallback(ScanFallback::All);
    assert_eq!(
        full.event_by_id(&events[7].id).await.unwrap().map(|e| e.id),
        Some(events[7].id),
        "scanning every shard finds it, at O(archive) per miss"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn new_writes_are_v1_alongside_old_v0_entries() {
    let dir = tmp_dir("mixed");
    let old = make_old_archive(&dir, 200);

    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    let keys = Keys::generate();
    let fresh = EventBuilder::new(Kind::Custom(30078), "written after upgrade")
        .sign_with_keys(&keys)
        .unwrap();
    db.save_event(&fresh).await.unwrap();
    db.flush().await.unwrap();

    // Mixed layouts coexist: old ones scan, new ones are located.
    assert!(db.locate(&old[0].id).unwrap().is_none());
    assert!(db.locate(&fresh.id).unwrap().is_some());
    assert_eq!(db.count_keys(), 201);

    let strict = db.clone().with_scan_fallback(ScanFallback::Off);
    assert_eq!(
        strict.event_by_id(&fresh.id).await.unwrap().map(|e| e.id),
        Some(fresh.id),
        "new events must be O(1) even while old ones are not"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn indexing_upgrades_old_entries_to_located_lookups() {
    let dir = tmp_dir("migrate");
    let events = make_old_archive(&dir, 400);
    let day_shard = dir.join(format!(
        "events_{}.jsonl.zst",
        chrono::Utc::now().format("%Y%m%d")
    ));
    assert!(
        !sidecar_path(&day_shard).exists(),
        "old archives have no sidecar"
    );

    let mut db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 8192).unwrap();
    // A full reindex is what migrates v0 -> v1 (the incremental pass skips the
    // shard the writer owns, which for a same-day archive is this one).
    db.rebuild_index().unwrap();

    assert_eq!(db.count_keys(), 400);
    let table = FrameTable::load(&sidecar_path(&day_shard))
        .unwrap()
        .unwrap();
    assert!(
        table.len() > 5,
        "shard should now be seekable: {} frames",
        table.len()
    );

    // Every event is now located, so lookups need no scanning at all.
    let strict = db.clone().with_scan_fallback(ScanFallback::Off);
    for ev in &events {
        assert!(
            strict.locate(&ev.id).unwrap().is_some(),
            "{} not migrated",
            ev.id
        );
        let got = strict.event_by_id(&ev.id).await.unwrap();
        assert_eq!(got.map(|e| e.id).as_ref(), Some(&ev.id));
    }
}

/// The old index has no per-shard bookkeeping, so the first incremental pass
/// treats every shard as new - and reframes it, which rewrites the archive.
#[tokio::test(flavor = "multi_thread")]
async fn first_incremental_pass_reindexes_everything_then_settles() {
    let dir = tmp_dir("incremental");
    make_old_archive(&dir, 300);
    // Rename off the live-shard name so the incremental indexer will touch it
    // (the current day's shard belongs to the writer).
    let day = chrono::Utc::now().format("%Y%m%d").to_string();
    std::fs::rename(
        dir.join(format!("events_{day}.jsonl.zst")),
        dir.join("events_20200101.jsonl.zst"),
    )
    .unwrap();

    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 8192).unwrap();
    let first = db.index_new_shards().unwrap();
    assert_eq!(first.shards, 1);
    assert_eq!(first.unchanged, 0, "no bookkeeping exists yet");
    assert_eq!(first.indexed, 1);
    assert_eq!(first.reframed, 1, "single-frame shard gets rewritten");
    assert_eq!(
        first.new_events, 0,
        "ids already existed; only their locations were added"
    );
    assert_eq!(db.count_keys(), 300, "count must not double-count");

    // ...and it settles: a second pass does nothing.
    let second = db.index_new_shards().unwrap();
    assert_eq!(second.unchanged, 1);
    assert_eq!(second.indexed, 0);
    assert_eq!(second.reframed, 0);
}
