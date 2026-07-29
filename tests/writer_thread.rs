#![cfg(all(feature = "db-rocksdb", feature = "sync"))]

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use std::time::Duration;

fn make_event(sk: &Keys, content: &str, kind: u16, created_offset: u64) -> Event {
    let builder = EventBuilder::new(Kind::Custom(kind), content)
        .custom_created_at(Timestamp::from_secs(1_700_000_000 + created_offset));
    builder.sign_with_keys(sk).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn writer_thread_persists_and_dedupes() {
    let dir = std::env::temp_dir().join(format!("nac-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert!(db.is_index_empty(), "fresh index should be empty");

    let keys = Keys::generate();

    // Save 100 unique events
    let mut ids = Vec::new();
    for i in 0..100u64 {
        let ev = make_event(&keys, &format!("event {i}"), 30078, i);
        ids.push(ev.id);
        let status = db.save_event(&ev).await.unwrap();
        assert_eq!(status, SaveEventStatus::Success);
    }

    // Save 50 duplicates (re-send first 50)
    for i in 0..50u64 {
        let ev = make_event(&keys, &format!("event {i}"), 30078, i);
        let status = db.save_event(&ev).await.unwrap();
        // Inline dedupe in save_event should reject duplicates
        assert_eq!(
            status,
            SaveEventStatus::Rejected(RejectedReason::Duplicate),
            "duplicate should be rejected"
        );
    }

    // Index should contain exactly 100 unique events (dedupe is inline/synchronous)
    assert_eq!(db.count_keys(), 100, "expected 100 unique events indexed");

    // Every event id should be findable via check_id
    for id in &ids {
        let st = db.check_id(id).await.unwrap();
        assert_eq!(st, DatabaseEventStatus::Saved, "id {id} should be saved");
    }

    // Give the writer thread time to drain + flush the compressed file
    tokio::time::sleep(Duration::from_millis(500)).await;
    let files = db.list_files().await.unwrap();
    assert!(!files.is_empty(), "expected at least one archive file");

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn meta_count_persists_across_reopen() {
    let dir = std::env::temp_dir().join(format!("nac-test-reopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let keys = Keys::generate();
    {
        let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
        for i in 0..25u64 {
            let ev = make_event(&keys, &format!("persist {i}"), 30078, i);
            db.save_event(&ev).await.unwrap();
        }
        assert_eq!(db.count_keys(), 25);
        drop(db);
    }

    // Reopen: count should come from the meta key (O(1), no full scan) and be exactly 25
    let db2 = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(db2.count_keys(), 25, "count should persist across reopen");
    assert!(!db2.is_index_empty(), "index should not be empty after reopen");

    drop(db2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn wipe_clears_index_and_count() {
    // rebuild_index() reads files the writer thread may still be compressing, so it
    // cannot be tested reliably while writes are in-flight (zstd frames are unreadable
    // until finished). This test covers the part rebuild depends on: wipe() resets the
    // index + persisted count, which previously bailed "Not supported" on RocksDB.
    let dir = std::env::temp_dir().join(format!("nac-test-wipe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let keys = Keys::generate();
    let mut db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    for i in 0..40u64 {
        let ev = make_event(&keys, &format!("wipe {i}"), 30078, i);
        db.save_event(&ev).await.unwrap();
    }
    assert_eq!(db.count_keys(), 40);
    assert!(!db.is_index_empty());

    // wipe must succeed (not bail) and zero the count
    db.database_wipe_for_test().unwrap();
    assert_eq!(db.count_keys(), 0, "count should be reset after wipe");
    assert!(db.is_index_empty(), "index should be empty after wipe");

    // count must persist as 0 across reopen (meta key written as 0)
    drop(db);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let db2 = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(db2.count_keys(), 0, "wiped count should persist across reopen");

    drop(db2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn repair_count_fixes_stale_meta() {
    // Reproduces the k8s rollout bug: a process opens the index, the persisted
    // meta count is wrong (e.g. written as 0 by a concurrent writer), and restart
    // trusts it. repair_count() must rescan real keys and fix it.
    let dir = std::env::temp_dir().join(format!("nac-test-repair-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let keys = Keys::generate();
    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    for i in 0..30u64 {
        let ev = make_event(&keys, &format!("repair {i}"), 30078, i);
        db.save_event(&ev).await.unwrap();
    }
    assert_eq!(db.count_keys(), 30);
    drop(db);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Simulate the stale-count scenario: overwrite the meta key with 0 directly.
    {
        let rocks = rocksdb::DB::open_default(dir.join("index-rocksdb")).unwrap();
        rocks.put(b"__meta_count__", 0u64.to_le_bytes()).unwrap();
        drop(rocks);
    }

    // Reopen: it trusts the (wrong) persisted 0 -> thinks index is empty
    let db2 = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(db2.count_keys(), 0, "stale meta count should be trusted on open");
    assert!(db2.is_index_empty(), "stale meta makes index look empty");

    // Repair: rescan must find the real 30 keys, fix the atomic + persisted value
    let repaired = db2.repair_count().unwrap();
    assert_eq!(repaired, 30, "repair should find all 30 real keys");
    assert_eq!(db2.count_keys(), 30);
    assert!(!db2.is_index_empty());

    // Persist across reopen
    drop(db2);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let db3 = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(db3.count_keys(), 30, "repaired count should persist");

    drop(db3);
    let _ = std::fs::remove_dir_all(&dir);
}
