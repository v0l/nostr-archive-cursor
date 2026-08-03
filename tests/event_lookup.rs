#![cfg(all(feature = "db-rocksdb", feature = "sync"))]

use nostr_archive_cursor::{DefaultJsonFilesDatabase, ScanFallback, rebuild_frame_index, shard_hash};
use nostr_sdk::prelude::*;
use std::path::PathBuf;

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nac-lookup-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn make_event(keys: &Keys, content: &str, created_offset: u64) -> Event {
    EventBuilder::new(Kind::Custom(30078), content)
        .custom_created_at(Timestamp::from_secs(1_700_000_000 + created_offset))
        .sign_with_keys(keys)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_saved_event_is_retrievable_by_id() {
    let dir = tmp_dir("roundtrip");
    // Tiny frames so 500 events span many frames and cross boundaries.
    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 2048).unwrap();
    let keys = Keys::generate();

    let mut saved = Vec::new();
    for i in 0..500u64 {
        let ev = make_event(&keys, &format!("event {i} {}", "x".repeat(50)), i);
        assert_eq!(db.save_event(&ev).await.unwrap(), SaveEventStatus::Success);
        saved.push(ev);
    }
    db.flush().await.unwrap();

    for ev in &saved {
        let got = db
            .event_by_id(&ev.id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("event {} not found", ev.id));
        assert_eq!(got.id, ev.id);
        assert_eq!(got.content, ev.content);
        assert_eq!(got.sig, ev.sig, "signature must survive the round trip");
        assert!(got.verify().is_ok(), "retrieved event must still verify");
    }

    // Unknown id
    let missing = make_event(&keys, "never saved", 99_999);
    assert!(db.event_by_id(&missing.id).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_lookup_matches_individual_lookup() {
    let dir = tmp_dir("batch");
    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 4096).unwrap();
    let keys = Keys::generate();

    let mut ids = Vec::new();
    for i in 0..300u64 {
        let ev = make_event(&keys, &format!("batch {i}"), i);
        db.save_event(&ev).await.unwrap();
        ids.push(ev.id);
    }
    db.flush().await.unwrap();

    // Interleave a missing id to check slot alignment.
    let missing = make_event(&keys, "missing", 1_000_000).id;
    let mut query: Vec<EventId> = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        query.push(*id);
        if i % 50 == 0 {
            query.push(missing);
        }
    }

    let batch = db.get_many_raw(&query).await;
    assert_eq!(batch.len(), query.len());
    for (raw, id) in batch.iter().zip(&query) {
        if *id == missing {
            assert!(raw.is_none(), "missing id must stay None");
            continue;
        }
        let ev: Event = serde_json::from_slice(raw.as_ref().unwrap()).unwrap();
        assert_eq!(&ev.id, id);
        // ...and identical to the single-id path
        let single = db.event_by_id(id).await.unwrap().unwrap();
        assert_eq!(single.id, ev.id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lookup_survives_reopen() {
    let dir = tmp_dir("reopen");
    let keys = Keys::generate();
    let mut ids = Vec::new();
    {
        let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 2048).unwrap();
        for i in 0..200u64 {
            let ev = make_event(&keys, &format!("before {i}"), i);
            db.save_event(&ev).await.unwrap();
            ids.push(ev.id);
        }
        db.flush().await.unwrap();
    }

    // Reopen and append more: offsets must continue the logical stream, and
    // the events written before the restart must still be findable.
    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 2048).unwrap();
    for i in 200..400u64 {
        let ev = make_event(&keys, &format!("after {i}"), i);
        db.save_event(&ev).await.unwrap();
        ids.push(ev.id);
    }
    db.flush().await.unwrap();

    for id in &ids {
        assert!(
            db.event_by_id(id).await.unwrap().is_some(),
            "event {id} lost across reopen"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn externally_dropped_archive_is_found_after_reindex() {
    let dir = tmp_dir("external");
    let keys = Keys::generate();

    // An archive produced elsewhere (external relay backup) with a name that
    // does not follow our events_YYYYMMDD convention.
    let external = dir.join("relay-backup-2019.jsonl.zst");
    let mut external_events = Vec::new();
    {
        let mut lines = Vec::new();
        for i in 0..100u64 {
            let ev = make_event(&keys, &format!("external {i}"), i);
            lines.extend_from_slice(serde_json::to_string(&ev).unwrap().as_bytes());
            lines.push(b'\n');
            external_events.push(ev);
        }
        std::fs::write(&external, zstd::encode_all(lines.as_slice(), 3).unwrap()).unwrap();
    }
    // No sidecar exists for it; generate one so lookups can seek.
    rebuild_frame_index(&external).unwrap();

    let mut db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    db.rebuild_index().unwrap();

    // The shard id is derived from the file name alone - no registration.
    assert_eq!(
        db.shard_path(shard_hash("relay-backup-2019.jsonl.zst")),
        Some(external.clone())
    );

    for ev in &external_events {
        let got = db
            .event_by_id(&ev.id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("external event {} not found", ev.id));
        assert_eq!(got.content, ev.content);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_offset_never_returns_the_wrong_event() {
    let dir = tmp_dir("stale");
    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 2048).unwrap();
    let keys = Keys::generate();

    let mut ids = Vec::new();
    for i in 0..100u64 {
        let ev = make_event(&keys, &format!("stale {i}"), i);
        db.save_event(&ev).await.unwrap();
        ids.push(ev.id);
    }
    db.flush().await.unwrap();

    // Corrupt one entry's location so it points at a different event's line,
    // with the scan fallback disabled so only the id check can save us.
    // Clone the handle (a second open would fight over the RocksDB lock).
    let strict = db.clone().with_scan_fallback(ScanFallback::Off);
    let victim = ids[10];
    let decoy = strict.locate(&ids[42]).unwrap().unwrap();
    strict.overwrite_location_for_test(&victim, decoy).unwrap();

    assert!(
        strict.event_by_id(&victim).await.unwrap().is_none(),
        "a location pointing at another event must be rejected, not returned"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_v0_index_still_serves_lookups_by_scanning() {
    // A database written before locations existed: values are 8 bytes, so the
    // index knows the event exists and when, but not where.
    let dir = tmp_dir("legacy");
    let keys = Keys::generate();
    let mut ids = Vec::new();
    {
        let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
        for i in 0..50u64 {
            let ev = make_event(&keys, &format!("legacy {i}"), i);
            db.save_event(&ev).await.unwrap();
            ids.push((ev.id, ev.created_at.as_secs()));
        }
        db.flush().await.unwrap();
    }

    // Downgrade every value to the legacy 8-byte layout, in place.
    {
        let rocks = rocksdb::DB::open_default(dir.join("index-rocksdb")).unwrap();
        for (id, created_at) in &ids {
            rocks
                .put(id.as_bytes(), created_at.to_le_bytes())
                .unwrap();
        }
    }

    // These events carry 2023 timestamps but were written to today's shard, so
    // the cheap Day guess cannot find them - exactly the historical-import case.
    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert!(db.locate(&ids[0].0).unwrap().is_none(), "v0 entries have no location");
    assert!(
        db.event_by_id(&ids[0].0).await.unwrap().is_none(),
        "Day fallback must not find an event written to a different day's shard"
    );

    let full = db.clone().with_scan_fallback(ScanFallback::All);
    for (id, _) in &ids {
        assert!(
            full.event_by_id(id).await.unwrap().is_some(),
            "v0 entry {id} must resolve under ScanFallback::All"
        );
    }

    // ...and with the fallback off it degrades to "not found", never wrong.
    let strict = db.clone().with_scan_fallback(ScanFallback::Off);
    assert!(strict.event_by_id(&ids[0].0).await.unwrap().is_none());
}
