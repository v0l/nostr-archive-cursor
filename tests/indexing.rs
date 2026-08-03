#![cfg(all(feature = "db-rocksdb", feature = "sync"))]

use nostr_archive_cursor::{DefaultJsonFilesDatabase, FrameTable, sidecar_path};
use nostr_sdk::prelude::*;
use std::path::{Path, PathBuf};

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nac-indexing-{name}-{}-{:?}",
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

/// Write an archive the way an external tool would: one big zstd frame, no
/// sidecar, and a name that follows none of our conventions.
fn write_import(path: &Path, keys: &Keys, range: std::ops::Range<u64>) -> Vec<EventId> {
    let mut lines = Vec::new();
    let mut ids = Vec::new();
    for i in range {
        let ev = make_event(keys, &format!("import {i} {}", "x".repeat(120)), i);
        lines.extend_from_slice(ev.as_json().as_bytes());
        lines.push(b'\n');
        ids.push(ev.id);
    }
    std::fs::write(path, zstd::encode_all(lines.as_slice(), 3).unwrap()).unwrap();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_indexing_skips_unchanged_shards() {
    let dir = tmp_dir("incremental");
    let keys = Keys::generate();
    let a = dir.join("relay-a.jsonl.zst");
    let ids_a = write_import(&a, &keys, 0..500);

    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();

    // First pass indexes the new shard.
    let r1 = db.index_new_shards().unwrap();
    assert_eq!(r1.shards, 1);
    assert_eq!(r1.indexed, 1);
    assert_eq!(r1.unchanged, 0);
    assert_eq!(r1.new_events, 500);
    assert_eq!(db.count_keys(), 500);

    // Second pass does no work at all: same size + mtime.
    let r2 = db.index_new_shards().unwrap();
    assert_eq!(r2.shards, 1);
    assert_eq!(r2.unchanged, 1, "unchanged shard must be skipped");
    assert_eq!(r2.indexed, 0);
    assert_eq!(r2.new_events, 0);
    assert_eq!(db.count_keys(), 500, "count must not drift on a re-run");

    // A newly dropped archive is picked up without touching the first one.
    let b = dir.join("relay-b.jsonl.zst");
    let ids_b = write_import(&b, &keys, 500..800);
    let r3 = db.index_new_shards().unwrap();
    assert_eq!(r3.shards, 2);
    assert_eq!(r3.unchanged, 1);
    assert_eq!(r3.indexed, 1);
    assert_eq!(r3.new_events, 300);
    assert_eq!(db.count_keys(), 800);

    for id in ids_a.iter().chain(&ids_b) {
        assert!(
            db.event_by_id(id).await.unwrap().is_some(),
            "event {id} not retrievable"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reindexing_the_same_events_does_not_inflate_the_count() {
    let dir = tmp_dir("count");
    let keys = Keys::generate();
    let a = dir.join("relay-a.jsonl.zst");
    write_import(&a, &keys, 0..400);

    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    assert_eq!(db.index_new_shards().unwrap().new_events, 400);
    assert_eq!(db.count_keys(), 400);

    // Force a re-index by touching the file (rewrite identical content).
    let bytes = std::fs::read(&a).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100)); // mtime granularity
    std::fs::write(&a, &bytes).unwrap();

    let r = db.index_new_shards().unwrap();
    assert_eq!(r.indexed, 1, "changed mtime must trigger a re-read");
    assert_eq!(
        r.new_events, 0,
        "re-indexing the same ids adds no new events"
    );
    assert_eq!(
        db.count_keys(),
        400,
        "count must stay exact without repair_count()"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn single_frame_imports_are_reframed_for_seeking() {
    let dir = tmp_dir("reframe");
    let keys = Keys::generate();
    let import = dir.join("relay-backup-2019.jsonl.zst");
    let ids = write_import(&import, &keys, 0..4000);

    // As written: one frame, no sidecar - a lookup would decode the whole file.
    assert!(!sidecar_path(&import).exists());

    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 8192).unwrap();
    let report = db.index_new_shards().unwrap();
    assert_eq!(report.reframed, 1, "coarse import should be reframed");
    assert_eq!(report.indexed, 1);

    let table = FrameTable::load(&sidecar_path(&import)).unwrap().unwrap();
    assert!(
        table.len() > 5,
        "expected many frames after reframing, got {}",
        table.len()
    );
    assert!(
        table.max_frame_span().unwrap() <= 8192 * 4,
        "frames still too coarse to seek"
    );

    // Content is untouched, so every event still reads back correctly.
    for id in [&ids[0], &ids[1999], &ids[3999]] {
        let got = db.event_by_id(id).await.unwrap();
        assert_eq!(got.map(|e| e.id).as_ref(), Some(id));
    }

    // ...and a second pass sees the reframed file as already indexed.
    let again = db.index_new_shards().unwrap();
    assert_eq!(
        again.unchanged, 1,
        "state must be recorded after reframing, not before"
    );
    assert_eq!(again.reframed, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_shard_is_left_to_the_writer() {
    let dir = tmp_dir("live");
    let keys = Keys::generate();
    let db = DefaultJsonFilesDatabase::new(&dir).unwrap();

    // Events saved through the API are indexed inline by the writer thread.
    let mut ids = Vec::new();
    for i in 0..50u64 {
        let ev = make_event(&keys, &format!("live {i}"), i);
        db.save_event(&ev).await.unwrap();
        ids.push(ev.id);
    }
    db.flush().await.unwrap();
    assert_eq!(db.count_keys(), 50);

    // The incremental indexer must not touch (or reframe) the shard being
    // appended to - that would fight the writer for the file.
    let report = db.index_new_shards().unwrap();
    assert_eq!(report.shards, 0, "live shard must be excluded");
    assert_eq!(report.indexed, 0);
    assert_eq!(report.reframed, 0);
    assert_eq!(db.count_keys(), 50);

    for id in &ids {
        assert!(db.event_by_id(id).await.unwrap().is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_index_records_state_so_incremental_is_a_no_op_after() {
    let dir = tmp_dir("rebuild");
    let keys = Keys::generate();
    write_import(&dir.join("relay-a.jsonl.zst"), &keys, 0..300);
    write_import(&dir.join("relay-b.jsonl.zst"), &keys, 300..600);

    let mut db = DefaultJsonFilesDatabase::new(&dir).unwrap();
    db.rebuild_index().unwrap();
    assert_eq!(db.count_keys(), 600);

    let report = db.index_new_shards().unwrap();
    assert_eq!(report.shards, 2);
    assert_eq!(
        report.unchanged, 2,
        "a full rebuild should leave nothing for the incremental pass"
    );
    assert_eq!(report.new_events, 0);
}

/// Bookkeeping keys share the keyspace with event keys, which are told apart
/// purely by length (32 = id, 40 = time index). A shard name that would land
/// on either length must be padded.
#[test]
fn bookkeeping_keys_never_look_like_event_keys() {
    for len in 0..80usize {
        let name = "n".repeat(len);
        let key = nostr_archive_cursor::meta_key(&name);
        assert!(
            key.len() != 32 && key.len() != 40,
            "name of {len} chars produced a {}-byte key, which would be counted as an event",
            key.len()
        );
        assert!(key.starts_with(b"shard/"));
    }
    // Distinct names still map to distinct keys.
    assert_ne!(
        nostr_archive_cursor::meta_key(&"a".repeat(26)),
        nostr_archive_cursor::meta_key(&"a".repeat(27))
    );
}

/// gzip and bzip2 archives cannot be seeked, so an event in a huge `.gz` costs
/// a full decompression. Converting to framed zstd is the fix; the JSON-L
/// stream must come through byte-identical.
#[tokio::test(flavor = "multi_thread")]
async fn gz_import_converts_to_seekable_zst_and_indexes() {
    use nostr_archive_cursor::convert_archive_to_zst;
    use std::io::Write;

    let dir = tmp_dir("convert");
    let keys = Keys::generate();

    // A gzipped relay dump, as found in the wild.
    let gz = dir.join("events-2022-12-27.jsonl.gz");
    let mut raw = Vec::new();
    let mut ids = Vec::new();
    for i in 0..3000u64 {
        let ev = make_event(&keys, &format!("gz {i} {}", "x".repeat(100)), i);
        raw.extend_from_slice(ev.as_json().as_bytes());
        raw.push(b'\n');
        ids.push(ev.id);
    }
    {
        let mut enc =
            flate2::write::GzEncoder::new(std::fs::File::create(&gz).unwrap(), Default::default());
        enc.write_all(&raw).unwrap();
        enc.finish().unwrap();
    }

    let out = convert_archive_to_zst(&gz, 8192).unwrap();
    assert_eq!(out, dir.join("events-2022-12-27.jsonl.zst"));
    assert!(gz.exists(), "the original must be left alone");

    // Byte-identical stream, and now seekable.
    let decoded = zstd::decode_all(std::fs::File::open(&out).unwrap()).unwrap();
    assert_eq!(decoded, raw, "conversion must preserve the JSON-L bytes");
    let table = FrameTable::load(&sidecar_path(&out)).unwrap().unwrap();
    assert!(
        table.len() > 5,
        "expected bounded frames, got {}",
        table.len()
    );

    // Drop the original so it is not indexed twice, then index.
    std::fs::remove_file(&gz).unwrap();
    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, 8192).unwrap();
    let report = db.index_new_shards().unwrap();
    assert_eq!(report.indexed, 1);
    assert_eq!(report.reframed, 0, "conversion already framed it");
    assert_eq!(report.new_events, 3000);

    for id in [&ids[0], &ids[1500], &ids[2999]] {
        let got = db.event_by_id(id).await.unwrap();
        assert_eq!(got.map(|e| e.id).as_ref(), Some(id));
    }
}

/// Converting a `.zst` in place just fixes its framing, rather than trying to
/// write a second file with the same name.
#[test]
fn converting_a_zst_reframes_in_place() {
    use nostr_archive_cursor::convert_archive_to_zst;

    let dir = tmp_dir("convert-zst");
    let keys = Keys::generate();
    let path = dir.join("import.jsonl.zst");
    write_import(&path, &keys, 0..2000);
    let before = zstd::decode_all(std::fs::File::open(&path).unwrap()).unwrap();

    let out = convert_archive_to_zst(&path, 4096).unwrap();
    assert_eq!(out, path);
    assert_eq!(
        zstd::decode_all(std::fs::File::open(&path).unwrap()).unwrap(),
        before
    );
    assert!(
        FrameTable::load(&sidecar_path(&path))
            .unwrap()
            .unwrap()
            .len()
            > 5
    );
}
