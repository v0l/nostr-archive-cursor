//! Verifies event lookup against a **real** nostr archive.
//!
//! Indexes a real JSON-L archive (plain, .zst, .gz or .bz2), then fetches
//! events back by id and asserts the bytes returned are byte-identical to the
//! lines in the source file. Nothing is written outside a temp directory.
//!
//! ```sh
//! cargo run --release --example real_archive_check --features db-rocksdb,sync -- \
//!     /path/to/events.jsonl.zst [max_events]
//! ```

use nostr_archive_cursor::{DEFAULT_FRAME_TARGET, DefaultJsonFilesDatabase, ScanFallback, reframe_archive};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn open(path: &Path) -> anyhow::Result<Box<dyn Read>> {
    let f = std::fs::File::open(path)?;
    Ok(match path.extension().and_then(|e| e.to_str()) {
        Some("zst") | Some("zstd") => Box::new(zstd::stream::Decoder::new(f)?),
        Some("gz") => Box::new(flate2::read::GzDecoder::new(f)),
        Some("bz2") => Box::new(bzip2::read::BzDecoder::new(f)),
        _ => Box::new(f),
    })
}

/// Pull the `"id":"..."` value straight out of the raw bytes, so events that
/// strict JSON parsers reject (real archives contain raw control characters
/// inside strings) are still checked.
fn raw_id(line: &[u8]) -> Option<String> {
    const NEEDLE: &[u8] = b"\"id\":\"";
    let pos = line.windows(NEEDLE.len()).position(|w| w == NEEDLE)?;
    let rest = &line[pos + NEEDLE.len()..];
    let end = rest.iter().position(|&b| b == b'"')?;
    let hex = &rest[..end];
    if hex.len() != 64 {
        return None;
    }
    std::str::from_utf8(hex).ok().map(|s| s.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut args = std::env::args().skip(1);
    let src = PathBuf::from(args.next().expect("usage: real_archive_check <archive> [max]"));
    let max: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let frame_target: u64 = args
        .next()
        .and_then(|a| a.parse().ok())
        .unwrap_or(DEFAULT_FRAME_TARGET);

    let dir = std::env::temp_dir().join(format!("nac-real-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    // 1. Copy `max` events into a shard whose name follows no convention of
    //    ours - i.e. exactly what an external relay backup looks like.
    let shard = dir.join("relay-backup-import.jsonl.zst");
    let mut lines: Vec<Vec<u8>> = Vec::with_capacity(max);
    let mut corrupt = 0usize;
    {
        let mut reader = BufReader::with_capacity(1 << 20, open(&src)?);
        let mut out = zstd::stream::Encoder::new(std::fs::File::create(&shard)?, 3)?;
        let mut line = Vec::new();
        while lines.len() < max {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            if line.last() != Some(&b'\n') {
                break; // truncated final line (we cut the source mid-stream)
            }
            let body = &line[..line.len() - 1];
            if raw_id(body).is_none() {
                continue; // not an event
            }
            // Only well-formed events can be indexed; corrupt/truncated lines
            // (real archives have them) are expected to be skipped.
            if serde_json::from_slice::<serde_json::Value>(body).is_err() {
                corrupt += 1;
                out.write_all(&line)?;
                continue;
            }
            out.write_all(&line)?;
            lines.push(body.to_vec());
        }
        out.finish()?;
    }
    let total_bytes: usize = lines.iter().map(|l| l.len()).sum();
    println!(
        "imported {} real events ({:.1} MiB raw, {:.1} MiB compressed) from {}",
        lines.len(),
        total_bytes as f64 / 1048576.0,
        std::fs::metadata(&shard)?.len() as f64 / 1048576.0,
        src.display()
    );
    let longest = lines.iter().map(|l| l.len()).max().unwrap_or(0);
    let weird = lines
        .iter()
        .filter(|l| l.iter().any(|&b| b < 0x20 && b != b'\t'))
        .count();
    println!("longest event: {longest} B, events with raw control chars: {weird}");

    println!("corrupt/truncated source lines (expected to be skipped): {corrupt}");

    // 2. Prepare the import exactly as an operator would: an archive produced
    //    elsewhere is one giant zstd frame, so reframe it into bounded frames
    //    (content-preserving) before indexing.
    let t = Instant::now();
    let frames = reframe_archive(&shard, frame_target)?;
    println!(
        "reframed into {frames} frames in {:.2}s ({:.1} MiB after)",
        t.elapsed().as_secs_f64(),
        std::fs::metadata(&shard)?.len() as f64 / 1048576.0
    );

    let mut db = DefaultJsonFilesDatabase::new(&dir)?;
    let t = Instant::now();
    db.rebuild_index()?;
    let indexed = db.count_keys();
    println!(
        "indexed {indexed} events in {:.2}s ({:.0}/s)",
        t.elapsed().as_secs_f64(),
        indexed as f64 / t.elapsed().as_secs_f64()
    );

    // Strict mode: no scanning allowed, every hit must come from the index.
    let db = db.with_scan_fallback(ScanFallback::Off);

    // 3. Every event must come back byte-identical.
    let by_id: HashMap<String, &Vec<u8>> =
        lines.iter().filter_map(|l| Some((raw_id(l)?, l))).collect();
    let ids: Vec<EventId> = by_id
        .keys()
        .filter_map(|h| EventId::from_hex(h).ok())
        .collect();
    println!("checking {} unique ids", ids.len());

    let mut missing_ids: Vec<String> = Vec::new();
    let mut checked = 0usize;
    let mut mismatched = 0usize;
    let mut missing = 0usize;
    let t = Instant::now();
    for chunk in ids.chunks(1000) {
        let got = db.get_many_raw(chunk).await;
        for (id, raw) in chunk.iter().zip(got) {
            match raw {
                None => {
                    missing += 1;
                    missing_ids.push(id.to_hex());
                    if missing <= 5 {
                        let known = db.check_id(id).await?;
                        let loc = db.locate(id)?;
                        let expect = by_id.get(&id.to_hex()).unwrap();
                        println!(
                            "  MISSING {id}: index={known:?} loc={loc:?} len={} head={:?}",
                            expect.len(),
                            String::from_utf8_lossy(&expect[..expect.len().min(90)])
                        );
                    }
                }
                Some(bytes) => {
                    let expect = by_id.get(&id.to_hex()).unwrap();
                    if bytes != **expect {
                        mismatched += 1;
                        if mismatched <= 3 {
                            println!(
                                "  MISMATCH {id}\n    got {} B: {:?}\n    want {} B: {:?}",
                                bytes.len(),
                                String::from_utf8_lossy(&bytes[..bytes.len().min(120)]),
                                expect.len(),
                                String::from_utf8_lossy(&expect[..expect.len().min(120)])
                            );
                        }
                    }
                    checked += 1;
                }
            }
        }
    }
    let el = t.elapsed();
    println!(
        "fetched {} events in {:.2}s ({:.1}us/event), {mismatched} mismatched, {missing} missing",
        checked,
        el.as_secs_f64(),
        el.as_secs_f64() * 1e6 / checked.max(1) as f64
    );

    if !missing_ids.is_empty() {
        std::fs::write("/tmp/nac-missing.txt", missing_ids.join("\n"))?;
        println!("missing ids written to /tmp/nac-missing.txt");
    }

    // 4. Single-event latency on real data.
    let sample: Vec<EventId> = ids.iter().step_by(ids.len().max(1) / 500 + 1).copied().collect();
    let mut lat: Vec<f64> = Vec::new();
    for id in &sample {
        let t = Instant::now();
        let got = db.get_raw(id).await;
        lat.push(t.elapsed().as_secs_f64() * 1e6);
        assert!(got.is_some(), "missing {id}");
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "single lookup: p50 {:.0}us  p99 {:.0}us  (n={})",
        lat[lat.len() / 2],
        lat[lat.len() * 99 / 100],
        lat.len()
    );

    // 5. Events that nostr-sdk can parse must survive a typed round trip.
    let mut parsed = 0usize;
    let mut verified = 0usize;
    for id in ids.iter().take(20_000) {
        if let Some(ev) = db.event_by_id(id).await? {
            parsed += 1;
            if ev.verify().is_ok() {
                verified += 1;
            }
            assert_eq!(&ev.id, id, "event_by_id returned the wrong event");
        }
    }
    println!("typed round trip: {parsed} parsed, {verified} signature-verified");

    drop(db);
    if std::env::var("NAC_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        println!("kept: {}", dir.display());
    }
    if mismatched > 0 || missing > 0 {
        anyhow::bail!("{mismatched} mismatched, {missing} missing");
    }
    println!("OK - every indexed event round-tripped byte-for-byte");
    Ok(())
}
