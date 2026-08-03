//! Measures index size per event, with and without event locations.
//!
//! ```sh
//! cargo run --release --example index_size --features db-rocksdb,sync -- \
//!     /path/to/events.jsonl.zst 5000000
//! ```

use nostr_archive_cursor::{EventLoc, IndexDb, IndexEntry, RocksDbIndex, shard_hash};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
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

fn field<'a>(line: &'a [u8], name: &str) -> Option<&'a [u8]> {
    let needle = format!("\"{name}\":");
    let p = line
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())?;
    let rest = &line[p + needle.len()..];
    Some(rest)
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            match e.metadata() {
                Ok(m) if m.is_dir() => total += dir_size(&e.path()),
                Ok(m) => total += m.len(),
                Err(_) => {}
            }
        }
    }
    total
}

fn gib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let src = std::path::PathBuf::from(args.next().expect("usage: index_size <archive> [n]"));
    let n: usize = args
        .next()
        .and_then(|a| a.parse().ok())
        .unwrap_or(5_000_000);
    const TARGET: u64 = 900_000_000;

    // Collect (id, created_at, offset, len) from real events, exactly what the
    // indexer would record.
    println!("reading {n} events from {}...", src.display());
    let t = Instant::now();
    let mut rows: Vec<([u8; 32], u64, u64, u32)> = Vec::with_capacity(n);
    let shard = shard_hash("events_20250801.jsonl.zst");
    let mut reader = BufReader::with_capacity(1 << 20, open(&src)?);
    let mut line = Vec::new();
    let mut offset = 0u64;
    while rows.len() < n {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let body = &line[..line.len().saturating_sub(1)];
        let (Some(id_rest), Some(ts_rest)) = (field(body, "id"), field(body, "created_at")) else {
            offset += line.len() as u64;
            continue;
        };
        let hex = id_rest.strip_prefix(b"\"").unwrap_or(id_rest);
        let mut id = [0u8; 32];
        if hex.len() < 64 || faster_hex::hex_decode(&hex[..64], &mut id).is_err() {
            offset += line.len() as u64;
            continue;
        }
        let ts: u64 = std::str::from_utf8(ts_rest)
            .ok()
            .and_then(|s| {
                s.split(|c: char| !c.is_ascii_digit())
                    .find(|p| !p.is_empty())?
                    .parse()
                    .ok()
            })
            .unwrap_or(0);
        rows.push((id, ts, offset, body.len() as u32));
        offset += line.len() as u64;
    }
    println!(
        "collected {} events in {:.1}s (archive spans {:.1} GiB decompressed)",
        rows.len(),
        t.elapsed().as_secs_f64(),
        gib(offset)
    );
    let count = rows.len() as u64;

    // Build both index layouts from identical data.
    let base = std::env::temp_dir().join(format!("nac-idxsize-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base)?;

    let mut results = Vec::new();
    for (label, located) in [("v0 (created_at only)", false), ("v1 (+ location)", true)] {
        let path = base.join(if located { "v1" } else { "v0" });
        let idx = RocksDbIndex::open(&path)?;
        let t = Instant::now();
        let mut batch = Vec::with_capacity(10_000);
        for (id, ts, off, len) in &rows {
            let entry = if located {
                IndexEntry::located(
                    *ts,
                    EventLoc {
                        shard,
                        offset: *off,
                        len: *len,
                    },
                )
            } else {
                IndexEntry::new(*ts)
            };
            batch.push((*id, entry));
            if batch.len() == 10_000 {
                idx.insert_batch(std::mem::take(&mut batch))?;
                batch.reserve(10_000);
            }
        }
        if !batch.is_empty() {
            idx.insert_batch(batch)?;
        }
        let elapsed = t.elapsed().as_secs_f64();
        drop(idx);

        let size = dir_size(&path);
        let per_event = size as f64 / count as f64;
        println!(
            "{label:22}: {:.2} GiB for {count} events = {per_event:.1} B/event  ({:.0} inserts/s)",
            gib(size),
            count as f64 / elapsed
        );
        results.push((label, per_event));
    }

    println!("\nExtrapolated to {} events:", TARGET);
    for (label, per_event) in &results {
        println!(
            "  {label:22}: {:.1} GiB",
            per_event * TARGET as f64 / 1024.0 / 1024.0 / 1024.0
        );
    }
    let delta = results[1].1 - results[0].1;
    println!(
        "  location overhead   : {:+.1} B/event = {:+.1} GiB at {} events ({:+.0}%)",
        delta,
        delta * TARGET as f64 / 1024.0 / 1024.0 / 1024.0,
        TARGET,
        (results[1].1 / results[0].1 - 1.0) * 100.0
    );

    let _ = std::fs::remove_dir_all(&base);
    Ok(())
}
