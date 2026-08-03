//! Exercises incremental indexing against real archives: import several shards,
//! index, add another, re-index, and confirm only the new work happens.
use nostr_archive_cursor::DefaultJsonFilesDatabase;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let src = PathBuf::from(std::env::args().nth(1).expect("usage: <archive> [per_shard] [shards]"));
    let per: usize = std::env::args().nth(2).and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let shards: usize = std::env::args().nth(3).and_then(|a| a.parse().ok()).unwrap_or(4);

    let dir = std::env::temp_dir().join(format!("nac-incr-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    // Split real events into N shards, written as single-frame imports.
    let mut reader = BufReader::with_capacity(1 << 20, open(&src)?);
    let mut line = Vec::new();
    for s in 0..shards {
        let path = dir.join(format!("relay-import-{s}.jsonl.zst"));
        let mut out = zstd::stream::Encoder::new(std::fs::File::create(&path)?, 3)?;
        let mut n = 0;
        while n < per {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 { break; }
            if line.last() != Some(&b'\n') { break; }
            out.write_all(&line)?;
            n += 1;
        }
        out.finish()?;
    }
    let total: u64 = std::fs::read_dir(&dir)?.flatten().filter_map(|e| e.metadata().ok().map(|m| m.len())).sum();
    println!("{shards} single-frame imports, {:.1} MiB total", total as f64 / 1048576.0);

    let db = DefaultJsonFilesDatabase::new(&dir)?;

    let t = Instant::now();
    let r = db.index_new_shards()?;
    println!("first pass : {r:?} in {:.2}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    let r2 = db.index_new_shards()?;
    println!("second pass: {r2:?} in {:.3}s", t.elapsed().as_secs_f64());

    // Drop in one more archive, as an external backup would.
    let extra = dir.join("relay-import-new.jsonl.zst");
    let mut out = zstd::stream::Encoder::new(std::fs::File::create(&extra)?, 3)?;
    let mut n = 0;
    while n < per {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 { break; }
        if line.last() != Some(&b'\n') { break; }
        out.write_all(&line)?;
        n += 1;
    }
    out.finish()?;

    let t = Instant::now();
    let r3 = db.index_new_shards()?;
    println!("after drop : {r3:?} in {:.2}s", t.elapsed().as_secs_f64());
    println!("total indexed events: {}", db.count_keys());

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
