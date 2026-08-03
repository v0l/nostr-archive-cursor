//! Measures point-lookup latency for the `shard + offset` index.
//!
//! ```sh
//! cargo run --release --example lookup_bench --features db-rocksdb,sync -- 200000 65536
//! ```
//!
//! Args: `[event_count] [frame_target_bytes]`.

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use std::time::Instant;

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut args = std::env::args().skip(1);
    let count: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(100_000);
    let frame_target: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(64 * 1024);

    let dir = std::env::temp_dir().join(format!("nac-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    println!("archive: {} ({count} events, {frame_target} B frames)", dir.display());

    let db = DefaultJsonFilesDatabase::new_with_frame_target(&dir, frame_target)?;
    let keys = Keys::generate();

    // --- write ---
    let t0 = Instant::now();
    let mut ids = Vec::with_capacity(count as usize);
    for i in 0..count {
        let ev = EventBuilder::new(Kind::TextNote, format!("bench event {i} {}", "x".repeat(180)))
            .custom_created_at(Timestamp::from_secs(1_700_000_000 + i))
            .sign_with_keys(&keys)?;
        ids.push(ev.id);
        db.save_event(&ev).await?;
    }
    db.flush().await?;
    let write = t0.elapsed();

    let archive: u64 = std::fs::read_dir(&dir)?
        .flatten()
        .filter(|e| e.path().is_file())
        .filter_map(|e| e.metadata().ok().map(|m| m.len()))
        .sum();
    println!(
        "write: {count} events in {:.2}s ({:.0}/s), archive {:.1} MiB",
        write.as_secs_f64(),
        count as f64 / write.as_secs_f64(),
        archive as f64 / 1024.0 / 1024.0
    );

    // --- random single lookups ---
    let sample: Vec<EventId> = (0..1000)
        .map(|i| ids[(i * 7919) % ids.len()])
        .collect();

    let mut lat = Vec::with_capacity(sample.len());
    for id in &sample {
        let t = Instant::now();
        let got = db.event_by_id(id).await?;
        lat.push(t.elapsed().as_secs_f64() * 1e6);
        assert!(got.is_some(), "missing {id}");
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "single lookup (warm): p50 {:.0}us  p90 {:.0}us  p99 {:.0}us  max {:.0}us",
        percentile(&lat, 0.50),
        percentile(&lat, 0.90),
        percentile(&lat, 0.99),
        lat.last().copied().unwrap_or_default()
    );

    // --- batched lookups (the shape a relay actually issues) ---
    for batch in [10usize, 100, 1000] {
        let ids: Vec<EventId> = sample.iter().take(batch).copied().collect();
        let t = Instant::now();
        let got = db.get_many_raw(&ids).await;
        let el = t.elapsed();
        assert_eq!(got.iter().filter(|g| g.is_some()).count(), batch);
        println!(
            "batch of {batch:>4}: {:.2}ms total, {:.1}us/event",
            el.as_secs_f64() * 1e3,
            el.as_secs_f64() * 1e6 / batch as f64
        );
    }

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
