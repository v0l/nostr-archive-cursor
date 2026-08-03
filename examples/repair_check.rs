//! Salvage report for damaged archives: `cargo run --example repair_check -- <file>...`
use nostr_archive_cursor::{repair_archive, scan_zstd_frames};
use std::fs::File;
use std::io::BufReader;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    for arg in std::env::args().skip(1) {
        let path = std::path::PathBuf::from(&arg);
        let len = File::open(&path)?.metadata()?.len();
        let report = scan_zstd_frames(&mut BufReader::new(File::open(&path)?), len)?;
        println!(
            "{arg}: {len} bytes, {} frames, {} clean prefix, {} damage point(s)",
            report.offsets.len(),
            report.clean_prefix,
            report.damage.len()
        );
        for d in &report.damage {
            println!(
                "   fault @{} in frame @{}: {} -> resync {:?}",
                d.offset, d.frame_start, d.reason, d.resync
            );
        }
        if let Some(r) = repair_archive(&path, 512 * 1024)? {
            println!(
                "   repaired: {} lines, {} bytes, {} dropped",
                r.lines, r.bytes, r.dropped
            );
        }
    }
    Ok(())
}
