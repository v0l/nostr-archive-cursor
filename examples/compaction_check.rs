// Does opening an old uncompressed index actually convert it to zstd?
use nostr_archive_cursor::{IndexDb, IndexEntry, RocksDbIndex};

/// Event ids are 32 bytes of hash output, i.e. uniformly random and
/// incompressible. Sequential/zero-padded ids would make compression look far
/// better than it is in production.
fn event_id(i: u64) -> [u8; 32] {
    let mut id = [0u8; 32];
    let mut x = i.wrapping_add(0x9E3779B97F4A7C15);
    for chunk in id.chunks_mut(8) {
        // splitmix64
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        chunk.copy_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    id
}

fn dir_size(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .map(|d| {
            d.flatten()
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .sum()
        })
        .unwrap_or(0)
}

fn main() -> anyhow::Result<()> {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(2_000_000);
    let dir = std::env::temp_dir().join(format!("nac-compact-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("index-rocksdb");

    // Old-format index: DB::open_default (no compression in this build) + v0 values.
    {
        let db = rocksdb::DB::open_default(&path)?;
        let mut batch = rocksdb::WriteBatch::new();
        for i in 0..n {
            let id = event_id(i as u64);
            let ts = 1_700_000_000u64 + i as u64;
            batch.put(id, ts.to_le_bytes());
            let mut tk = [0u8; 40];
            tk[..8].copy_from_slice(&ts.to_be_bytes());
            tk[8..].copy_from_slice(&id);
            batch.put(tk, []);
            if batch.len() >= 100_000 {
                db.write(std::mem::take(&mut batch))?;
            }
        }
        db.write(batch)?;
        db.put(b"__meta_count__", (n as u64).to_le_bytes())?;
        db.put(b"__meta_timeidx__", [1u8])?;
        db.flush()?;
        let mut opts = rocksdb::Options::default();
        opts.set_disable_auto_compactions(false);
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
    }
    let before = dir_size(&path);
    println!(
        "old-format index ({n} events, uncompressed): {:.1} MiB",
        before as f64 / 1048576.0
    );

    // Open with the new code (zstd + bloom) and read a few keys.
    let idx = RocksDbIndex::open(&path)?;
    println!("opened OK, count = {}", idx.count_keys());
    let id = event_id(5);
    println!("existing v0 entry reads back: {:?}", idx.get(&id)?);
    let after_open = dir_size(&path);
    println!(
        "size after simply opening    : {:.1} MiB",
        after_open as f64 / 1048576.0
    );

    // Force compaction, which is what happens gradually in normal operation.
    idx.compact();
    let after_compact = dir_size(&path);
    println!(
        "size after full compaction   : {:.1} MiB ({:+.0}%)",
        after_compact as f64 / 1048576.0,
        (after_compact as f64 / before as f64 - 1.0) * 100.0
    );
    println!(
        "count still {}, entry still readable: {:?}",
        idx.count_keys(),
        idx.get(&id)?.is_some()
    );

    // And a v1 write into the upgraded DB.
    idx.insert(
        id,
        IndexEntry::located(
            123,
            nostr_archive_cursor::EventLoc {
                shard: 7,
                offset: 42,
                len: 9,
            },
        ),
    )?;
    println!("v1 overwrite of a v0 key -> {:?}", idx.get(&id)?);

    drop(idx);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
