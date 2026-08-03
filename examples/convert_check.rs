//! Converts a real archive to framed zstd and verifies the stream survives.
use nostr_archive_cursor::{
    DEFAULT_FRAME_TARGET, FrameTable, convert_archive_to_zst, sidecar_path,
};
use std::io::{Read, Write};
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

fn main() -> anyhow::Result<()> {
    let src = PathBuf::from(std::env::args().nth(1).expect("usage: <archive> [bytes]"));
    let limit: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(200_000_000);

    let dir = std::env::temp_dir().join(format!("nac-conv-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    // Copy a prefix of the real archive, preserving its original format.
    let name = src.file_name().unwrap().to_str().unwrap();
    let local = dir.join(name);
    let mut raw = vec![0u8; limit];
    let n = {
        let mut r = open(&src)?;
        let mut got = 0;
        while got < limit {
            match r.read(&mut raw[got..])? {
                0 => break,
                k => got += k,
            }
        }
        got
    };
    raw.truncate(
        raw[..n]
            .iter()
            .rposition(|&b| b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(n),
    );
    match src.extension().and_then(|e| e.to_str()) {
        Some("gz") => {
            let mut e =
                flate2::write::GzEncoder::new(std::fs::File::create(&local)?, Default::default());
            e.write_all(&raw)?;
            e.finish()?;
        }
        Some("bz2") => {
            let mut e = bzip2::write::BzEncoder::new(
                std::fs::File::create(&local)?,
                bzip2::Compression::default(),
            );
            e.write_all(&raw)?;
            e.finish()?;
        }
        Some("zst") | Some("zstd") => std::fs::write(&local, zstd::encode_all(&raw[..], 3)?)?,
        _ => std::fs::write(&local, &raw)?,
    }
    println!(
        "source {name}: {:.1} MiB raw -> local copy {:.1} MiB",
        raw.len() as f64 / 1048576.0,
        std::fs::metadata(&local)?.len() as f64 / 1048576.0
    );

    let t = Instant::now();
    let out = convert_archive_to_zst(&local, DEFAULT_FRAME_TARGET)?;
    let elapsed = t.elapsed().as_secs_f64();

    let mut decoded = Vec::new();
    zstd::stream::Decoder::new(std::fs::File::open(&out)?)?.read_to_end(&mut decoded)?;
    let table = FrameTable::load(&sidecar_path(&out))?.unwrap();
    println!(
        "converted in {elapsed:.1}s -> {} ({:.1} MiB, {} frames, max span {} B)",
        out.file_name().unwrap().to_str().unwrap(),
        std::fs::metadata(&out)?.len() as f64 / 1048576.0,
        table.len(),
        table.max_frame_span().unwrap_or(0)
    );
    assert_eq!(decoded, raw, "conversion changed the JSON-L stream!");
    println!(
        "OK - {} bytes byte-identical after conversion",
        decoded.len()
    );

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
