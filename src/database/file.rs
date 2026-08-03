use crate::database::frames::{FrameLog, FrameStart, FrameTable, sidecar_path};
use crate::database::value::EventLoc;
use anyhow::{Result, bail};
use log::warn;
use serde::Serialize;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zstd::Encoder;

/// Default amount of *uncompressed* data per zstd frame.
///
/// This is the main lookup-latency knob: a point lookup decodes from the start
/// of the containing frame, so average decode work is ~half a frame.
///
/// Measured on 200k **real** events from a nostr.band archive
/// (`examples/real_archive_check.rs`):
///
/// ```text
/// frame        archive   vs single frame   single lookup   batched
///   8 KiB     57.7 MiB        +46%              22 us      4.2 us/ev
///  32 KiB     52.0 MiB        +32%              35 us      4.4 us/ev
/// 128 KiB     48.5 MiB        +23%              85 us      5.7 us/ev
/// 512 KiB     44.6 MiB        +13%             472 us      7.1 us/ev  <- default
///   4 MiB     40.0 MiB        +1.5%           2131 us     10.9 us/ev
/// single      39.4 MiB          -            (whole shard decoded)
/// ```
///
/// Smaller frames are *not* free on real data: events share pubkeys, tags and
/// phrasing, so a bigger zstd window compresses better. Raising the
/// compression level does not recover it (a frame cannot see past itself);
/// only a trained dictionary would, at the cost of the archive no longer being
/// a plain `.zst`.
///
/// 512 KiB trades single-event latency - which batching largely hides, since
/// [`ShardReaderPool::read_many`](crate::ShardReaderPool::read_many) decodes
/// each frame once for the whole group - for 19% less disk than 32 KiB.
/// Lower it if interactive single-id fetches dominate.
pub const DEFAULT_FRAME_TARGET: u64 = 512 * 1024;

/// A ZSTD compressed JSON-L appender that reports where each event landed.
///
/// Events are written as concatenated bounded zstd frames (still a plain
/// valid `.zst` file), and every frame boundary is recorded in a
/// `<shard>.frames` sidecar so a reader can seek near an offset instead of
/// decoding the whole shard.
pub struct CompressedJsonLFile {
    /// `None` only between finishing one frame and starting the next.
    stream: Option<Encoder<'static, File>>,
    frames: FrameLog,
    path: PathBuf,
    level: i32,
    frame_target: u64,
    /// Offset in the decompressed stream where the next event will start.
    uncompressed_pos: u64,
    /// Offset in the file where the current frame starts.
    frame_compressed_start: u64,
    /// Decompressed offset where the current frame starts.
    frame_uncompressed_start: u64,
    /// Scratch buffer for serialising events.
    buf: Vec<u8>,
}

impl CompressedJsonLFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_frame_target(path, DEFAULT_FRAME_TARGET)
    }

    /// Open an archive with a custom frame size (see [`DEFAULT_FRAME_TARGET`]).
    pub fn with_frame_target<P: AsRef<Path>>(path: P, frame_target: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let level = 3;
        let mut file = File::options().create(true).append(true).open(&path)?;
        let compressed_len = file.seek(SeekFrom::End(0))?;

        // Resume: work out the decompressed length already in the file so new
        // offsets continue the logical stream.
        let uncompressed_pos = Self::resume_position(&path, compressed_len)?;

        let mut frames = FrameLog::open(&path)?;
        // Record the boundary for the frame we are about to open.
        frames.append(FrameStart {
            uncompressed: uncompressed_pos,
            compressed: compressed_len,
        })?;

        Ok(Self {
            stream: Some(Encoder::new(file, level)?),
            frames,
            path,
            level,
            frame_target: frame_target.max(1),
            uncompressed_pos,
            frame_compressed_start: compressed_len,
            frame_uncompressed_start: uncompressed_pos,
            buf: Vec::with_capacity(4096),
        })
    }

    /// Decompressed length of an existing archive, cheaply where possible.
    ///
    /// The sidecar's last record is the start of the last frame, so only that
    /// frame (bounded by the frame target) needs decoding. Without a sidecar
    /// we must decode the whole file once - correct, but slow, hence the warn.
    fn resume_position(path: &Path, compressed_len: u64) -> Result<u64> {
        if compressed_len == 0 {
            return Ok(0);
        }
        let (tail_uncompressed, tail_compressed) = match FrameTable::load(&sidecar_path(path))? {
            Some(t) => match t.last() {
                Some(last) => (last.uncompressed, last.compressed),
                None => (0, 0),
            },
            None => {
                warn!(
                    "{}: no frame index, decoding the whole shard once to resume appending",
                    path.display()
                );
                (0, 0)
            }
        };
        if tail_compressed >= compressed_len {
            // Sidecar already covers the whole file (clean shutdown).
            return Ok(tail_uncompressed);
        }
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(tail_compressed))?;
        let mut decoder = zstd::stream::Decoder::new(file)?;
        // Count as we go: the last frame is normally *unfinished* (the writer
        // block-flushes rather than closing frames), and a crash can also
        // truncate it. Either way we keep every byte that decoded cleanly and
        // append after it - `io::copy` would discard that count on error.
        let mut n = 0u64;
        let mut buf = vec![0u8; 128 * 1024];
        loop {
            match decoder.read(&mut buf) {
                Ok(0) => break,
                Ok(read) => n += read as u64,
                Err(e) => {
                    warn!(
                        "{}: incomplete tail frame ({e}), resuming after {n} decoded bytes",
                        path.display()
                    );
                    break;
                }
            }
        }
        Ok(tail_uncompressed + n)
    }

    /// Append one event, returning where its JSON line landed in the
    /// decompressed stream.
    pub fn write_event<O: Serialize>(&mut self, event: &O) -> Result<EventLoc> {
        self.buf.clear();
        serde_json::to_writer(&mut self.buf, event)?;
        self.buf.push(b'\n');

        let offset = self.uncompressed_pos;
        let len = (self.buf.len() - 1) as u32; // excluding the newline

        let stream = self
            .stream
            .as_mut()
            .expect("encoder is only detached inside roll_frame");
        stream.write_all(&self.buf)?;
        self.uncompressed_pos += self.buf.len() as u64;

        if self.uncompressed_pos - self.frame_uncompressed_start >= self.frame_target {
            self.roll_frame()?;
        }

        Ok(EventLoc {
            shard: 0, // filled in by the caller, which knows the file name
            offset,
            len,
        })
    }

    /// Close the current zstd frame and open a new one, recording the boundary.
    fn roll_frame(&mut self) -> Result<()> {
        let encoder = self.stream.take().expect("encoder present");
        let mut file = encoder.finish()?;
        let compressed = file.stream_position()?;
        self.frames.append(FrameStart {
            uncompressed: self.uncompressed_pos,
            compressed,
        })?;
        self.frame_compressed_start = compressed;
        self.frame_uncompressed_start = self.uncompressed_pos;
        self.stream = Some(Encoder::new(file, self.level)?);
        Ok(())
    }

    /// Current decompressed length (offset the next event would get).
    pub fn uncompressed_len(&self) -> u64 {
        self.uncompressed_pos
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Make everything written so far readable, without ending the frame.
    ///
    /// This is a zstd block flush (`ZSTD_e_flush`): the bytes hit the file and
    /// a decoder streaming the frame can produce them, but the frame stays
    /// open so it keeps growing to [`frame_target`](Self::with_frame_target).
    ///
    /// Ending the frame here instead would make frames as small as a writer
    /// batch - one event per frame for a quiet relay - which bloats the frame
    /// index (16 bytes per frame) and wastes frame headers.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.as_mut() {
            stream.flush()?;
        }
        Ok(())
    }

    /// Close the current frame early (e.g. before rotating shards).
    pub fn flush_frame(&mut self) -> Result<()> {
        if self.uncompressed_pos > self.frame_uncompressed_start {
            self.roll_frame()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        if let Some(encoder) = self.stream.take() {
            let mut file = encoder.finish()?;
            file.flush()?;
        }
        Ok(())
    }
}

impl Drop for CompressedJsonLFile {
    fn drop(&mut self) {
        // Close the frame cleanly so the file is not left with a partial one.
        if let Some(encoder) = self.stream.take() {
            match encoder.finish() {
                Ok(mut f) => {
                    let _ = f.flush();
                }
                Err(e) => warn!("{}: failed to finish zstd frame: {e}", self.path.display()),
            }
        }
    }
}

/// Rebuild the `.frames` sidecar for an existing zstd archive by walking its
/// frame headers and decoding each frame to measure its decompressed size.
///
/// Needed for shards written before framing existed and for archives dropped
/// into the directory by an external relay backup. Costs one full decompress.
pub fn rebuild_frame_index(path: &Path) -> Result<usize> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    if data.is_empty() {
        return Ok(0);
    }
    let offsets = crate::database::frames::scan_zstd_frame_offsets(&data)?;

    // Write to a temp path then rename, so a crash can't leave a sidecar that
    // disagrees with the shard.
    let final_path = sidecar_path(path);
    let mut tmp = final_path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let mut log = FrameLog::open_at(&tmp)?;
    let mut uncompressed = 0u64;
    for (i, &start) in offsets.iter().enumerate() {
        log.append(FrameStart {
            uncompressed,
            compressed: start,
        })?;
        let end = offsets.get(i + 1).copied().unwrap_or(data.len() as u64);
        let mut decoder = zstd::stream::Decoder::new(&data[start as usize..end as usize])?;
        uncompressed += std::io::copy(&mut decoder, &mut std::io::sink())?;
    }
    drop(log);
    std::fs::rename(&tmp, &final_path)?;
    Ok(offsets.len())
}

/// Convert any supported archive (`.gz`, `.bz2`, plain `.jsonl`, or a
/// single-frame `.zst`) into a **framed** zstd archive that lookups can seek
/// into, writing `<stem>.jsonl.zst` beside it plus its frame sidecar.
///
/// gzip and bzip2 cannot be seeked at all, so an event in a 550 GB `.gz` costs
/// a full decompression to read. Converting is the only way to make such an
/// import usable.
///
/// The decompressed bytes are preserved exactly, so the file is the same
/// JSON-L stream, just packaged differently. The shard id is derived from the
/// file *name*, which changes here, so convert **before** indexing - or
/// re-index afterwards, which self-heals because the same event ids are simply
/// re-recorded against the new shard.
///
/// The original is left in place; delete or move it once you are satisfied,
/// otherwise both copies get indexed (harmless, but wasted work).
/// Returns the path written.
pub fn convert_archive_to_zst(path: &Path, frame_target: u64) -> Result<PathBuf> {
    let stem = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("{}: no file name", path.display()))?;
    // Strip the compression extension, keeping any `.jsonl`.
    let base = stem
        .strip_suffix(".gz")
        .or_else(|| stem.strip_suffix(".bz2"))
        .or_else(|| stem.strip_suffix(".zst"))
        .or_else(|| stem.strip_suffix(".zstd"))
        .unwrap_or(stem);
    let base = if base.ends_with(".jsonl") || base.ends_with(".json") {
        base.to_string()
    } else {
        format!("{base}.jsonl")
    };
    let out_path = path.with_file_name(format!("{base}.zst"));
    if out_path == path {
        // Already a .zst: just fix its framing in place.
        reframe_archive(path, frame_target)?;
        return Ok(out_path);
    }
    if out_path.exists() {
        bail!("{} already exists", out_path.display());
    }

    let mut src: Box<dyn Read> = match path.extension().and_then(|e| e.to_str()) {
        #[cfg(feature = "sync")]
        Some("gz") => Box::new(flate2::read::GzDecoder::new(File::open(path)?)),
        #[cfg(feature = "sync")]
        Some("bz2") => Box::new(bzip2::read::BzDecoder::new(File::open(path)?)),
        #[cfg(not(feature = "sync"))]
        Some(ext @ ("gz" | "bz2")) => {
            bail!("converting {ext} archives needs the `sync` feature")
        }
        Some("zst") | Some("zstd") => Box::new(zstd::stream::Decoder::new(File::open(path)?)?),
        _ => Box::new(File::open(path)?),
    };

    let mut tmp = out_path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(sidecar_path(&tmp));

    write_framed(&mut src, &tmp, frame_target)?;

    std::fs::rename(sidecar_path(&tmp), sidecar_path(&out_path))?;
    std::fs::rename(&tmp, &out_path)?;
    Ok(out_path)
}

/// Stream `src` into `dst` as concatenated zstd frames of at most
/// `frame_target` uncompressed bytes each, recording every boundary in
/// `dst`'s sidecar. Shared by conversion and reframing.
fn write_framed(src: &mut dyn Read, dst: &Path, frame_target: u64) -> Result<usize> {
    let frame_target = frame_target.max(1);
    let level = 3;
    let mut out = Some(File::create(dst)?);
    let mut encoder: Option<Encoder<'static, File>> = None;
    let mut log = FrameLog::open(dst)?;

    let mut buf = vec![0u8; 256 * 1024];
    let mut uncompressed = 0u64;
    let mut frames = 0usize;
    let mut frame_bytes = 0u64;

    loop {
        let read = match src.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                // Truncated or still-open final frame: keep what decoded.
                warn!("{}: stopping at {uncompressed} bytes: {e}", dst.display());
                break;
            }
        };

        let mut written = 0usize;
        while written < read {
            if let Some(mut file) = out.take() {
                let compressed = file.seek(SeekFrom::End(0))?;
                log.append(FrameStart {
                    uncompressed,
                    compressed,
                })?;
                frames += 1;
                frame_bytes = 0;
                encoder = Some(Encoder::new(file, level)?);
            }
            let room = (frame_target - frame_bytes) as usize;
            let take = room.min(read - written);
            let enc = encoder.as_mut().expect("encoder present");
            enc.write_all(&buf[written..written + take])?;
            written += take;
            frame_bytes += take as u64;
            uncompressed += take as u64;
            if frame_bytes >= frame_target {
                out = Some(encoder.take().expect("encoder present").finish()?);
            }
        }
    }
    if let Some(enc) = encoder.take() {
        out = Some(enc.finish()?);
    }
    if let Some(mut file) = out.take() {
        file.flush()?;
    }
    drop(log);
    Ok(frames)
}

/// Rewrite a zstd archive into bounded frames, so lookups into it can seek.
///
/// **The decompressed bytes are unchanged**, so any offsets already stored in
/// the index stay valid - only the compressed framing changes. This is the fix
/// for an archive imported from elsewhere as one giant frame, where every
/// lookup would otherwise decode from the start of the file.
///
/// The new archive is written beside the original and renamed into place with
/// its sidecar, so a crash leaves the original intact. Returns the frame count.
///
/// Only `.zst` archives are handled: converting `.gz`/`.bz2` would change the
/// file name, and the shard id is derived from that name.
pub fn reframe_archive(path: &Path, frame_target: u64) -> Result<usize> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".reframe.tmp");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(sidecar_path(&tmp));

    let mut src = zstd::stream::Decoder::new(File::open(path)?)?;
    let frames = write_framed(&mut src, &tmp, frame_target)?;

    std::fs::rename(sidecar_path(&tmp), sidecar_path(path))?;
    std::fs::rename(&tmp, path)?;
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::frames::FrameTable;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Ev {
        id: String,
        n: u64,
    }

    fn ev(n: u64) -> Ev {
        Ev {
            id: format!("{n:064x}"),
            n,
        }
    }

    /// Decode the whole archive and return the raw decompressed bytes.
    fn decode_all(path: &Path) -> Vec<u8> {
        let f = File::open(path).unwrap();
        let mut out = Vec::new();
        zstd::stream::Decoder::new(f)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn offsets_point_at_the_right_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        let mut locs = Vec::new();
        for n in 0..500u64 {
            locs.push(w.write_event(&ev(n)).unwrap());
        }
        w.finish().unwrap();

        let all = decode_all(&path);
        for (n, loc) in locs.iter().enumerate() {
            let line = &all[loc.offset as usize..loc.offset as usize + loc.len as usize];
            let v: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), n as u64);
        }
    }

    #[test]
    fn small_frame_target_produces_many_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..500u64 {
            w.write_event(&ev(n)).unwrap();
        }
        let total = w.uncompressed_len();
        w.finish().unwrap();

        let t = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert!(t.len() > 5, "expected multiple frames, got {}", t.len());
        let starts = t.starts();
        // Every recorded boundary must be a real zstd frame start.
        let mut data = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut data).unwrap();
        let real = crate::database::frames::scan_zstd_frame_offsets(&data).unwrap();
        for s in &starts {
            assert!(
                real.contains(&s.compressed),
                "boundary {} is not a zstd frame start",
                s.compressed
            );
        }
        assert_eq!(decode_all(&path).len() as u64, total);
    }

    #[test]
    fn reopening_continues_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..100u64 {
            w.write_event(&ev(n)).unwrap();
        }
        let len_before = w.uncompressed_len();
        w.finish().unwrap();

        // Reopen (process restart, same day) and keep appending.
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        assert_eq!(
            w.uncompressed_len(),
            len_before,
            "resume must continue the logical stream"
        );
        let mut locs = Vec::new();
        for n in 100..200u64 {
            locs.push(w.write_event(&ev(n)).unwrap());
        }
        w.finish().unwrap();

        let all = decode_all(&path);
        for (i, loc) in locs.iter().enumerate() {
            let line = &all[loc.offset as usize..loc.offset as usize + loc.len as usize];
            let v: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), 100 + i as u64);
        }
    }

    #[test]
    fn resume_without_sidecar_recovers_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..100u64 {
            w.write_event(&ev(n)).unwrap();
        }
        let len_before = w.uncompressed_len();
        w.finish().unwrap();
        std::fs::remove_file(sidecar_path(&path)).unwrap();

        let w = CompressedJsonLFile::new(&path).unwrap();
        assert_eq!(w.uncompressed_len(), len_before);
    }

    #[test]
    fn reframe_preserves_bytes_and_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import.jsonl.zst");

        // An archive imported as one giant frame (what `zstd file` produces).
        let mut raw = Vec::new();
        let mut locs = Vec::new();
        for n in 0..2000u64 {
            let line = serde_json::to_vec(&ev(n)).unwrap();
            locs.push((raw.len() as u64, line.len() as u32));
            raw.extend_from_slice(&line);
            raw.push(b'\n');
        }
        std::fs::write(&path, zstd::encode_all(raw.as_slice(), 3).unwrap()).unwrap();
        assert_eq!(
            crate::database::frames::scan_zstd_frame_offsets(&std::fs::read(&path).unwrap())
                .unwrap()
                .len(),
            1,
            "fixture should be a single frame"
        );

        let frames = reframe_archive(&path, 4096).unwrap();
        assert!(frames > 5, "expected many frames, got {frames}");

        // Decompressed bytes identical => previously indexed offsets still valid.
        assert_eq!(decode_all(&path), raw, "reframing must not change content");

        let table = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert_eq!(table.len(), frames);

        let pool = crate::ShardReaderPool::new();
        for (n, (offset, len)) in locs.iter().enumerate() {
            let got = pool.read_zstd_range(&path, *offset, *len).unwrap();
            let v: serde_json::Value = serde_json::from_slice(&got).unwrap();
            assert_eq!(v["n"].as_u64().unwrap(), n as u64);
        }
    }

    #[test]
    fn rebuild_sidecar_matches_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.zst");
        let mut w = CompressedJsonLFile::with_frame_target(&path, 1024).unwrap();
        for n in 0..300u64 {
            w.write_event(&ev(n)).unwrap();
        }
        w.finish().unwrap();
        let original = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();

        std::fs::remove_file(sidecar_path(&path)).unwrap();
        let n = rebuild_frame_index(&path).unwrap();
        let rebuilt = FrameTable::load(&sidecar_path(&path)).unwrap().unwrap();
        assert_eq!(n, rebuilt.len());
        assert_eq!(original.starts(), rebuilt.starts());
    }
}
