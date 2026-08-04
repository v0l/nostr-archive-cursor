//! Frame index sidecar (`<shard>.frames`).
//!
//! Offsets stored in the event index are offsets into the *decompressed*
//! stream. To avoid decoding a whole shard for one lookup, the writer emits
//! bounded zstd frames and records every frame boundary here:
//!
//! ```text
//! header: b"NAFR" || u32_le version(1)
//! record: u64_le uncompressed_start || u64_le compressed_start   (16 bytes)
//! ```
//!
//! A record is appended when a boundary is *created*, so the last record is
//! the start of the frame currently being written - which is exactly the state
//! a writer needs to resume appending, and lets a reader compute each frame's
//! extent from the following record.
//!
//! Fixed-size records mean the table is binary-searchable. Concatenated zstd
//! frames are still a valid `.zst` file, so `zstd -d` and all existing reader
//! paths keep working whether or not a sidecar exists.

use anyhow::{Result, bail};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const FRAME_MAGIC: &[u8; 4] = b"NAFR";
pub const FRAME_VERSION: u32 = 1;
pub const FRAME_HEADER_LEN: u64 = 8;
pub const FRAME_RECORD_LEN: u64 = 16;

/// Sidecar path for a shard (`events_x.jsonl.zst` -> `events_x.jsonl.zst.frames`).
pub fn sidecar_path(shard: &Path) -> PathBuf {
    let mut s = shard.as_os_str().to_os_string();
    s.push(".frames");
    PathBuf::from(s)
}

/// One frame boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameStart {
    /// Offset of the first byte of this frame in the decompressed stream.
    pub uncompressed: u64,
    /// Offset of the first byte of this frame in the compressed file.
    pub compressed: u64,
}

/// Byte range to read + decode in order to reach a given offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameSpan {
    pub uncompressed_start: u64,
    pub compressed_start: u64,
    /// End of the compressed frame, or `None` when it is the last (open) frame
    /// and therefore runs to EOF.
    pub compressed_end: Option<u64>,
}

/// Loaded sidecar.
///
/// Records are kept as raw bytes and binary-searched in place: at the default
/// 64 KiB frame target a 5 GB shard has ~80k records (1.3 MB), and parsing
/// that into a `Vec<FrameStart>` on first touch would cost more than the
/// lookups it serves. Reading the bytes is one sequential read; searching them
/// is `log2(n)` slice reads with no allocation.
#[derive(Debug, Default, Clone)]
pub struct FrameTable {
    /// `n * 16` bytes: `u64_le uncompressed || u64_le compressed`.
    records: Vec<u8>,
}

impl FrameTable {
    pub fn from_starts(starts: Vec<FrameStart>) -> Self {
        let mut records = Vec::with_capacity(starts.len() * 16);
        for s in starts {
            records.extend_from_slice(&s.uncompressed.to_le_bytes());
            records.extend_from_slice(&s.compressed.to_le_bytes());
        }
        Self { records }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn len(&self) -> usize {
        self.records.len() / FRAME_RECORD_LEN as usize
    }

    /// Boundary at index `i`.
    pub fn get(&self, i: usize) -> Option<FrameStart> {
        let r = self.records.get(i * 16..i * 16 + 16)?;
        Some(FrameStart {
            uncompressed: u64::from_le_bytes(r[..8].try_into().unwrap()),
            compressed: u64::from_le_bytes(r[8..].try_into().unwrap()),
        })
    }

    /// All boundaries, materialised. For tests and tooling - the read path
    /// uses [`get`](Self::get)/[`span_for`](Self::span_for) instead.
    pub fn starts(&self) -> Vec<FrameStart> {
        (0..self.len()).filter_map(|i| self.get(i)).collect()
    }

    /// Largest gap between consecutive frame starts, i.e. the biggest frame we
    /// know the extent of. `None` when the table has fewer than two records,
    /// which means the archive is one (unbounded) frame as far as we can tell.
    ///
    /// Used to decide whether an imported archive needs reframing: a single
    /// giant frame makes every lookup decode from the start of the file.
    pub fn max_frame_span(&self) -> Option<u64> {
        let n = self.len();
        if n < 2 {
            return None;
        }
        let mut max = 0;
        let mut prev = self.get(0)?.uncompressed;
        for i in 1..n {
            let cur = self.get(i)?.uncompressed;
            max = max.max(cur.saturating_sub(prev));
            prev = cur;
        }
        Some(max)
    }

    /// The last boundary, i.e. the start of the frame currently being appended.
    pub fn last(&self) -> Option<FrameStart> {
        self.len().checked_sub(1).and_then(|i| self.get(i))
    }

    /// Load a sidecar. Returns `Ok(None)` when it does not exist (legacy or
    /// externally-dropped shard) - callers then decode from offset 0.
    pub fn load(path: &Path) -> Result<Option<Self>> {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut header = [0u8; FRAME_HEADER_LEN as usize];
        if let Err(e) = f.read_exact(&mut header) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None); // empty/truncated sidecar: treat as absent
            }
            return Err(e.into());
        }
        if &header[..4] != FRAME_MAGIC {
            bail!("{}: bad frame index magic", path.display());
        }
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if version != FRAME_VERSION {
            bail!(
                "{}: unsupported frame index version {version}",
                path.display()
            );
        }

        let mut records = Vec::new();
        f.read_to_end(&mut records)?;
        // A crash can leave a partial record; ignore the tail.
        records.truncate(records.len() / 16 * 16);
        let table = Self { records };
        // Boundaries must advance; anything else means a corrupt sidecar and we
        // would rather rebuild than seek to garbage.
        // (Cheap: one pass over ~16 bytes per frame, done once per shard.)
        //
        // `compressed` strictly increases -- each frame starts after the last.
        // `uncompressed` only has to be non-decreasing, because an *empty*
        // frame contributes no decompressed bytes and so shares its
        // predecessor's offset. Demanding that both increase rejected the
        // legitimate sidecar of a shard holding empty frames, and since
        // rebuilding regenerates exactly the same table, such a shard could
        // never be opened again -- the writer dropped every event for it.
        let mut prev: Option<FrameStart> = None;
        for i in 0..table.len() {
            let cur = table.get(i).unwrap();
            if let Some(p) = prev
                && (cur.compressed <= p.compressed || cur.uncompressed < p.uncompressed)
            {
                bail!("{}: non-monotonic frame index", path.display());
            }
            prev = Some(cur);
        }
        Ok(Some(table))
    }

    /// Load the frame sidecar for `shard`, rebuilding it when it is corrupt.
    ///
    /// The sidecar is pure derived data: every boundary in it is recomputable
    /// from the `.zst` it describes. So a sidecar that fails to load (bad
    /// magic, unsupported version, or non-monotonic boundaries -- the usual
    /// residue of a crash mid-append) is not an error we should propagate up
    /// to a panic; it is a signal to delete it and regenerate it from the
    /// frames. Returns `Ok(None)` exactly when there is no sidecar and none
    /// could be produced (e.g. an empty shard), matching `[`FrameTable::load`]`.
    ///
    /// `shard` is the `.zst`/`.jsonl` path (not the sidecar path); the sidecar
    /// is derived from it.
    pub fn load_or_rebuild(shard: &Path) -> Result<Option<Self>> {
        let sidecar = sidecar_path(shard);
        match Self::load(&sidecar) {
            Ok(v) => return Ok(v),
            Err(e) => {
                log::warn!(
                    "{}: corrupt frame sidecar ({e}); rebuilding it from the shard",
                    sidecar.display()
                );
            }
        }
        // Drop the bad sidecar, then regenerate it from the shard's frames.
        // `rebuild_frame_index` writes via a temp path + rename, so a crash
        // here cannot leave a sidecar that disagrees with the shard.
        let _ = std::fs::remove_file(&sidecar);
        match crate::database::file::rebuild_frame_index(shard) {
            Ok(n) if n > 0 => {}
            // Empty shard or a rebuild that found nothing clean: treat as
            // no sidecar, which callers already handle (decode from offset 0).
            _ => return Ok(None),
        }
        Self::load(&sidecar)
    }

    /// Compressed range that must be decoded to read `offset..end`.
    ///
    /// Frames are not guaranteed to break between events: the writer cuts at
    /// event boundaries, but a reframed or externally-produced archive can
    /// split an event across frames. So the span runs to the end of the last
    /// frame the range touches, not just the first one. zstd decodes
    /// concatenated frames transparently.
    pub fn span_for_range(&self, offset: u64, end: u64) -> FrameSpan {
        let start = self.span_for(offset);
        let n = self.len();
        if n == 0 {
            return start;
        }
        // First boundary at or past `end`: everything before it is needed.
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.get(mid).unwrap().uncompressed < end {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        FrameSpan {
            compressed_end: self.get(lo).map(|s| s.compressed),
            ..start
        }
    }

    /// Find the frame containing `offset` (an offset in the decompressed
    /// stream). Falls back to "one frame starting at 0" when the table is
    /// empty, which is exactly the legacy single-frame layout.
    pub fn span_for(&self, offset: u64) -> FrameSpan {
        let n = self.len();
        if n == 0 {
            return FrameSpan {
                uncompressed_start: 0,
                compressed_start: 0,
                compressed_end: None,
            };
        }
        // Greatest boundary with `uncompressed <= offset`.
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.get(mid).unwrap().uncompressed <= offset {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // `lo` is the first boundary past `offset`; an offset before the very
        // first boundary decodes from the start.
        let idx = lo.saturating_sub(1);
        let cur = self.get(idx).unwrap();
        FrameSpan {
            uncompressed_start: cur.uncompressed,
            compressed_start: cur.compressed,
            compressed_end: self.get(idx + 1).map(|s| s.compressed),
        }
    }
}

/// Appender for the sidecar, owned by the archive writer.
pub struct FrameLog {
    file: BufWriter<File>,
}

impl FrameLog {
    /// Open (creating if needed) the sidecar for `shard`.
    pub fn open(shard: &Path) -> Result<Self> {
        Self::open_at(&sidecar_path(shard))
    }

    /// Open (creating if needed) a sidecar at an exact path.
    pub fn open_at(path: &Path) -> Result<Self> {
        let mut file = File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        if file.seek(SeekFrom::End(0))? == 0 {
            file.write_all(FRAME_MAGIC)?;
            file.write_all(&FRAME_VERSION.to_le_bytes())?;
        }
        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    /// Record a new frame boundary and flush it immediately: the sidecar must
    /// never claim fewer frames than the shard actually contains.
    pub fn append(&mut self, start: FrameStart) -> Result<()> {
        self.file.write_all(&start.uncompressed.to_le_bytes())?;
        self.file.write_all(&start.compressed.to_le_bytes())?;
        self.file.flush()?;
        Ok(())
    }
}

/// First four bytes of every zstd data frame.
pub const ZSTD_MAGIC_BYTES: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// A point where the frame walk could not continue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Damage {
    /// Start of the frame that failed to parse.
    pub frame_start: u64,
    /// Offset the parse gave up at.
    pub offset: u64,
    /// Why it gave up (`truncated block body`, `reserved zstd block type`, ...).
    pub reason: String,
    /// Offset the scan resynchronised at, or `None` when it ran out of file.
    pub resync: Option<u64>,
}

/// Result of walking a possibly damaged archive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanReport {
    /// Compressed offset of every frame that parsed cleanly, in file order.
    pub offsets: Vec<u64>,
    /// Number of leading offsets that form an unbroken run from byte 0. Frames
    /// past this point are still readable but no longer describe a contiguous
    /// decompressed stream, so their uncompressed offsets cannot be trusted.
    pub clean_prefix: usize,
    /// Faults, in file order. Empty means the archive is structurally intact.
    pub damage: Vec<Damage>,
}

impl ScanReport {
    pub fn is_clean(&self) -> bool {
        self.damage.is_empty()
    }
}

/// Every offset in the stream that looks like the start of a zstd frame.
///
/// Deliberately dumber than [`scan_zstd_frames`], and that is the point: when
/// two writers interleave, a frame's *headers* can parse cleanly while
/// describing byte ranges that belong to the other stream, which hides the
/// real frames living inside them. On one 156 MB shard the structural walk
/// found 1 frame; the byte search found the frame holding 433 MB of the
/// events. Only the decoder can settle which candidates are real, so hand it
/// all of them.
///
/// Coincidental 4-byte matches inside compressed data are expected; they
/// simply fail to decode.
pub fn scan_zstd_frame_starts<R: Read + Seek>(src: &mut R, len: u64) -> Result<Vec<u64>> {
    let mut starts = Vec::new();
    let mut from = 0u64;
    while let Some(at) = find_frame_magic(src, from, len)? {
        starts.push(at);
        from = at + 4;
    }
    Ok(starts)
}

/// Boundaries of every zstd frame in `data`, found by walking frame headers
/// and block headers - no decompression.
///
/// Used to regenerate a sidecar for a shard written before framing existed
/// (or imported from elsewhere). Returns the compressed offset of each frame
/// plus the total compressed length consumed.
///
/// Errors on the first structural fault; use [`scan_zstd_frames`] to walk a
/// damaged archive.
pub fn scan_zstd_frame_offsets(data: &[u8]) -> Result<Vec<u64>> {
    scan_zstd_frame_offsets_reader(&mut std::io::Cursor::new(data), data.len() as u64)
}

/// Frame boundaries, read from a stream instead of a buffer.
///
/// The scan only ever moves forward — it reads a header, then skips the bytes
/// that header describes — so it never needed the file in memory. Reading it
/// all in cost one byte of RAM per byte of archive, which on a corpus-sized
/// shard is the difference between a few MiB and the whole file.
///
/// Bounded memory: headers are read a few bytes at a time and block bodies are
/// seeked over, never read. `len` is the stream's total length, used for the
/// same truncation checks the buffered scan made against `data.len()`.
///
/// Strict: any fault fails the whole scan. [`scan_zstd_frames`] keeps what it
/// found and reports where the damage is instead.
pub fn scan_zstd_frame_offsets_reader<R: Read + Seek>(src: &mut R, len: u64) -> Result<Vec<u64>> {
    let report = scan_zstd_frames(src, len)?;
    if let Some(d) = report.damage.first() {
        bail!("{}", d.reason);
    }
    Ok(report.offsets)
}

/// Walk every frame in the archive, surviving damage.
///
/// A zstd frame cannot be resynchronised *internally* - one bad block header
/// and the rest of that frame is gone - but a file is a sequence of frames, so
/// the next intact frame is recoverable. On a fault the scan hunts forward for
/// the next frame magic and carries on, recording what it skipped.
///
/// This is why a truncated tail (the common crash case) no longer costs the
/// whole index: the frames before it are still described exactly.
pub fn scan_zstd_frames<R: Read + Seek>(src: &mut R, len: u64) -> Result<ScanReport> {
    let mut report = ScanReport::default();
    let mut pos = 0u64;
    let mut contiguous = true;

    while pos + 4 <= len {
        match frame_extent(src, pos, len)? {
            Extent::Frame(end) => {
                report.offsets.push(pos);
                if contiguous {
                    report.clean_prefix = report.offsets.len();
                }
                pos = end;
            }
            Extent::Skippable(end) => pos = end, // not a data frame, no boundary
            Extent::Fault { offset, reason } => {
                contiguous = false;
                let resync = find_frame_magic(src, pos + 4, len)?;
                report.damage.push(Damage {
                    frame_start: pos,
                    offset,
                    reason,
                    resync,
                });
                match resync {
                    Some(next) => pos = next,
                    None => break,
                }
            }
        }
    }
    Ok(report)
}

enum Extent {
    /// Data frame ending at this offset.
    Frame(u64),
    /// Skippable frame ending at this offset.
    Skippable(u64),
    Fault {
        offset: u64,
        reason: String,
    },
}

/// Parse the frame starting at `start`, returning where it ends.
///
/// Only I/O errors are `Err`; malformed data is [`Extent::Fault`] so the
/// caller can decide whether to give up or resynchronise.
fn frame_extent<R: Read + Seek>(src: &mut R, start: u64, len: u64) -> Result<Extent> {
    const ZSTD_MAGIC: u32 = 0xFD2F_B528;
    const SKIPPABLE_MASK: u32 = 0xFFFF_FFF0;
    const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;

    let mut pos = start;
    let mut buf = [0u8; 4];
    src.seek(SeekFrom::Start(pos))?;

    macro_rules! fault {
        ($what:expr) => {
            return Ok(Extent::Fault {
                offset: pos,
                reason: $what.to_string(),
            })
        };
    }
    // Read exactly `n` bytes at the cursor, advancing `pos`.
    macro_rules! take {
        ($n:expr, $what:literal) => {{
            if pos + $n as u64 > len {
                fault!($what);
            }
            src.read_exact(&mut buf[..$n])?;
            pos += $n as u64;
            &buf[..$n]
        }};
    }
    // Skip `n` bytes without reading them.
    macro_rules! skip {
        ($n:expr, $what:literal) => {{
            let n = $n as u64;
            if pos + n > len {
                fault!(format!("{} (short by {} bytes)", $what, pos + n - len));
            }
            src.seek(SeekFrom::Current(n as i64))?;
            pos += n;
        }};
    }

    let magic = u32::from_le_bytes(take!(4, "truncated frame magic").try_into().unwrap());
    if magic & SKIPPABLE_MASK == SKIPPABLE_MAGIC {
        let size = u32::from_le_bytes(
            take!(4, "truncated skippable frame header")
                .try_into()
                .unwrap(),
        );
        skip!(size, "truncated skippable frame body");
        return Ok(Extent::Skippable(pos));
    }
    if magic != ZSTD_MAGIC {
        pos = start;
        fault!(format!("not a zstd frame at offset {start}"));
    }

    // Frame header
    let fhd = take!(1, "truncated frame header descriptor")[0];
    let fcs_flag = fhd >> 6;
    let single_segment = (fhd >> 5) & 1 == 1;
    let checksum = (fhd >> 2) & 1 == 1;
    let did_flag = fhd & 3;
    let mut header_rest = usize::from(!single_segment); // window descriptor
    header_rest += match did_flag {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    header_rest += match fcs_flag {
        0 => usize::from(single_segment),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    skip!(header_rest, "truncated frame header");

    // Blocks
    loop {
        let hdr = take!(3, "truncated block header");
        let hdr = u32::from(hdr[0]) | (u32::from(hdr[1]) << 8) | (u32::from(hdr[2]) << 16);
        let last = hdr & 1 == 1;
        let block_type = (hdr >> 1) & 3;
        let block_size = hdr >> 3;
        if block_type == 3 {
            pos -= 3;
            fault!("reserved zstd block type");
        }
        // RLE blocks are a single byte on the wire.
        let body = if block_type == 1 { 1 } else { block_size };
        skip!(body, "truncated block body");
        if last {
            break;
        }
    }
    if checksum {
        skip!(4u32, "truncated frame checksum");
    }
    Ok(Extent::Frame(pos))
}

/// Next zstd frame magic at or after `from`, scanned in bounded windows.
///
/// A match can be a coincidence inside compressed data; the caller proves it
/// by parsing the frame there, and comes back for the next candidate if that
/// fails.
fn find_frame_magic<R: Read + Seek>(src: &mut R, from: u64, len: u64) -> Result<Option<u64>> {
    const WINDOW: usize = 256 * 1024;
    if from + 4 > len {
        return Ok(None);
    }
    let mut pos = from;
    let mut buf = vec![0u8; WINDOW];
    src.seek(SeekFrom::Start(pos))?;
    let mut filled = 0usize;
    loop {
        let want = ((len - pos) as usize).min(WINDOW - filled);
        if want == 0 {
            return Ok(None);
        }
        let mut read = 0usize;
        while read < want {
            match src.read(&mut buf[filled + read..filled + want]) {
                Ok(0) => break,
                Ok(n) => read += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }
        if read == 0 {
            return Ok(None);
        }
        let end = filled + read;
        if let Some(i) = buf[..end]
            .windows(4)
            .position(|w| w == ZSTD_MAGIC_BYTES.as_slice())
        {
            return Ok(Some(pos - filled as u64 + i as u64));
        }
        // Keep the last 3 bytes: a magic can straddle the window edge.
        let keep = 3.min(end);
        buf.copy_within(end - keep..end, 0);
        pos += read as u64;
        filled = keep;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Multi-frame zstd data plus the offsets it should yield.
    fn framed(frames: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..frames {
            let body = format!("frame {i} ").repeat(200);
            out.extend(zstd::encode_all(body.as_bytes(), 3).unwrap());
        }
        out
    }

    /// The streaming scan is the buffered scan: same boundaries, without
    /// holding the archive in memory.
    #[test]
    fn streaming_scan_matches_the_buffered_scan() {
        for n in [1usize, 2, 7] {
            let data = framed(n);
            let want = scan_zstd_frame_offsets(&data).unwrap();
            assert_eq!(want.len(), n, "expected {n} frames");

            let mut cursor = std::io::Cursor::new(&data);
            let got = scan_zstd_frame_offsets_reader(&mut cursor, data.len() as u64).unwrap();
            assert_eq!(got, want);

            // Every reported boundary is a real frame start.
            for off in &got {
                assert_eq!(&data[*off as usize..*off as usize + 4], b"\x28\xb5\x2f\xfd");
            }
        }
    }

    /// Truncation must still be an error rather than a short frame list.
    #[test]
    fn streaming_scan_rejects_a_truncated_frame() {
        let data = framed(2);
        let cut = data.len() - 8;
        let mut cursor = std::io::Cursor::new(&data[..cut]);
        assert!(scan_zstd_frame_offsets_reader(&mut cursor, cut as u64).is_err());
    }

    #[test]
    fn sidecar_path_appends_suffix() {
        assert_eq!(
            sidecar_path(Path::new("/a/events_20250801.jsonl.zst")),
            PathBuf::from("/a/events_20250801.jsonl.zst.frames")
        );
    }

    #[test]
    fn missing_sidecar_is_none_and_spans_from_zero() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            FrameTable::load(&dir.path().join("nope.frames"))
                .unwrap()
                .is_none()
        );
        let empty = FrameTable::default();
        assert_eq!(
            empty.span_for(12_345),
            FrameSpan {
                uncompressed_start: 0,
                compressed_start: 0,
                compressed_end: None
            }
        );
    }

    #[test]
    fn append_load_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("events_20250801.jsonl.zst");
        let mut log = FrameLog::open(&shard).unwrap();
        log.append(FrameStart {
            uncompressed: 0,
            compressed: 0,
        })
        .unwrap();
        log.append(FrameStart {
            uncompressed: 1000,
            compressed: 400,
        })
        .unwrap();
        log.append(FrameStart {
            uncompressed: 2000,
            compressed: 900,
        })
        .unwrap();
        drop(log);

        let t = FrameTable::load(&sidecar_path(&shard)).unwrap().unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(
            t.last(),
            Some(FrameStart {
                uncompressed: 2000,
                compressed: 900
            })
        );
        assert_eq!(
            t.span_for(0),
            FrameSpan {
                uncompressed_start: 0,
                compressed_start: 0,
                compressed_end: Some(400)
            }
        );
        assert_eq!(
            t.span_for(1500),
            FrameSpan {
                uncompressed_start: 1000,
                compressed_start: 400,
                compressed_end: Some(900)
            }
        );
        // exact boundary hit
        assert_eq!(t.span_for(2000).compressed_start, 900);
        // inside the open tail frame
        assert_eq!(t.span_for(9_999).compressed_end, None);

        // A range crossing a boundary must cover every frame it touches.
        let crossing = t.span_for_range(900, 1100);
        assert_eq!(crossing.compressed_start, 0);
        assert_eq!(
            crossing.compressed_end,
            Some(900),
            "must decode through the second frame"
        );
        // A range spanning all frames runs to EOF.
        assert_eq!(t.span_for_range(0, 9_999).compressed_end, None);
        // A range inside one frame stops at that frame.
        assert_eq!(t.span_for_range(1100, 1200).compressed_end, Some(900));
    }

    #[test]
    fn partial_trailing_record_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("s.zst");
        let mut log = FrameLog::open(&shard).unwrap();
        log.append(FrameStart {
            uncompressed: 0,
            compressed: 0,
        })
        .unwrap();
        drop(log);
        // Simulate a crash mid-record.
        let mut f = File::options()
            .append(true)
            .open(sidecar_path(&shard))
            .unwrap();
        f.write_all(&[1, 2, 3]).unwrap();
        drop(f);
        let t = FrameTable::load(&sidecar_path(&shard)).unwrap().unwrap();
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn scan_finds_concatenated_frames() {
        // Three independently compressed frames, concatenated - the layout the
        // writer produces.
        let mut data = Vec::new();
        let mut expected = Vec::new();
        for i in 0..3u8 {
            expected.push(data.len() as u64);
            let payload = vec![i; 5000];
            data.extend_from_slice(&zstd::encode_all(payload.as_slice(), 3).unwrap());
        }
        assert_eq!(scan_zstd_frame_offsets(&data).unwrap(), expected);
    }
}
