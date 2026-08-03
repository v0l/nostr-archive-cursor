//! Versioned encoding for primary index values.
//!
//! The primary index maps `event id [32] -> value`. The value always starts
//! with `created_at` (8 bytes, little endian) so every version can serve
//! timestamp queries, followed by an explicit version byte:
//!
//! ```text
//! offset  size  field
//! 0       8     created_at        u64 le      (always present, all versions)
//! 8       1     version           u8          (absent => v0)
//! ---- v1 payload ----
//! 9       8     shard_hash        u64 le      (hash of the relative file name)
//! 17      6     offset_bytes      u48 le      (offset in the *decompressed* shard)
//! 23      4     length_bytes      u32 le      (JSON line length, excl. newline)
//!                                             total: 27 bytes
//! ```
//!
//! Rules:
//! * `len == 8` is **v0** — every database written before event lookup existed.
//!   It carries no location, so `event_by_id` falls back to scanning.
//! * An unknown (newer) version byte degrades to "timestamp only" instead of
//!   being misparsed, so an old binary can read a newer database.
//! * Never reuse a version number.

use anyhow::{Result, bail};
use log::warn;

/// Version byte for the `shard_hash + offset + len` layout.
pub const VALUE_V1: u8 = 1;

/// Encoded size of a v0 (timestamp only) value.
pub const V0_LEN: usize = 8;
/// Encoded size of a v1 value.
pub const V1_LEN: usize = 27;

/// Largest offset representable in the 6-byte `offset_bytes` field (256 TiB).
pub const MAX_OFFSET: u64 = (1 << 48) - 1;

/// Stable 64-bit id for a shard, derived purely from its file name.
///
/// Deriving the id from the name (instead of allocating one from a registry)
/// means any process that merely *sees* a file - including archives dropped
/// into the directory by an external relay backup - computes the same id
/// without coordinating with the index. A wiped-and-rebuilt index produces
/// identical ids too.
///
/// `rel_name` is the path relative to the archive directory, using `/`
/// separators. The hash is FNV-1a/64: dependency-free and perfectly adequate
/// for the few thousand distinct names an archive holds (collisions are
/// detected when resolving the id back to a name, and a wrong shard is caught
/// by the `event.id == id` check after decoding).
pub fn shard_hash(rel_name: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET_BASIS;
    for b in rel_name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// Where an event's JSON line lives inside a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventLoc {
    /// [`shard_hash`] of the shard file name.
    pub shard: u64,
    /// Byte offset of the first `{` in the *decompressed* shard stream.
    pub offset: u64,
    /// Length of the JSON line in bytes, excluding the trailing newline.
    pub len: u32,
}

/// A decoded primary index value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub created_at: u64,
    /// `None` for v0 entries (and unknown future versions): the event exists,
    /// but we do not know where it is.
    pub loc: Option<EventLoc>,
}

impl IndexEntry {
    /// Timestamp-only entry (encodes as v0).
    pub fn new(created_at: u64) -> Self {
        Self {
            created_at,
            loc: None,
        }
    }

    /// Located entry (encodes as v1).
    pub fn located(created_at: u64, loc: EventLoc) -> Self {
        Self {
            created_at,
            loc: Some(loc),
        }
    }

    /// Encode into a stack buffer - no heap allocation on the insert path.
    pub fn encode(&self) -> IndexValue {
        let mut buf = [0u8; V1_LEN];
        buf[..8].copy_from_slice(&self.created_at.to_le_bytes());
        let len = match self.loc {
            None => V0_LEN,
            Some(loc) => {
                debug_assert!(loc.offset <= MAX_OFFSET, "offset exceeds u48");
                buf[8] = VALUE_V1;
                buf[9..17].copy_from_slice(&loc.shard.to_le_bytes());
                buf[17..23].copy_from_slice(&loc.offset.to_le_bytes()[..6]);
                buf[23..27].copy_from_slice(&loc.len.to_le_bytes());
                V1_LEN
            }
        };
        IndexValue { buf, len }
    }

    /// Decode a stored value, dispatching on the version byte.
    pub fn decode(v: &[u8]) -> Result<Self> {
        if v.len() < V0_LEN {
            bail!("index value too short: {} bytes", v.len());
        }
        let created_at = u64::from_le_bytes(v[..8].try_into().unwrap());
        match v.get(8).copied() {
            // v0: legacy timestamp-only entry.
            None => Ok(Self::new(created_at)),
            Some(VALUE_V1) if v.len() >= V1_LEN => {
                let shard = u64::from_le_bytes(v[9..17].try_into().unwrap());
                let mut off = [0u8; 8];
                off[..6].copy_from_slice(&v[17..23]);
                let offset = u64::from_le_bytes(off);
                let len = u32::from_le_bytes(v[23..27].try_into().unwrap());
                Ok(Self::located(created_at, EventLoc { shard, offset, len }))
            }
            Some(VALUE_V1) => bail!("truncated v1 index value: {} bytes", v.len()),
            // Written by a newer binary: we can still serve created_at,
            // check_id, count and the time index.
            Some(other) => {
                warn!("unknown index value version {other}, ignoring location");
                Ok(Self::new(created_at))
            }
        }
    }

    /// Decode just the timestamp. Used on hot paths (time index backfill)
    /// where the location is irrelevant.
    pub fn decode_created_at(v: &[u8]) -> Option<u64> {
        if v.len() < V0_LEN {
            return None;
        }
        Some(u64::from_le_bytes(v[..8].try_into().ok()?))
    }
}

/// Encoded [`IndexEntry`], 8 or 27 bytes, stored inline.
#[derive(Debug, Clone, Copy)]
pub struct IndexValue {
    buf: [u8; V1_LEN],
    len: usize,
}

impl IndexValue {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl AsRef<[u8]> for IndexValue {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<IndexEntry> for IndexValue {
    fn from(e: IndexEntry) -> Self {
        e.encode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v0_roundtrip() {
        let e = IndexEntry::new(1_700_000_000);
        let v = e.encode();
        assert_eq!(v.as_slice().len(), V0_LEN, "timestamp-only stays 8 bytes");
        assert_eq!(IndexEntry::decode(v.as_slice()).unwrap(), e);
    }

    #[test]
    fn v1_roundtrip() {
        let e = IndexEntry::located(
            1_700_000_000,
            EventLoc {
                shard: shard_hash("events_20250801.jsonl.zst"),
                offset: MAX_OFFSET,
                len: 4242,
            },
        );
        let v = e.encode();
        assert_eq!(v.as_slice().len(), V1_LEN);
        assert_eq!(IndexEntry::decode(v.as_slice()).unwrap(), e);
    }

    #[test]
    fn legacy_8_byte_value_is_v0() {
        // Exactly what every pre-existing database contains.
        let raw = 1_234u64.to_le_bytes();
        let got = IndexEntry::decode(&raw).unwrap();
        assert_eq!(got.created_at, 1_234);
        assert!(got.loc.is_none(), "v0 carries no location");
    }

    #[test]
    fn unknown_version_degrades_to_timestamp_only() {
        let mut raw = [0u8; V1_LEN];
        raw[..8].copy_from_slice(&99u64.to_le_bytes());
        raw[8] = 200; // some future version
        let got = IndexEntry::decode(&raw).unwrap();
        assert_eq!(got.created_at, 99);
        assert!(got.loc.is_none(), "must not misparse a newer layout");
    }

    #[test]
    fn truncated_v1_is_an_error() {
        let mut raw = vec![0u8; 20];
        raw[8] = VALUE_V1;
        assert!(
            IndexEntry::decode(&raw).is_err(),
            "a short v1 value must error, not silently misread"
        );
        assert!(IndexEntry::decode(&[0u8; 3]).is_err());
    }

    #[test]
    fn shard_hash_is_stable() {
        // Golden values: this hash is persisted in the index, so it must never
        // change between versions/processes.
        assert_eq!(shard_hash(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(shard_hash("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(
            shard_hash("events_20250801.jsonl.zst"),
            shard_hash("events_20250801.jsonl.zst")
        );
        assert_ne!(
            shard_hash("events_20250801.jsonl.zst"),
            shard_hash("events_20250802.jsonl.zst")
        );
    }
}
