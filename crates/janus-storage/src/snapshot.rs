//! Binary snapshot codec (`JANUSNAP`) for durable export/import of live entries.

use std::fmt;

/// One live key/value pair as stored in a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// `None` = no expiry; `Some(secs)` = remaining TTL in whole seconds (>= 1 when encoded).
    pub ttl_secs: Option<u64>,
}

/// Decode / validation failure for a snapshot blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    BadMagic,
    UnsupportedVersion(u32),
    Truncated,
    InvalidHasTtl(u8),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotError::BadMagic => write!(f, "bad snapshot magic"),
            SnapshotError::UnsupportedVersion(v) => write!(f, "unsupported snapshot version {v}"),
            SnapshotError::Truncated => write!(f, "truncated snapshot"),
            SnapshotError::InvalidHasTtl(b) => write!(f, "invalid has_ttl byte {b}"),
        }
    }
}

impl std::error::Error for SnapshotError {}

const MAGIC: &[u8; 8] = b"JANUSNAP";
const VERSION: u32 = 1;

/// Encode live entries into the versioned `JANUSNAP` binary format (little-endian).
///
/// Entries with `ttl_secs == Some(0)` are omitted (no useful remaining life).
pub fn encode(entries: &[SnapshotEntry]) -> Vec<u8> {
    let live: Vec<&SnapshotEntry> = entries
        .iter()
        .filter(|e| e.ttl_secs != Some(0))
        .collect();

    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(live.len() as u32).to_le_bytes());

    for e in live {
        let key_len = e.key.len() as u32;
        let val_len = e.value.len() as u32;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&e.key);
        out.extend_from_slice(&val_len.to_le_bytes());
        out.extend_from_slice(&e.value);
        match e.ttl_secs {
            None => {
                out.push(0);
                out.extend_from_slice(&0u64.to_le_bytes());
            }
            Some(secs) => {
                out.push(1);
                out.extend_from_slice(&secs.to_le_bytes());
            }
        }
    }
    out
}

/// Decode a `JANUSNAP` blob into entries.
pub fn decode(bytes: &[u8]) -> Result<Vec<SnapshotEntry>, SnapshotError> {
    let mut cursor = 0usize;

    let magic = read_exact(bytes, &mut cursor, 8)?;
    if magic != MAGIC {
        return Err(SnapshotError::BadMagic);
    }

    let version = u32::from_le_bytes(read_array(bytes, &mut cursor)?);
    if version != VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }

    let count = u32::from_le_bytes(read_array(bytes, &mut cursor)?) as usize;
    let mut entries = Vec::with_capacity(count);

    for _ in 0..count {
        let key_len = u32::from_le_bytes(read_array(bytes, &mut cursor)?) as usize;
        let key = read_exact(bytes, &mut cursor, key_len)?.to_vec();
        let val_len = u32::from_le_bytes(read_array(bytes, &mut cursor)?) as usize;
        let value = read_exact(bytes, &mut cursor, val_len)?.to_vec();
        let has_ttl = *read_exact(bytes, &mut cursor, 1)?.first().unwrap();
        let ttl_raw = u64::from_le_bytes(read_array(bytes, &mut cursor)?);
        let ttl_secs = match has_ttl {
            0 => None,
            1 => Some(ttl_raw),
            other => return Err(SnapshotError::InvalidHasTtl(other)),
        };
        // Spec: has_ttl==1 && ttl_secs==0 must not appear in a well-formed dump;
        // if present, keep as Some(0) and let import treat via expire_at(now).
        entries.push(SnapshotEntry {
            key,
            value,
            ttl_secs,
        });
    }

    if cursor != bytes.len() {
        // Trailing garbage: treat as truncated/invalid for strictness.
        return Err(SnapshotError::Truncated);
    }

    Ok(entries)
}

fn read_exact<'a>(bytes: &'a [u8], cursor: &mut usize, n: usize) -> Result<&'a [u8], SnapshotError> {
    let end = cursor.checked_add(n).ok_or(SnapshotError::Truncated)?;
    if end > bytes.len() {
        return Err(SnapshotError::Truncated);
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], SnapshotError> {
    let slice = read_exact(bytes, cursor, N)?;
    let mut arr = [0u8; N];
    arr.copy_from_slice(slice);
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let bytes = encode(&[]);
        assert_eq!(decode(&bytes).unwrap(), Vec::<SnapshotEntry>::new());
    }

    #[test]
    fn entry_without_ttl_roundtrip() {
        let entries = vec![SnapshotEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_secs: None,
        }];
        assert_eq!(decode(&encode(&entries)).unwrap(), entries);
    }

    #[test]
    fn entry_with_ttl_roundtrip() {
        let entries = vec![SnapshotEntry {
            key: b"\x00\xff".to_vec(),
            value: b"".to_vec(),
            ttl_secs: Some(42),
        }];
        assert_eq!(decode(&encode(&entries)).unwrap(), entries);
    }

    #[test]
    fn some_zero_ttl_omitted_from_encode() {
        let entries = vec![
            SnapshotEntry {
                key: b"keep".to_vec(),
                value: b"1".to_vec(),
                ttl_secs: None,
            },
            SnapshotEntry {
                key: b"drop".to_vec(),
                value: b"2".to_vec(),
                ttl_secs: Some(0),
            },
        ];
        let decoded = decode(&encode(&entries)).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].key, b"keep");
    }

    #[test]
    fn bad_magic_errors() {
        let mut bytes = encode(&[]);
        bytes[0] = b'X';
        assert_eq!(decode(&bytes), Err(SnapshotError::BadMagic));
    }

    #[test]
    fn bad_version_errors() {
        let mut bytes = encode(&[]);
        // version sits after 8-byte magic
        bytes[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(decode(&bytes), Err(SnapshotError::UnsupportedVersion(2)));
    }

    #[test]
    fn truncated_errors() {
        let bytes = encode(&[SnapshotEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_secs: None,
        }]);
        assert_eq!(decode(&bytes[..bytes.len() - 1]), Err(SnapshotError::Truncated));
        assert_eq!(decode(&[]), Err(SnapshotError::Truncated));
    }
}
