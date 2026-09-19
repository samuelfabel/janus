//! Append-only WAL codec (`JANUSWAL`) for durable mutation replay.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::storage::engine::StorageEngine;

const MAGIC: &[u8; 8] = b"JANUSWAL";
const VERSION: u32 = 1;

const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_EXPIRE: u8 = 3;

/// One mutation recorded in the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// Upsert value (clears TTL on apply, same as `StorageEngine::set`).
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Remove key if present.
    Delete { key: Vec<u8> },
    /// Absolute expiry deadline as Unix UTC seconds.
    Expire {
        key: Vec<u8>,
        deadline_unix_secs: u64,
    },
}

/// Failure while reading or applying a WAL.
#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    BadMagic,
    UnsupportedVersion(u32),
    UnknownOp(u8),
    Truncated,
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(err) => write!(f, "wal i/o: {err}"),
            WalError::BadMagic => write!(f, "bad wal magic"),
            WalError::UnsupportedVersion(v) => write!(f, "unsupported wal version {v}"),
            WalError::UnknownOp(op) => write!(f, "unknown wal op {op}"),
            WalError::Truncated => write!(f, "truncated wal"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(value: io::Error) -> Self {
        WalError::Io(value)
    }
}

/// Append-only WAL writer (fsync after every record).
pub struct WalWriter {
    file: File,
}

impl WalWriter {
    /// Creates a new WAL file with header (truncates if it already exists).
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = File::create(path)?;
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.sync_all()?;
        Ok(WalWriter { file })
    }

    /// Opens an existing WAL for append (validates header, seeks to end).
    pub fn open_append(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        validate_header(&mut file)?;
        file.seek(SeekFrom::End(0))?;
        Ok(WalWriter { file })
    }

    /// Encodes `record`, writes it, and `sync_all`s the file.
    pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        let bytes = encode_record(record);
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// Replays all records from `path` into `engine`.
///
/// Expire deadlines use Unix seconds: remaining = `deadline_unix - now_unix`,
/// mapped onto `engine.now()` (monotonic Instant). Past deadlines become
/// `expire_at(now)` so lazy purge removes the key on access.
pub fn replay(path: impl AsRef<Path>, engine: &mut impl StorageEngine) -> Result<(), WalError> {
    let mut file = File::open(path)?;
    validate_header(&mut file)?;

    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let mut cursor = 0usize;
    while cursor < buf.len() {
        let (record, next) = decode_record(&buf, cursor)?;
        apply_record(engine, &record);
        cursor = next;
    }
    Ok(())
}

fn validate_header(file: &mut File) -> Result<(), WalError> {
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(WalError::BadMagic);
    }
    let mut ver = [0u8; 4];
    file.read_exact(&mut ver)?;
    let version = u32::from_le_bytes(ver);
    if version != VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }
    Ok(())
}

fn encode_record(record: &WalRecord) -> Vec<u8> {
    let mut out = Vec::new();
    match record {
        WalRecord::Set { key, value } => {
            out.push(OP_SET);
            write_bytes(&mut out, key);
            write_bytes(&mut out, value);
        }
        WalRecord::Delete { key } => {
            out.push(OP_DELETE);
            write_bytes(&mut out, key);
        }
        WalRecord::Expire {
            key,
            deadline_unix_secs,
        } => {
            out.push(OP_EXPIRE);
            write_bytes(&mut out, key);
            out.extend_from_slice(&deadline_unix_secs.to_le_bytes());
        }
    }
    out
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn decode_record(buf: &[u8], cursor: usize) -> Result<(WalRecord, usize), WalError> {
    let op = *buf.get(cursor).ok_or(WalError::Truncated)?;
    let mut c = cursor + 1;
    match op {
        OP_SET => {
            let (key, c1) = read_bytes(buf, c)?;
            let (value, c2) = read_bytes(buf, c1)?;
            Ok((WalRecord::Set { key, value }, c2))
        }
        OP_DELETE => {
            let (key, c1) = read_bytes(buf, c)?;
            Ok((WalRecord::Delete { key }, c1))
        }
        OP_EXPIRE => {
            let (key, c1) = read_bytes(buf, c)?;
            c = c1;
            let deadline = u64::from_le_bytes(read_array(buf, &mut c)?);
            Ok((
                WalRecord::Expire {
                    key,
                    deadline_unix_secs: deadline,
                },
                c,
            ))
        }
        other => Err(WalError::UnknownOp(other)),
    }
}

fn read_bytes(buf: &[u8], cursor: usize) -> Result<(Vec<u8>, usize), WalError> {
    let mut c = cursor;
    let len = u32::from_le_bytes(read_array(buf, &mut c)?) as usize;
    let end = c.checked_add(len).ok_or(WalError::Truncated)?;
    if end > buf.len() {
        return Err(WalError::Truncated);
    }
    let bytes = buf[c..end].to_vec();
    Ok((bytes, end))
}

fn read_array<const N: usize>(buf: &[u8], cursor: &mut usize) -> Result<[u8; N], WalError> {
    let end = cursor.checked_add(N).ok_or(WalError::Truncated)?;
    if end > buf.len() {
        return Err(WalError::Truncated);
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&buf[*cursor..end]);
    *cursor = end;
    Ok(arr)
}

fn apply_record(engine: &mut impl StorageEngine, record: &WalRecord) {
    match record {
        WalRecord::Set { key, value } => engine.set(key, value),
        WalRecord::Delete { key } => {
            let _ = engine.delete(key);
        }
        WalRecord::Expire {
            key,
            deadline_unix_secs,
        } => {
            let deadline = unix_to_engine_deadline(engine, *deadline_unix_secs);
            let _ = engine.expire_at(key, deadline);
        }
    }
}

/// Map Unix-UTC deadline onto the engine's monotonic clock.
///
/// `remaining = deadline_unix.saturating_sub(now_unix)`;
/// past/equal → `engine.now()` (expires on next access).
fn unix_to_engine_deadline(engine: &impl StorageEngine, deadline_unix_secs: u64) -> Instant {
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let now_instant = engine.now();
    if deadline_unix_secs <= now_unix {
        now_instant
    } else {
        now_instant + Duration::from_secs(deadline_unix_secs - now_unix)
    }
}

/// Current Unix UTC seconds (helper for tests / callers building Expire records).
pub fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{engine::Ttl, memory::MemoryStorageEngine};
    use std::path::PathBuf;

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";

    fn temp_wal(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "janus-f401-{}-{}-{label}.wal",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn append_set_replay_get_hit() {
        let path = temp_wal("set");
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(&WalRecord::Set {
                key: KEY.to_vec(),
                value: VALUE.to_vec(),
            })
            .unwrap();
        }
        let mut engine = MemoryStorageEngine::new();
        replay(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), Some(VALUE));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_delete_after_set_replay_miss() {
        let path = temp_wal("del");
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(&WalRecord::Set {
                key: KEY.to_vec(),
                value: VALUE.to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Delete {
                key: KEY.to_vec(),
            })
            .unwrap();
        }
        let mut engine = MemoryStorageEngine::new();
        replay(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_expire_future_replay_ttl_remaining() {
        let path = temp_wal("expire-future");
        let deadline = unix_now_secs() + 30;
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(&WalRecord::Set {
                key: KEY.to_vec(),
                value: VALUE.to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Expire {
                key: KEY.to_vec(),
                deadline_unix_secs: deadline,
            })
            .unwrap();
        }
        let mut engine = MemoryStorageEngine::new();
        replay(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), Some(VALUE));
        match engine.ttl(KEY) {
            Ttl::Remaining(d) => assert!(d > Duration::ZERO && d <= Duration::from_secs(30)),
            other => panic!("unexpected {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_expire_past_replay_get_miss() {
        let path = temp_wal("expire-past");
        let deadline = unix_now_secs().saturating_sub(10);
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(&WalRecord::Set {
                key: KEY.to_vec(),
                value: VALUE.to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Expire {
                key: KEY.to_vec(),
                deadline_unix_secs: deadline,
            })
            .unwrap();
        }
        let mut engine = MemoryStorageEngine::new();
        replay(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bad_magic_errors() {
        let path = temp_wal("magic");
        std::fs::write(&path, b"XXXXXXXX\x01\x00\x00\x00").unwrap();
        let mut engine = MemoryStorageEngine::new();
        assert!(matches!(
            replay(&path, &mut engine),
            Err(WalError::BadMagic)
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bad_version_errors() {
        let path = temp_wal("ver");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&2u32.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let mut engine = MemoryStorageEngine::new();
        assert!(matches!(
            replay(&path, &mut engine),
            Err(WalError::UnsupportedVersion(2))
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn truncated_record_errors() {
        let path = temp_wal("trunc");
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(&WalRecord::Set {
                key: KEY.to_vec(),
                value: VALUE.to_vec(),
            })
            .unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.pop();
        std::fs::write(&path, &bytes).unwrap();
        let mut engine = MemoryStorageEngine::new();
        assert!(matches!(
            replay(&path, &mut engine),
            Err(WalError::Truncated)
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_append_continues_log() {
        let path = temp_wal("append");
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(&WalRecord::Set {
                key: KEY.to_vec(),
                value: VALUE.to_vec(),
            })
            .unwrap();
        }
        {
            let mut w = WalWriter::open_append(&path).unwrap();
            w.append(&WalRecord::Delete {
                key: KEY.to_vec(),
            })
            .unwrap();
        }
        let mut engine = MemoryStorageEngine::new();
        replay(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), None);
        let _ = std::fs::remove_file(&path);
    }
}
