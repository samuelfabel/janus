//! Length-prefixed binary framing for the replica channel (not RESP, Cap'n Proto, or WAL).

use janus_kernel::replication::ReplicationRecord;

/// Wire kind for [`ReplicationRecord::Set`].
pub const KIND_SET: u8 = 1;
/// Wire kind for [`ReplicationRecord::Delete`].
pub const KIND_DELETE: u8 = 2;
/// Wire kind for [`ReplicationRecord::Expire`].
pub const KIND_EXPIRE: u8 = 3;
/// Wire kind for a domain snapshot full-sync payload (`JANUSNAP` bytes).
pub const KIND_SNAPSHOT: u8 = 4;

/// Maximum accepted key or value length (16 MiB).
pub const MAX_FIELD_LEN: usize = 16 * 1024 * 1024;

/// Maximum accepted frame payload length.
pub const MAX_FRAME_LEN: usize = 32 * 1024 * 1024 + 16;

/// One framed message on the replica wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Incremental mutation after (or instead of) a prior full sync.
    Record(ReplicationRecord),
    /// Domain snapshot bytes (`export_snapshot` / `JANUSNAP` codec).
    Snapshot(Vec<u8>),
}

/// Encode one frame into a length-prefixed blob (`u32` BE length + payload).
pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut payload = Vec::new();
    match frame {
        Frame::Record(record) => encode_record_payload(&mut payload, record),
        Frame::Snapshot(bytes) => {
            payload.push(KIND_SNAPSHOT);
            write_bytes(&mut payload, bytes);
        }
    }
    wrap_payload(payload)
}

/// Encode one record into a length-prefixed frame (`u32` BE length + payload).
pub fn encode(record: &ReplicationRecord) -> Vec<u8> {
    encode_frame(&Frame::Record(record.clone()))
}

/// Encode a domain snapshot into a length-prefixed frame.
pub fn encode_snapshot(bytes: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::Snapshot(bytes.to_vec()))
}

fn wrap_payload(payload: Vec<u8>) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn encode_record_payload(payload: &mut Vec<u8>, record: &ReplicationRecord) {
    match record {
        ReplicationRecord::Set { key, value } => {
            payload.push(KIND_SET);
            write_bytes(payload, key);
            write_bytes(payload, value);
        }
        ReplicationRecord::Delete { key } => {
            payload.push(KIND_DELETE);
            write_bytes(payload, key);
        }
        ReplicationRecord::Expire { key, seconds } => {
            payload.push(KIND_EXPIRE);
            write_bytes(payload, key);
            payload.extend_from_slice(&seconds.to_be_bytes());
        }
    }
}

/// Decode one complete frame payload (without the leading length prefix).
pub fn decode_payload(payload: &[u8]) -> Result<Frame, DecodeError> {
    if payload.is_empty() {
        return Err(DecodeError::Empty);
    }
    let kind = payload[0];
    let mut offset = 1;
    match kind {
        KIND_SET => {
            let key = read_bytes(payload, &mut offset)?;
            let value = read_bytes(payload, &mut offset)?;
            if offset != payload.len() {
                return Err(DecodeError::TrailingBytes);
            }
            Ok(Frame::Record(ReplicationRecord::Set { key, value }))
        }
        KIND_DELETE => {
            let key = read_bytes(payload, &mut offset)?;
            if offset != payload.len() {
                return Err(DecodeError::TrailingBytes);
            }
            Ok(Frame::Record(ReplicationRecord::Delete { key }))
        }
        KIND_EXPIRE => {
            let key = read_bytes(payload, &mut offset)?;
            if payload.len() - offset < 8 {
                return Err(DecodeError::Truncated);
            }
            let mut secs = [0u8; 8];
            secs.copy_from_slice(&payload[offset..offset + 8]);
            offset += 8;
            if offset != payload.len() {
                return Err(DecodeError::TrailingBytes);
            }
            Ok(Frame::Record(ReplicationRecord::Expire {
                key,
                seconds: u64::from_be_bytes(secs),
            }))
        }
        KIND_SNAPSHOT => {
            let bytes = read_bytes(payload, &mut offset)?;
            if offset != payload.len() {
                return Err(DecodeError::TrailingBytes);
            }
            Ok(Frame::Snapshot(bytes))
        }
        _ => Err(DecodeError::UnknownKind(kind)),
    }
}

/// Why a frame could not be decoded (returned to callers; never panics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Payload was empty.
    Empty,
    /// Declared length exceeded [`MAX_FRAME_LEN`] or a field exceeded [`MAX_FIELD_LEN`].
    TooLarge,
    /// Not enough bytes for the declared fields.
    Truncated,
    /// Bytes remained after a complete record.
    TrailingBytes,
    /// Kind byte was not Set / Delete / Expire / Snapshot.
    UnknownKind(u8),
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn read_bytes(input: &[u8], offset: &mut usize) -> Result<Vec<u8>, DecodeError> {
    if input.len() - *offset < 4 {
        return Err(DecodeError::Truncated);
    }
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&input[*offset..*offset + 4]);
    *offset += 4;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FIELD_LEN {
        return Err(DecodeError::TooLarge);
    }
    if input.len() - *offset < len {
        return Err(DecodeError::Truncated);
    }
    let bytes = input[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_delete_expire_round_trip() {
        let records = [
            ReplicationRecord::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            ReplicationRecord::Delete {
                key: b"k".to_vec(),
            },
            ReplicationRecord::Expire {
                key: b"k".to_vec(),
                seconds: 60,
            },
        ];
        for record in &records {
            let frame = encode(record);
            let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
            assert_eq!(len, frame.len() - 4);
            let decoded = decode_payload(&frame[4..]).expect("decode");
            assert_eq!(decoded, Frame::Record(record.clone()));
        }
    }

    #[test]
    fn snapshot_round_trip() {
        let snap = b"JANUSNAP\x01\x00\x00\x00".to_vec();
        let frame = encode_snapshot(&snap);
        let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4);
        assert_eq!(
            decode_payload(&frame[4..]).expect("decode"),
            Frame::Snapshot(snap)
        );
    }

    #[test]
    fn invalid_kind_is_rejected_without_panic() {
        let payload = [0x7Fu8];
        assert_eq!(
            decode_payload(&payload),
            Err(DecodeError::UnknownKind(0x7F))
        );
    }

    #[test]
    fn truncated_payload_is_rejected() {
        assert_eq!(decode_payload(&[KIND_DELETE]), Err(DecodeError::Truncated));
        assert_eq!(decode_payload(&[]), Err(DecodeError::Empty));
        assert_eq!(
            decode_payload(&[KIND_SNAPSHOT]),
            Err(DecodeError::Truncated)
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut payload = Vec::new();
        payload.push(KIND_DELETE);
        write_bytes(&mut payload, b"k");
        payload.push(0);
        assert_eq!(decode_payload(&payload), Err(DecodeError::TrailingBytes));
    }
}
