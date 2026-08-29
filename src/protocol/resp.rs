//! RESP protocol instance: decode → kernel → encode → callback.
use crate::{
    kernel::kernel::Kernel,
    protocol::{Protocol, ProtocolError},
    serializer::{DecodeOutcome, Serializer},
    storage::engine::StorageEngine,
};

/// RESP protocol instance owning a kernel and serializer (v1: one per connection).
pub struct RespProtocol<E, S>
where
    E: StorageEngine + Send + 'static,
    S: Serializer,
{
    kernel: Kernel<E>,
    serializer: S,
}

impl<E, S> RespProtocol<E, S>
where
    E: StorageEngine + Send + 'static,
    S: Serializer,
{
    /// Create a new protocol instance.
    pub fn new(kernel: Kernel<E>, serializer: S) -> Self {
        RespProtocol { kernel, serializer }
    }
}

impl<E, S> Protocol for RespProtocol<E, S>
where
    E: StorageEngine + Send + 'static,
    S: Serializer,
{
    fn execute(
        &mut self,
        buffer: &[u8],
        mut callback: impl FnMut(&[u8]),
    ) -> Result<usize, ProtocolError> {
        // Normative loop (LAYERS §2 / STREAM-PROCESSING §4):
        // offset ← 0
        // loop
        //   match decode(buffer[offset..])
        //     Incomplete → return Ok(offset)
        //     Err(e)     → return Err(e)
        //     Ok(cmd, n) → kernel; encode; callback; offset += n
        let mut offset = 0usize;

        while offset < buffer.len() {
            match self.serializer.decode_one(&buffer[offset..]) {
                DecodeOutcome::Incomplete => return Ok(offset),
                DecodeOutcome::Ok { command, consumed } => {
                    let response = self.kernel.execute(&command);
                    let encoded = self.serializer.encode(&response);
                    callback(&encoded);
                    offset += consumed;
                }
                DecodeOutcome::Invalid { message } => {
                    return Err(ProtocolError::Invalid { message });
                }
                DecodeOutcome::UnknownCommand { name } => {
                    return Err(ProtocolError::UnknownCommand { name });
                }
            }
        }

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{serializer::resp::RespSerializer, storage::memory::MemoryStorageEngine};

    fn protocol() -> RespProtocol<MemoryStorageEngine, RespSerializer> {
        RespProtocol::new(Kernel::new(MemoryStorageEngine::new()), RespSerializer)
    }

    const SET_A: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
    const GET_A: &[u8] = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    const SET_B: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    const SET_KEY_VALUE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    /// S1 — one complete SET → 1 callback `+OK`
    #[test]
    fn s1_one_complete_set() {
        let mut p = protocol();
        let mut responses = Vec::new();
        let offset = p
            .execute(SET_KEY_VALUE, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, SET_KEY_VALUE.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
    }

    /// S2 — SET+GET same buffer → 2 callbacks in order
    #[test]
    fn s2_set_then_get_same_buffer() {
        let mut p = protocol();
        let mut buf = SET_A.to_vec();
        buf.extend_from_slice(GET_A);
        let mut responses = Vec::new();
        let offset = p.execute(&buf, |out| responses.push(out.to_vec())).unwrap();
        assert_eq!(offset, buf.len());
        assert_eq!(
            responses,
            vec![b"+OK\r\n".to_vec(), b"$1\r\n1\r\n".to_vec()]
        );
    }

    /// S3 — fragment mid-bulk → 0 callbacks; offset 0 (sobra = everything)
    #[test]
    fn s3_fragment_mid_bulk() {
        let mut p = protocol();
        // Truncate inside the key bulk of SET
        let partial = &SET_KEY_VALUE[..SET_KEY_VALUE.len() - 8];
        let mut responses = Vec::new();
        let offset = p
            .execute(partial, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, 0);
        assert!(responses.is_empty());
    }

    /// S4 — complete on second execute after Incomplete
    #[test]
    fn s4_complete_on_second_execute() {
        let mut p = protocol();
        let split = SET_KEY_VALUE.len() - 5;
        let first = &SET_KEY_VALUE[..split];
        let second = &SET_KEY_VALUE[split..];

        let mut responses = Vec::new();
        let offset1 = p
            .execute(first, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset1, 0);
        assert!(responses.is_empty());

        let mut combined = first.to_vec();
        combined.extend_from_slice(second);
        let offset2 = p
            .execute(&combined, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset2, combined.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
    }

    /// S5 — 2 complete + 1 incomplete → 2 callbacks; offset = end of second
    #[test]
    fn s5_two_complete_plus_incomplete() {
        let mut p = protocol();
        let incomplete = &SET_B[..10];
        let mut buf = SET_A.to_vec();
        buf.extend_from_slice(GET_A);
        buf.extend_from_slice(incomplete);

        let mut responses = Vec::new();
        let offset = p.execute(&buf, |out| responses.push(out.to_vec())).unwrap();
        assert_eq!(offset, SET_A.len() + GET_A.len());
        assert_eq!(
            responses,
            vec![b"+OK\r\n".to_vec(), b"$1\r\n1\r\n".to_vec()]
        );
        assert_eq!(&buf[offset..], incomplete);
    }

    /// S6 — `*3` without body → Incomplete (offset 0)
    #[test]
    fn s6_star3_without_body() {
        let mut p = protocol();
        let mut responses = Vec::new();
        let offset = p
            .execute(b"*3\r\n", |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, 0);
        assert!(responses.is_empty());
    }

    /// S7 — command `FOO` → Err (not Incomplete)
    #[test]
    fn s7_unknown_command_foo() {
        let mut p = protocol();
        let input = b"*2\r\n$3\r\nFOO\r\n$3\r\nkey\r\n";
        let mut responses = Vec::new();
        let err = p
            .execute(input, |out| responses.push(out.to_vec()))
            .unwrap_err();
        assert!(responses.is_empty());
        match err {
            ProtocolError::UnknownCommand { name } => assert_eq!(name, b"FOO"),
            other => panic!("unexpected {other:?}"),
        }
    }

    /// Incomplete + append + second execute completes one command
    #[test]
    fn incomplete_append_second_execute() {
        let mut p = protocol();
        let mut buffer = SET_KEY_VALUE[..12].to_vec();
        let mut responses = Vec::new();
        assert_eq!(
            p.execute(&buffer, |out| responses.push(out.to_vec()))
                .unwrap(),
            0
        );
        assert!(responses.is_empty());

        buffer.extend_from_slice(&SET_KEY_VALUE[12..]);
        let offset = p
            .execute(&buffer, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, buffer.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
    }

    /// Two SETs + GET in one buffer → 3 callbacks with correct payloads
    #[test]
    fn two_sets_and_get_three_callbacks() {
        let mut p = protocol();
        let mut buf = SET_A.to_vec();
        buf.extend_from_slice(SET_B);
        buf.extend_from_slice(GET_A);

        let mut responses = Vec::new();
        let offset = p.execute(&buf, |out| responses.push(out.to_vec())).unwrap();
        assert_eq!(offset, buf.len());
        assert_eq!(
            responses,
            vec![
                b"+OK\r\n".to_vec(),
                b"+OK\r\n".to_vec(),
                b"$1\r\n1\r\n".to_vec(),
            ]
        );
    }

    #[test]
    fn empty_buffer_is_ok_zero() {
        let mut p = protocol();
        let mut n = 0;
        assert_eq!(p.execute(b"", |_| n += 1).unwrap(), 0);
        assert_eq!(n, 0);
    }

    #[test]
    fn invalid_framing_is_err() {
        let mut p = protocol();
        let err = p.execute(b"+OK\r\n", |_| {}).unwrap_err();
        assert!(matches!(err, ProtocolError::Invalid { .. }));
    }
}
