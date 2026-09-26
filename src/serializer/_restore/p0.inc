//! RESP2 subset codec for SET / GET / DEL(DELETE) / EXPIRE / TTL / SAVE /
//! MULTI / EXEC / DISCARD / INFO.

use crate::{
    command::types::Command,
    response::types::Response,
    serializer::{DecodeOutcome, Serializer},
    shared::helpers::{count_digits, find_crlf, parse_integer},
};

/// RESP serializer for the v1 command subset.
#[derive(Debug, Default, Clone, Copy)]
pub struct RespSerializer;

impl Serializer for RespSerializer {
    fn encode(&self, response: &Response) -> Vec<u8> {
        match response {
            Response::Empty => b"+OK\r\n".to_vec(),
            Response::Queued => b"+QUEUED\r\n".to_vec(),
            Response::Value(None) => b"$-1\r\n".to_vec(),
            Response::Deleted(true) => b":1\r\n".to_vec(),
            Response::Deleted(false) => b":0\r\n".to_vec(),
            Response::Integer(n) => {
                let mut buffer = Vec::new();
                buffer.push(b':');
                buffer.extend_from_slice(n.to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer
            }
            Response::Error(message) => {
                let mut buffer = Vec::with_capacity(1 + message.len() + 2);
                buffer.push(b'-');
                buffer.extend_from_slice(message.as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer
            }
            Response::Value(Some(payload)) => {
                let digits = count_digits(payload.len());
                let mut buffer = Vec::with_capacity(1 + digits + 2 + payload.len() + 2);
                buffer.push(b'$');
                buffer.extend_from_slice(payload.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer.extend_from_slice(payload);
                buffer.extend_from_slice(b"\r\n");
                buffer
            }
            Response::Array(items) => {
                let mut buffer = Vec::new();
                buffer.push(b'*');
                buffer.extend_from_slice(items.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                for item in items {
                    buffer.extend_from_slice(&self.encode(item));
                }
                buffer
            }
        }
    }

    fn decode_one<'a>(&self, input: &'a [u8]) -> DecodeOutcome<'a> {
        if input.is_empty() {
            return DecodeOutcome::Incomplete;
        }
        if input[0] != b'*' {
            return DecodeOutcome::Invalid {
                message: "expected array",
            };
        }

        let mut cursor = 1usize;
        let Some(crlf) = find_crlf(&input[cursor..]) else {
            return DecodeOutcome::Incomplete;
        };
        let Ok(arg_count) = parse_integer(&input[cursor..cursor + crlf]) else {
            return DecodeOutcome::Invalid {
                message: "invalid array length",
            };
        };
        cursor += crlf + 2;

        if arg_count == 0 {
            return DecodeOutcome::Invalid {
                message: "empty array",
            };
        }

        let verb = match read_bulk(input, &mut cursor) {
            BulkRead::Ok(v) => v,
            BulkRead::Incomplete => return DecodeOutcome::Incomplete,
            BulkRead::Invalid => {
                return DecodeOutcome::Invalid {
                    message: "invalid bulk verb",
                }
            }
        };

        let verb_upper = normalize_verb(verb);

        match verb_upper.as_slice() {
            b"SET" => {
                if arg_count != 3 {
                    return DecodeOutcome::Invalid {
                        message: "SET arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid SET key",
                        }
                    }
                };
                let value = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid SET value",
