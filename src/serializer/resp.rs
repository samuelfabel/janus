//! RESP2 subset codec for SET / GET / DEL(DELETE) / EXPIRE / TTL.

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
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Set { key, value },
                    consumed: cursor,
                }
            }
            b"GET" => {
                if arg_count != 2 {
                    return DecodeOutcome::Invalid {
                        message: "GET arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid GET key",
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Get { key },
                    consumed: cursor,
                }
            }
            b"DEL" | b"DELETE" => {
                if arg_count != 2 {
                    return DecodeOutcome::Invalid {
                        message: "DEL arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid DEL key",
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Delete { key },
                    consumed: cursor,
                }
            }
            b"EXPIRE" => {
                if arg_count != 3 {
                    return DecodeOutcome::Invalid {
                        message: "EXPIRE arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid EXPIRE key",
                        }
                    }
                };
                let seconds_bytes = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid EXPIRE seconds",
                        }
                    }
                };
                let Ok(seconds) = parse_u64_decimal(seconds_bytes) else {
                    return DecodeOutcome::Invalid {
                        message: "EXPIRE seconds parse",
                    };
                };
                DecodeOutcome::Ok {
                    command: Command::Expire { key, seconds },
                    consumed: cursor,
                }
            }
            b"TTL" => {
                if arg_count != 2 {
                    return DecodeOutcome::Invalid {
                        message: "TTL arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid TTL key",
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Ttl { key },
                    consumed: cursor,
                }
            }
            _ => DecodeOutcome::UnknownCommand {
                name: verb.to_vec(),
            },
        }
    }
}

enum BulkRead<'a> {
    Ok(&'a [u8]),
    Incomplete,
    Invalid,
}

fn read_bulk<'a>(input: &'a [u8], cursor: &mut usize) -> BulkRead<'a> {
    if *cursor >= input.len() {
        return BulkRead::Incomplete;
    }
    if input[*cursor] != b'$' {
        return BulkRead::Invalid;
    }
    let after_dollar = *cursor + 1;
    let Some(crlf) = find_crlf(&input[after_dollar..]) else {
        return BulkRead::Incomplete;
    };
    let Ok(len) = parse_integer(&input[after_dollar..after_dollar + crlf]) else {
        return BulkRead::Invalid;
    };
    let payload_start = after_dollar + crlf + 2;
    if payload_start + len + 2 > input.len() {
        return BulkRead::Incomplete;
    }
    if &input[payload_start + len..payload_start + len + 2] != b"\r\n" {
        return BulkRead::Invalid;
    }
    let payload = &input[payload_start..payload_start + len];
    *cursor = payload_start + len + 2;
    BulkRead::Ok(payload)
}

fn normalize_verb(verb: &[u8]) -> Vec<u8> {
    verb.iter().map(|b| b.to_ascii_uppercase()).collect()
}

/// Parse an unsigned decimal integer from ASCII digits (EXPIRE seconds).
fn parse_u64_decimal(slice: &[u8]) -> Result<u64, ()> {
    if slice.is_empty() {
        return Err(());
    }
    let mut num: u64 = 0;
    for &b in slice {
        if !b.is_ascii_digit() {
            return Err(());
        }
        num = num
            .checked_mul(10)
            .and_then(|n| n.checked_add((b - b'0') as u64))
            .ok_or(())?;
    }
    Ok(num)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::types::Command, kernel::kernel::Kernel, response::types::Response,
        storage::memory::MemoryStorageEngine,
    };

    const SET_FIXTURE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    const GET_FIXTURE: &[u8] = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    const DEL_FIXTURE: &[u8] = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
    const EXPIRE_FIXTURE: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n";
    const TTL_FIXTURE: &[u8] = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";

    #[test]
    fn decode_set_fixture() {
        let s = RespSerializer;
        match s.decode_one(SET_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Set { key, value },
                consumed,
            } => {
                assert_eq!(key, b"key");
                assert_eq!(value, b"value");
                assert_eq!(consumed, SET_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn decode_get_and_del_fixtures() {
        let s = RespSerializer;
        match s.decode_one(GET_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Get { key },
                consumed,
            } => {
                assert_eq!(key, b"key");
                assert_eq!(consumed, GET_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
        match s.decode_one(DEL_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Delete { key },
                ..
            } => assert_eq!(key, b"key"),
            other => panic!("unexpected {other:?}"),
        }
        let delete = b"*2\r\n$6\r\nDELETE\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(delete),
            DecodeOutcome::Ok {
                command: Command::Delete { .. },
                ..
            }
        ));
    }

    #[test]
    fn encode_response_variants() {
        let s = RespSerializer;
        assert_eq!(s.encode(&Response::Empty), b"+OK\r\n");
        assert_eq!(s.encode(&Response::Value(None)), b"$-1\r\n");
        assert_eq!(s.encode(&Response::Deleted(true)), b":1\r\n");
        assert_eq!(s.encode(&Response::Deleted(false)), b":0\r\n");
        assert_eq!(s.encode(&Response::Integer(1)), b":1\r\n");
        assert_eq!(s.encode(&Response::Integer(-1)), b":-1\r\n");
        assert_eq!(s.encode(&Response::Integer(-2)), b":-2\r\n");
        assert_eq!(
            s.encode(&Response::Value(Some(b"value".to_vec()))),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            s.encode(&Response::Value(Some(b"a\r\nb".to_vec()))),
            b"$4\r\na\r\nb\r\n"
        );
    }

    #[test]
    fn decode_expire_fixture() {
        let s = RespSerializer;
        match s.decode_one(EXPIRE_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Expire { key, seconds },
                consumed,
            } => {
                assert_eq!(key, b"key");
                assert_eq!(seconds, 10);
                assert_eq!(consumed, EXPIRE_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
        let lower = b"*3\r\n$6\r\nexpire\r\n$3\r\nkey\r\n$1\r\n0\r\n";
        assert!(matches!(
            s.decode_one(lower),
            DecodeOutcome::Ok {
                command: Command::Expire { seconds: 0, .. },
                ..
            }
        ));
    }

    #[test]
    fn decode_ttl_fixture() {
        let s = RespSerializer;
        match s.decode_one(TTL_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Ttl { key },
                consumed,
            } => {
                assert_eq!(key, b"key");
                assert_eq!(consumed, TTL_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn expire_wrong_arity_and_bad_seconds_are_invalid() {
        let s = RespSerializer;
        let arity = b"*2\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(arity),
            DecodeOutcome::Invalid {
                message: "EXPIRE arity"
            }
        ));
        let bad_secs = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$3\r\nabc\r\n";
        assert!(matches!(
            s.decode_one(bad_secs),
            DecodeOutcome::Invalid {
                message: "EXPIRE seconds parse"
            }
        ));
        let ttl_arity = b"*3\r\n$3\r\nTTL\r\n$3\r\nkey\r\n$1\r\nx\r\n";
        assert!(matches!(
            s.decode_one(ttl_arity),
            DecodeOutcome::Invalid {
                message: "TTL arity"
            }
        ));
    }

    #[test]
    fn truncated_set_is_incomplete() {
        let s = RespSerializer;
        let partial = &SET_FIXTURE[..SET_FIXTURE.len() - 3];
        assert_eq!(s.decode_one(partial), DecodeOutcome::Incomplete);
    }

    #[test]
    fn unknown_verb_is_unknown_command() {
        let s = RespSerializer;
        let input = b"*2\r\n$4\r\nPING\r\n$3\r\nkey\r\n";
        match s.decode_one(input) {
            DecodeOutcome::UnknownCommand { name } => assert_eq!(name, b"PING"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn wrong_arity_is_invalid() {
        let s = RespSerializer;
        // SET with only one bulk after verb → arity 2 instead of 3
        let input = b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(input),
            DecodeOutcome::Invalid { message: "SET arity" }
        ));
    }

    #[test]
    fn decode_kernel_encode_roundtrip() {
        let s = RespSerializer;
        let mut kernel = Kernel::new(MemoryStorageEngine::new());

        let DecodeOutcome::Ok {
            command: set_cmd, ..
        } = s.decode_one(SET_FIXTURE)
        else {
            panic!("set decode");
        };
        let set_resp = kernel.execute(&set_cmd);
        assert_eq!(s.encode(&set_resp), b"+OK\r\n");

        let DecodeOutcome::Ok {
            command: get_cmd, ..
        } = s.decode_one(GET_FIXTURE)
        else {
            panic!("get decode");
        };
        let get_resp = kernel.execute(&get_cmd);
        assert_eq!(s.encode(&get_resp), b"$5\r\nvalue\r\n");
    }
}
