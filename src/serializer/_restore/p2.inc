                }
                DecodeOutcome::Ok {
                    command: Command::Multi,
                    consumed: cursor,
                }
            }
            b"EXEC" => {
                if arg_count != 1 {
                    return DecodeOutcome::Invalid {
                        message: "EXEC arity",
                    };
                }
                DecodeOutcome::Ok {
                    command: Command::Exec,
                    consumed: cursor,
                }
            }
            b"DISCARD" => {
                if arg_count != 1 {
                    return DecodeOutcome::Invalid {
                        message: "DISCARD arity",
                    };
                }
                DecodeOutcome::Ok {
                    command: Command::Discard,
                    consumed: cursor,
                }
            }
            b"INFO" => {
                if arg_count != 1 {
                    return DecodeOutcome::Invalid {
                        message: "INFO arity",
                    };
                }
                DecodeOutcome::Ok {
                    command: Command::Info,
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
    const SAVE_FIXTURE: &[u8] = b"*1\r\n$4\r\nSAVE\r\n";
    const MULTI_FIXTURE: &[u8] = b"*1\r\n$5\r\nMULTI\r\n";
    const EXEC_FIXTURE: &[u8] = b"*1\r\n$4\r\nEXEC\r\n";
    const DISCARD_FIXTURE: &[u8] = b"*1\r\n$7\r\nDISCARD\r\n";
    const INFO_FIXTURE: &[u8] = b"*1\r\n$4\r\nINFO\r\n";

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
