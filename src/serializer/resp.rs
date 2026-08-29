//! RespSerializer implements RESP encoding and decoding for the v1 command subset.
use crate::{
    command::types::Command,
    response::types::Response,
    serializer::Serializer,
    shared::helpers::{count_digits, find_crlf, parse_integer},
};

/// RespSerializer implements RESP encoding and decoding.
pub struct RespSerializer;

impl Serializer for RespSerializer {
    type DecodeIter<'a> = std::vec::IntoIter<Command<'a>>;

    fn encode(&self, _: &Command<'_>, response: &Response) -> Vec<u8> {
        match response {
            Response::Empty => b"+OK\r\n".to_vec(),
            Response::Value(None) => b"$-1\r\n".to_vec(),
            Response::Boolean(true) => b":1\r\n".to_vec(),
            Response::Boolean(false) => b":0\r\n".to_vec(),
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

    fn decode<'a>(&self, data: &'a [u8], cursor: &mut usize) -> Self::DecodeIter<'a> {
        let mut commands = Vec::new();

        loop {
            let start = *cursor;
            match parse_one_command(data, cursor) {
                ParseResult::Command(cmd) => commands.push(cmd),
                ParseResult::Incomplete => {
                    *cursor = start;
                    break;
                }
                ParseResult::Invalid => break,
            }
        }

        commands.into_iter()
    }
}

enum ParseResult<'a> {
    Command(Command<'a>),
    Incomplete,
    Invalid,
}

fn parse_one_command<'a>(data: &'a [u8], cursor: &mut usize) -> ParseResult<'a> {
    if *cursor >= data.len() {
        return ParseResult::Incomplete;
    }
    if data[*cursor] != b'*' {
        return ParseResult::Invalid;
    }
    *cursor += 1;

    let Some(crlf) = find_crlf(&data[*cursor..]) else {
        return ParseResult::Incomplete;
    };
    let Ok(arg_count) = parse_integer(&data[*cursor..*cursor + crlf]) else {
        return ParseResult::Invalid;
    };
    *cursor += crlf + 2;

    if arg_count == 0 {
        return ParseResult::Invalid;
    }

    let Some(cmd_name) = read_bulk_string(data, cursor) else {
        return match bulk_status(data, *cursor) {
            BulkStatus::Incomplete => ParseResult::Incomplete,
            BulkStatus::Invalid => ParseResult::Invalid,
        };
    };

    match cmd_name {
        b"GET" | b"get" => {
            if arg_count != 2 {
                return ParseResult::Invalid;
            }
            match read_bulk_string(data, cursor) {
                Some(key) => ParseResult::Command(Command::Get { key }),
                None => match bulk_status(data, *cursor) {
                    BulkStatus::Incomplete => ParseResult::Incomplete,
                    BulkStatus::Invalid => ParseResult::Invalid,
                },
            }
        }
        b"SET" | b"set" => {
            if arg_count != 3 {
                return ParseResult::Invalid;
            }
            let Some(key) = read_bulk_string(data, cursor) else {
                return match bulk_status(data, *cursor) {
                    BulkStatus::Incomplete => ParseResult::Incomplete,
                    BulkStatus::Invalid => ParseResult::Invalid,
                };
            };
            match read_bulk_string(data, cursor) {
                Some(value) => ParseResult::Command(Command::Set { key, value }),
                None => match bulk_status(data, *cursor) {
                    BulkStatus::Incomplete => ParseResult::Incomplete,
                    BulkStatus::Invalid => ParseResult::Invalid,
                },
            }
        }
        b"DEL" | b"del" | b"DELETE" | b"delete" => {
            if arg_count != 2 {
                return ParseResult::Invalid;
            }
            match read_bulk_string(data, cursor) {
                Some(key) => ParseResult::Command(Command::Delete { key }),
                None => match bulk_status(data, *cursor) {
                    BulkStatus::Incomplete => ParseResult::Incomplete,
                    BulkStatus::Invalid => ParseResult::Invalid,
                },
            }
        }
        _ => ParseResult::Invalid,
    }
}

enum BulkStatus {
    Incomplete,
    Invalid,
}

fn bulk_status(data: &[u8], cursor: usize) -> BulkStatus {
    if cursor >= data.len() {
        return BulkStatus::Incomplete;
    }
    if data[cursor] != b'$' {
        return BulkStatus::Invalid;
    }
    let rest = &data[cursor + 1..];
    let Some(crlf) = find_crlf(rest) else {
        return BulkStatus::Incomplete;
    };
    let Ok(len) = parse_integer(&rest[..crlf]) else {
        return BulkStatus::Invalid;
    };
    let payload_start = cursor + 1 + crlf + 2;
    if payload_start + len + 2 > data.len() {
        BulkStatus::Incomplete
    } else {
        BulkStatus::Invalid
    }
}

#[inline]
fn read_bulk_string<'a>(data: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let start = *cursor;
    if start >= data.len() || data[start] != b'$' {
        return None;
    }

    let after_dollar = start + 1;
    let crlf = find_crlf(&data[after_dollar..])?;
    let len = parse_integer(&data[after_dollar..after_dollar + crlf]).ok()?;
    let payload_start = after_dollar + crlf + 2;
    if payload_start + len + 2 > data.len() {
        return None;
    }

    let payload = &data[payload_start..payload_start + len];
    *cursor = payload_start + len + 2;
    Some(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::types::Command;

    #[test]
    fn decode_set_get_del_fixtures() {
        let serializer = RespSerializer;
        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let mut cursor = 0;
        let commands: Vec<_> = serializer.decode(set, &mut cursor).collect();
        assert_eq!(cursor, set.len());
        assert!(matches!(
            commands.as_slice(),
            [Command::Set {
                key: b"key",
                value: b"value"
            }]
        ));

        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        cursor = 0;
        let commands: Vec<_> = serializer.decode(get, &mut cursor).collect();
        assert!(matches!(commands.as_slice(), [Command::Get { key: b"key" }]));

        let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        cursor = 0;
        let commands: Vec<_> = serializer.decode(del, &mut cursor).collect();
        assert!(matches!(
            commands.as_slice(),
            [Command::Delete { key: b"key" }]
        ));
    }

    #[test]
    fn decode_incomplete_does_not_consume() {
        let serializer = RespSerializer;
        let partial = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nval";
        let mut cursor = 0;
        let commands: Vec<_> = serializer.decode(partial, &mut cursor).collect();
        assert!(commands.is_empty());
        assert_eq!(cursor, 0);
    }

    #[test]
    fn encode_empty_and_boolean() {
        let serializer = RespSerializer;
        let cmd = Command::Set {
            key: b"k",
            value: b"v",
        };
        assert_eq!(serializer.encode(&cmd, &Response::Empty), b"+OK\r\n");
        assert_eq!(serializer.encode(&cmd, &Response::Boolean(true)), b":1\r\n");
        assert_eq!(
            serializer.encode(&cmd, &Response::Boolean(false)),
            b":0\r\n"
        );
    }
}
