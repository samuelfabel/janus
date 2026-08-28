///! RespSerializer implements the RESP (REdis Serialization Protocol) encoding and decoding.
use crate::{
    command::types::Command,
    response::types::Response,
    serializer::Serializer,
    shared::helpers::{count_digits, find_crlf, parse_integer},
};

/// RespSerializer implements the RESP (REdis Serialization Protocol) encoding and decoding.
pub struct RespSerializer;

impl Serializer for RespSerializer {
    /// Encode a command and its response into a byte slice.
    ///
    /// # Arguments
    /// * `command` - The command to encode
    /// * `response` - The response to encode
    ///
    /// # Returns
    /// A byte slice containing the encoded command and response
    fn encode<'a>(&self, _: &Command<'a>, response: &Response) -> &[u8] {
        let required_size = match response {
            Response::Empty | Response::Value(None) => 5,
            Response::Boolean(_) => 4,
            Response::Value(Some(payload)) => {
                let len = payload.len();
                let digits = count_digits(len);
                1 + digits + 2 + len + 2
            }
        };

        let mut buffer = Vec::with_capacity(required_size);

        match response {
            Response::Empty => {
                buffer.extend_from_slice(b"$-1\r\n");
            }
            Response::Value(None) => {
                buffer.extend_from_slice(b"$-1\r\n");
            }
            Response::Boolean(b) => {
                buffer.extend_from_slice(if *b { b"$1\r\n#t\r\n" } else { b"$1\r\n#f\r\n" });
            }
            Response::Value(Some(payload)) => {
                buffer.push(b'$');
                buffer.extend_from_slice(&count_digits(payload.len()).to_string().into_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer.extend_from_slice(payload);
                buffer.extend_from_slice(b"\r\n");
            }
        }

        unsafe { std::slice::from_raw_parts(buffer.as_ptr(), buffer.len()) }
    }

    /// Decode a byte slice into an iterator of commands.
    ///
    /// # Arguments
    /// * `data` - The byte slice to decode
    /// * `cursor` - The cursor position in the byte slice
    ///
    /// # Returns
    /// An iterator over the decoded commands.
    fn decode<'a>(&self, data: &'a [u8], cursor: &mut usize) -> impl Iterator<Item = Command<'a>> {
        let data_len = data.len();

        std::iter::from_fn(move || {
            while *cursor < data_len {
                if data[*cursor] != b'*' {
                    return None;
                }
                *cursor += 1;

                if let Some(crlf) = find_crlf(&data[*cursor..]) {
                    let arg_count_res = parse_integer(&data[*cursor..*cursor + crlf]);
                    *cursor += crlf + 2;

                    if let Ok(arg_count) = arg_count_res {
                        if arg_count == 0 {
                            continue;
                        }

                        if let Some(cmd_name) = read_bulk_string(data, &mut *cursor) {
                            match cmd_name {
                                b"GET" | b"get" => {
                                    if let Some(key) = read_bulk_string(data, &mut *cursor) {
                                        return Some(Command::Get { key });
                                    }
                                }
                                b"SET" | b"set" => {
                                    if let Some(key) = read_bulk_string(data, &mut *cursor) {
                                        if let Some(value) = read_bulk_string(data, &mut *cursor) {
                                            return Some(Command::Set { key, value });
                                        }
                                    }
                                }
                                b"DEL" | b"del" => {
                                    if let Some(key) = read_bulk_string(data, &mut *cursor) {
                                        return Some(Command::Delete { key });
                                    }
                                }
                                _ => return None,
                            }
                        }
                    }
                }
                return None;
            }
            None
        })
    }
}

/// Helper functions for RESP encoding and decoding
///
/// # Arguments
/// * `data` - The byte slice to decode
/// * `cursor` - The cursor position in the byte slice
///
/// # Returns
/// The decoded bulk string, or None if the input is invalid.
#[inline]
fn read_bulk_string<'a>(data: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let start = *cursor;
    if start >= data.len() || data[start] != b'$' {
        return None;
    }
    *cursor += 1;

    let crlf = find_crlf(&data[*cursor..])?;
    let len = parse_integer(&data[*cursor..*cursor + crlf]).ok()?;
    *cursor += crlf + 2;

    if *cursor + len + 2 > data.len() {
        return None;
    }

    let payload = &data[*cursor..*cursor + len];
    *cursor += len + 2;

    Some(payload)
}
