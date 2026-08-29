//! Serializer layer: bytes ↔ domain [`Command`] / [`Response`].

use crate::{command::types::Command, response::types::Response};

pub mod resp;

/// Result of attempting to decode one RESP command frame from an input slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeOutcome<'a> {
    /// Need more bytes; no domain command produced.
    Incomplete,
    /// One complete command; `consumed` is the frame size from the start of `input`.
    Ok { command: Command<'a>, consumed: usize },
    /// Framing or arity error (not Incomplete).
    Invalid { message: &'static str },
    /// Verb is not in the v1 table.
    UnknownCommand { name: Vec<u8> },
}

/// Codec between RESP bytes and domain types.
pub trait Serializer: Send {
    /// Encode a domain response to owned RESP bytes.
    fn encode(&self, response: &Response) -> Vec<u8>;

    /// Decode at most one command starting at `input[0]`.
    fn decode_one<'a>(&self, input: &'a [u8]) -> DecodeOutcome<'a>;
}
