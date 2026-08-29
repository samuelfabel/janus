use crate::{command::types::Command, response::types::Response};

/// Serializer trait defines the interface for encoding and decoding commands and responses.
pub trait Serializer: Send {
    /// The associated type for the iterator returned by the decode method.
    type DecodeIter<'a>: Iterator<Item = Command<'a>>
    where
        Self: 'a;

    /// Encode a response into owned RESP bytes.
    fn encode(&self, command: &Command<'_>, response: &Response) -> Vec<u8>;

    /// Decode complete commands from `data` starting at `cursor`.
    ///
    /// Advances `cursor` only past fully parsed commands. Incomplete trailing
    /// bytes leave `cursor` at the start of the incomplete frame.
    fn decode<'a>(&self, data: &'a [u8], cursor: &mut usize) -> Self::DecodeIter<'a>;
}

pub mod resp;
