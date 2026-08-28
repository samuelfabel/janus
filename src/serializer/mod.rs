use crate::{command::types::Command, response::types::Response};

/// Serializer trait defines the interface for encoding and decoding commands and responses.
pub trait Serializer {
    /// The associated type for the iterator returned by the decode method.
    type DecodeIter<'a>: Iterator<Item = Command<'a>>
    where
        Self: 'a;

    /// Encode a command and its response into a byte slice.
    ///
    /// # Arguments
    /// * `command` - The command to encode
    /// * `response` - The response to encode
    ///
    /// # Returns
    /// A byte slice containing the encoded command and response
    fn encode<'a>(&self, command: &Command<'a>, response: &Response) -> &[u8];

    /// Decode a byte slice into an iterator of commands.
    ///
    /// # Arguments
    /// * `data` - The byte slice to decode
    /// * `cursor` - The cursor position in the byte slice
    ///
    /// # Returns
    /// An iterator over the decoded commands, each with its start offset.
    fn decode<'a>(&self, data: &'a [u8], cursor: &mut usize) -> Self::DecodeIter<'a>;
}

#[cfg(test)]
mockall::mock! {
    pub Serializer {}

    impl Serializer for Serializer {
        type DecodeIter<'a> = std::vec::IntoIter<Command<'a>>;

        fn encode<'a>(&self, command: &Command<'a>, response: &Response) -> &'static [u8];

        // No mock, desvinculamos o lifetime do retorno do argumento 'a, usando 'static para os comandos simulados
        fn decode<'a>(&self, data: &'a [u8], cursor: &mut usize) -> std::vec::IntoIter<Command<'static>>;
    }
}

pub mod resp;
