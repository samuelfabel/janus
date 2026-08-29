//! Protocol-layer error types for [`crate::protocol::Protocol::execute`].

/// Fatal protocol outcome for the current connection (v1: caller closes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Framing or arity error from the serializer.
    Invalid { message: &'static str },
    /// Verb outside the v1 command table.
    UnknownCommand { name: Vec<u8> },
}
