//! Protocol Instance: offset loop, kernel, encode, callback.

pub mod resp;
pub mod types;

pub use types::ProtocolError;

/// One connection's protocol orchestrator (no sockets).
pub trait Protocol {
    /// Process `buffer` from the start: decode → kernel → encode → `callback`.
    ///
    /// Returns `Ok(offset)` = bytes consumed from the front of `buffer`.
    /// The caller must preserve `buffer[offset..]` (sobra). Returns `Err` on
    /// invalid framing or unknown command; the transport closes the connection.
    fn execute(
        &mut self,
        buffer: &[u8],
        callback: impl FnMut(&[u8]),
    ) -> Result<usize, ProtocolError>;
}
