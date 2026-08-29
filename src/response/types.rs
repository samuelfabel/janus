//! Domain responses — independent of RESP and transport.

/// Result of executing a [`crate::command::types::Command`].
///
/// Variants carry domain meaning only. RESP encoding happens in the serializer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// SET succeeded (serializer maps this to `+OK`).
    Empty,
    /// GET result: present value or absence.
    Value(Option<Vec<u8>>),
    /// DELETE result: `true` if a key was removed.
    Deleted(bool),
}
