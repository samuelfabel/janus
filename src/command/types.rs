//! Domain commands — independent of RESP and transport.

/// A kernel command over opaque byte keys and values.
///
/// Borrowed keys/values are used so the protocol layer can pass buffer slices
/// without copying. Ownership can be introduced later if measured necessary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<'a> {
    /// Store `value` under `key`.
    Set { key: &'a [u8], value: &'a [u8] },
    /// Read the value for `key`.
    Get { key: &'a [u8] },
    /// Remove `key` if present.
    Delete { key: &'a [u8] },
}
