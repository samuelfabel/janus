//! Domain commands — independent of RESP and transport.

/// A kernel command over opaque byte keys and values.
///
/// Borrowed keys/values are used so the protocol layer can pass buffer slices
/// without copying. Ownership can be introduced later if measured necessary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<'a> {
    /// Store `value` under `key` (clears any previous TTL).
    Set { key: &'a [u8], value: &'a [u8] },
    /// Read the value for `key`.
    Get { key: &'a [u8] },
    /// Remove `key` if present.
    Delete { key: &'a [u8] },
    /// Set a TTL of `seconds` on an existing key (`0` = expire immediately).
    Expire { key: &'a [u8], seconds: u64 },
    /// Query remaining TTL for `key`.
    Ttl { key: &'a [u8] },
    /// Synchronously persist the current store to the configured dbfile.
    Save,
    /// Enter transaction mode (subsequent commands are queued until Exec/Discard).
    Multi,
    /// Apply the queued transaction commands in order.
    Exec,
    /// Abort the transaction and clear the queue without applying.
    Discard,
}
