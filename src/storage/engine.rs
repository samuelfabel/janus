//! Storage engine trait: opaque byte keys and values.

/// Pluggable key/value storage used by the kernel.
///
/// Keys and values are opaque byte sequences (including empty). There is no
/// TTL, encoding, or I/O error surface in the v1 memory engine.
pub trait StorageEngine: Send {
    /// Returns the value for `key`, if present.
    ///
    /// The borrowed slice remains valid until the next `&mut self` call on this
    /// engine.
    fn get(&self, key: &[u8]) -> Option<&[u8]>;

    /// Stores an owned copy of `value` under `key`, replacing any previous value.
    fn set(&mut self, key: &[u8], value: &[u8]);

    /// Removes `key` if it exists.
    ///
    /// Returns `true` when a value was removed, `false` when the key was absent.
    fn delete(&mut self, key: &[u8]) -> bool;
}
