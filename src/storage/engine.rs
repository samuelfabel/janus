//! Storage engine trait: opaque byte keys and values, optional TTL.

use std::time::Instant;

/// Result of querying remaining time-to-live for a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// Key absent or already expired (after lazy purge).
    Missing,
    /// Key present with no deadline.
    NoExpiry,
    /// Key present; time left until deadline.
    Remaining(std::time::Duration),
}

/// Pluggable key/value storage used by the kernel.
///
/// Keys and values are opaque byte sequences (including empty).
pub trait StorageEngine: Send {
    /// Returns the value for `key`, if present and not expired.
    ///
    /// Expired keys are removed (lazy expire). The borrowed slice remains valid
    /// until the next `&mut self` call on this engine.
    fn get(&mut self, key: &[u8]) -> Option<&[u8]>;

    /// Stores an owned copy of `value` under `key`, replacing any previous value
    /// and clearing any previous TTL.
    fn set(&mut self, key: &[u8], value: &[u8]);

    /// Removes `key` if it exists and is not already expired.
    ///
    /// Returns `true` when a live value was removed, `false` when absent/expired.
    fn delete(&mut self, key: &[u8]) -> bool;

    /// Sets an absolute expiry deadline for a live key.
    ///
    /// Returns `false` if the key is missing or already expired (and purged).
    fn expire_at(&mut self, key: &[u8], deadline: Instant) -> bool;

    /// Remaining TTL for `key` (lazy-purges when expired).
    fn ttl(&mut self, key: &[u8]) -> Ttl;
}
