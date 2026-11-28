//! Redis-compatible key → slot mapping and local ownership (domain only).
//!
//! Transient MIGRATING / IMPORTING flags live on the Kernel; RESP `-MOVED` /
//! `-ASK` and slot→endpoint maps belong at the edge.

mod crc16;
mod hash_tag;
mod ownership;

pub use ownership::SlotOwnership;

use crc16::crc16;
use hash_tag::hash_tag_bytes;

/// Number of hash slots in Redis Cluster.
pub const SLOT_COUNT: u16 = 16384;

/// Map `key` to a Redis Cluster slot (`CRC16(hash_tag(key)) % 16384`).
pub fn slot(key: &[u8]) -> u16 {
    crc16(hash_tag_bytes(key)) % SLOT_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_slots() {
        assert_eq!(slot(b"foo"), 12182);
        assert_eq!(slot(b"bar"), 5061);
    }

    #[test]
    fn hash_tags_share_slot() {
        assert_eq!(slot(b"{user}a"), slot(b"{user}b"));
        assert_eq!(slot(b"{user}a"), 5474);
    }

    #[test]
    fn empty_braces_do_not_force_same_slot() {
        assert_ne!(slot(b"{}x"), slot(b"x"));
    }
}
