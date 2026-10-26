//! Owned mutation records for primary → replica replication.

/// A domain mutation to replicate (owned bytes; independent of RESP/`Command` borrows).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationRecord {
    /// Upsert `value` under `key` (clears any previous TTL on apply).
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Remove `key` if present.
    Delete { key: Vec<u8> },
    /// Set a TTL of `seconds` on an existing key (`0` = expire immediately on next access).
    Expire { key: Vec<u8>, seconds: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_delete_expire_variants_roundtrip_equality() {
        let set = ReplicationRecord::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let delete = ReplicationRecord::Delete {
            key: b"k".to_vec(),
        };
        let expire = ReplicationRecord::Expire {
            key: b"k".to_vec(),
            seconds: 10,
        };
        assert_eq!(set, set.clone());
        assert_eq!(delete, delete.clone());
        assert_eq!(expire, expire.clone());
        assert_ne!(set, delete);
    }
}
