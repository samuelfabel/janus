//! In-memory [`StorageEngine`] backed by a [`HashMap`].

use std::collections::HashMap;

use crate::storage::engine::StorageEngine;

/// HashMap-backed storage engine for the v1 milestone.
pub struct MemoryStorageEngine {
    map: HashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryStorageEngine {
    /// Creates an empty in-memory engine.
    pub fn new() -> Self {
        MemoryStorageEngine {
            map: HashMap::new(),
        }
    }
}

impl Default for MemoryStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for MemoryStorageEngine {
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.map.get(key).map(|v| v.as_slice())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.map.insert(key.to_vec(), value.to_vec());
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        self.map.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageEngine;

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    #[test]
    fn set_then_get_returns_value() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(KEY, VALUE);
        assert_eq!(engine.get(KEY), Some(VALUE));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let engine = MemoryStorageEngine::new();
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn set_overwrites_previous_value() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(KEY, VALUE);
        engine.set(KEY, VALUE2);
        assert_eq!(engine.get(KEY), Some(VALUE2));
    }

    #[test]
    fn delete_existing_key_returns_true_and_removes() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(KEY, VALUE);
        assert!(engine.delete(KEY));
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn delete_missing_key_returns_false() {
        let mut engine = MemoryStorageEngine::new();
        assert!(!engine.delete(KEY));
    }

    #[test]
    fn empty_key_and_empty_value_are_allowed() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(b"", b"");
        assert_eq!(engine.get(b""), Some(b"".as_slice()));
        assert!(engine.delete(b""));
        assert_eq!(engine.get(b""), None);
    }

    #[test]
    fn binary_opaque_bytes_roundtrip() {
        let mut engine = MemoryStorageEngine::new();
        let key = b"\x00\xffkey";
        let value = b"val\r\n\x00ue";
        engine.set(key, value);
        assert_eq!(engine.get(key), Some(value.as_slice()));
    }
}
