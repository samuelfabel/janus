//! This module implements an in-memory storage engine that adheres to the StorageEngine trait defined in engine.rs. It uses a HashMap to store key-value pairs in memory.

use std::collections::HashMap;

use crate::storage::engine::StorageEngine;

/// Define the MemoryStorageEngine struct that implements the StorageEngine trait
pub struct MemoryStorageEngine {
    // In-memory storage represented as a HashMap
    storage: HashMap<Vec<u8>, Vec<u8>>,
}

/// Implement the StorageEngine trait for MemoryStorageEngine
impl MemoryStorageEngine {
    /// Create a new instance of MemoryStorageEngine
    ///
    /// # Returns
    /// * `Self` - A new instance of MemoryStorageEngine with an empty HashMap
    pub fn new() -> Self {
        MemoryStorageEngine {
            storage: HashMap::new(),
        }
    }
}

impl StorageEngine for MemoryStorageEngine {
    /// Get the value for a given key
    ///
    /// # Arguments
    /// * `key` - A vector of bytes representing the key
    ///
    /// # Returns
    /// * `Option<&[u8]>` - An optional slice of bytes representing the value
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.storage.get(key).map(|v| v.as_slice())
    }

    /// Stores the value associated with the given key.
    ///
    /// # Arguments
    /// * `key` - A vector of bytes representing the key
    /// * `value` - A vector of bytes representing the value
    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.storage.insert(key.to_vec(), value.to_vec());
    }

    /// Deletes the value associated with the given key.
    ///
    /// # Arguments
    /// * `key` - A vector of bytes representing the key
    ///
    /// # Returns
    /// * `bool` - A boolean indicating whether the key was found and deleted
    fn delete(&mut self, key: &[u8]) -> bool {
        self.storage.remove(key).is_some()
    }
}

mod tests {
    use crate::storage::engine::StorageEngine;

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    #[test]
    fn delete_existing_key_remove_it_and_return_true() {
        // Assert
        let mut target = super::MemoryStorageEngine::new();

        // Act
        let result = target.delete(KEY);

        // Assert
        assert!(!result);
    }

    #[test]
    fn delete_non_existing_key_return_false() {
        // Assert
        let mut target = super::MemoryStorageEngine::new();

        // Act
        let result = target.delete(KEY);

        // Assert
        assert!(!result);
    }

    #[test]
    fn execute_get_non_existing_key_return_none() {
        // Assert
        let target = super::MemoryStorageEngine::new();

        // Act
        let result = target.get(KEY);

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn execute_get_existing_key_return_value() {
        // Assert
        let mut target = super::MemoryStorageEngine::new();
        target.storage.insert(KEY.to_vec(), VALUE.to_vec());

        // Act
        let result = target.get(KEY);

        // Assert
        assert!(result.is_some());
        assert_eq!(result.unwrap(), VALUE);
    }

    #[test]
    fn execute_set_key_non_existing_key_store_it_and_return_empty() {
        // Assert
        let mut target = super::MemoryStorageEngine::new();

        // Act
        target.set(KEY, VALUE);

        let stored_value = target.get(KEY).unwrap();

        // Assert
        assert_eq!(stored_value, VALUE);
    }

    #[test]
    fn execute_set_key_store_it_and_return_empty() {
        // Assert
        let mut target = super::MemoryStorageEngine::new();
        target.storage.insert(KEY.to_vec(), VALUE.to_vec());

        // Act
        target.set(KEY, VALUE2);

        let stored_value = target.get(KEY).unwrap();

        // Assert
        assert_eq!(stored_value, VALUE2);
    }
}
