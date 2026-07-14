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
