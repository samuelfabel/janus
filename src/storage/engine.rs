//! This module defines the trait for the storage engine

/// Define the trait for the storage engine
pub trait StorageEngine {
    /// Define the methods for the storage engine
    ///
    /// # Arguments
    /// * `key` - A vector of bytes representing the key
    ///
    /// # Returns
    /// * `Option<&[u8]>` - An optional slice of bytes representing the value
    fn get(&self, key: &[u8]) -> Option<&[u8]>;

    /// Stores the value associated with the given key.
    ///
    /// # Arguments
    /// * `key` - A vector of bytes representing the key
    /// * `value` - A vector of bytes representing the value
    fn set(&mut self, key: &[u8], value: &[u8]);

    /// Deletes the value associated with the given key.
    ///
    /// # Arguments
    /// * `key` - A vector of bytes representing the key
    ///
    /// # Returns
    /// * `bool` - A boolean indicating whether the key was found and deleted
    fn delete(&mut self, key: &[u8]) -> bool;
}
