//! Kernel module that provides the core functionality of the application.

use crate::{command::types::Command, response::types::Response, storage::engine::StorageEngine};

/// Kernel struct that holds a reference to the storage engine.
///
/// # Type Parameters
/// * `T` - A type that implements the `StorageEngine` trait.
pub struct Kernel<T: StorageEngine> {
    /// The storage engine used by the kernel.
    storage: T,
}

impl<T: StorageEngine> Kernel<T> {
    /// Creates a new instance of the Kernel with the given storage engine.
    pub fn new(storage: T) -> Self {
        Kernel { storage }
    }

    /// Executes a command by delegating to the appropriate method on the storage engine.
    ///
    /// # Arguments
    /// * `command` - The command to execute.
    ///
    /// # Returns
    /// * `CommandResult` - The result of executing the command.
    pub fn execute(&mut self, command: &Command) -> Response {
        match command {
            Command::Delete { key } => Response::Boolean(self.storage.delete(key)),
            Command::Get { key } => Response::Value(self.storage.get(key).map(|v| v.to_vec())),
            Command::Set { key, value } => {
                self.storage.set(key, value);
                Response::Empty
            }
        }
    }
}

#[cfg(test)]
mod tests {
    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    use crate::{
        command::types::Command, response::types::Response, storage::engine::StorageEngine,
    };

    pub struct MockStorage {
        data: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    }

    impl MockStorage {
        pub fn new() -> Self {
            MockStorage {
                data: std::collections::HashMap::new(),
            }
        }
        pub fn with_value() -> Self {
            let mut data = std::collections::HashMap::new();
            data.insert(KEY.to_vec(), VALUE.to_vec());
            MockStorage { data }
        }
    }

    impl StorageEngine for MockStorage {
        fn get(&self, key: &[u8]) -> Option<&[u8]> {
            self.data.get(key).map(|v| v.as_slice())
        }

        fn set(&mut self, key: &[u8], value: &[u8]) {
            self.data.insert(key.to_vec(), value.to_vec());
        }

        fn delete(&mut self, key: &[u8]) -> bool {
            self.data.remove(key).is_some()
        }
    }

    #[test]
    fn execute_delete_existing_key_remove_it_and_return_true() {
        // Assert
        let storage = MockStorage::with_value();

        let mut target = super::Kernel::new(storage);

        // Act
        let result = target.execute(&Command::Delete { key: KEY.to_vec() });

        // Assert
        assert!(matches!(result, Response::Boolean(true)));
    }

    #[test]
    fn execute_delete_non_existing_key_return_false() {
        // Assert
        let storage = MockStorage::new();

        let mut target = super::Kernel::new(storage);

        // Act
        let result = target.execute(&Command::Delete { key: KEY.to_vec() });

        // Assert
        assert!(matches!(result, Response::Boolean(false)));
    }

    #[test]
    fn execute_get_non_existing_key_return_none() {
        // Assert
        let storage = MockStorage::new();

        let mut target = super::Kernel::new(storage);

        // Act
        let result = target.execute(&Command::Get { key: KEY.to_vec() });

        // Assert
        assert!(matches!(result, Response::Value(None)));
    }

    #[test]
    fn execute_get_existing_key_return_value() {
        // Assert
        let storage = MockStorage::with_value();

        let mut target = super::Kernel::new(storage);

        // Act
        let result = target.execute(&Command::Get { key: KEY.to_vec() });

        // Assert
        match result {
            Response::Value(Some(value)) => assert_eq!(value, VALUE),
            _ => panic!("Expected value"),
        }
    }

    #[test]
    fn execute_set_key_non_existing_key_store_it_and_return_empty() {
        // Assert
        let storage = MockStorage::new();

        let mut target = super::Kernel::new(storage);

        // Act
        let result = target.execute(&Command::Set {
            key: KEY.to_vec(),
            value: VALUE.to_vec(),
        });

        let stored_value = target.execute(&Command::Get { key: KEY.to_vec() });

        // Assert
        assert!(matches!(result, Response::Empty));
        match stored_value {
            Response::Value(Some(value)) => assert_eq!(value, VALUE),
            _ => panic!("Expected value"),
        }
    }

    #[test]
    fn execute_set_key_store_it_and_return_empty() {
        // Assert
        let storage = MockStorage::with_value();

        let mut target = super::Kernel::new(storage);

        // Act
        let result = target.execute(&Command::Set {
            key: KEY.to_vec(),
            value: VALUE2.to_vec(),
        });

        let stored_value = target.execute(&Command::Get { key: KEY.to_vec() });

        // Assert
        assert!(matches!(result, Response::Empty));
        match stored_value {
            Response::Value(Some(value)) => assert_eq!(value, VALUE2),
            _ => panic!("Expected value"),
        }
    }
}
