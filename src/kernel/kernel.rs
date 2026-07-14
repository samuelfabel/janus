//! Kernel module that provides the core functionality of the application.

use crate::{command::types::Command, response::types::Response, storage::engine::StorageEngine};

/// Kernel struct that holds a reference to the storage engine.
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
            Command::DELETE { key } => Response::Boolean(self.storage.delete(key)),
            Command::GET { key } => Response::Value(self.storage.get(key).map(|v| v.to_vec())),
            Command::SET { key, value } => {
                self.storage.set(key, value);
                Response::Empty
            }
        }
    }
}
