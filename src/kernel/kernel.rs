//! Kernel: map domain [`Command`](crate::command::types::Command) to
//! [`Response`](crate::response::types::Response) via a [`StorageEngine`].

use crate::{
    command::types::Command, response::types::Response, storage::engine::StorageEngine,
};

/// Executes domain commands against a storage engine.
pub struct Kernel<S: StorageEngine> {
    storage: S,
}

impl<S: StorageEngine> Kernel<S> {
    /// Creates a kernel bound to `storage`.
    pub fn new(storage: S) -> Self {
        Kernel { storage }
    }

    /// Runs `command` and returns a domain response (no RESP bytes).
    pub fn execute(&mut self, command: &Command<'_>) -> Response {
        match command {
            Command::Set { key, value } => {
                self.storage.set(key, value);
                Response::Empty
            }
            Command::Get { key } => {
                Response::Value(self.storage.get(key).map(|v| v.to_vec()))
            }
            Command::Delete { key } => Response::Deleted(self.storage.delete(key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryStorageEngine;

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    #[test]
    fn set_then_get_roundtrip() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        let set = kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(set, Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
    }

    #[test]
    fn get_miss_returns_value_none() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn set_overwrites_previous_value() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        let overwritten = kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE2,
        });
        assert_eq!(overwritten, Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE2.to_vec()))
        );
    }

    #[test]
    fn delete_hit_then_get_absent() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Delete { key: KEY }),
            Response::Deleted(true)
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn delete_miss_returns_false() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Delete { key: KEY }),
            Response::Deleted(false)
        );
    }
}
