//! Kernel: map domain [`Command`](crate::command::types::Command) to
//! [`Response`](crate::response::types::Response) via a [`StorageEngine`].

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::{
    command::types::Command,
    response::types::Response,
    storage::{
        engine::{StorageEngine, Ttl},
        snapshot,
        store::SnapshotStore,
        wal::{WalRecord, WalWriter, unix_now_secs},
    },
};

const ERR_SAVE_FAILED: &str = "ERR save failed";
const ERR_SAVE_DISABLED: &str = "ERR save disabled";
const ERR_WAL_APPEND_FAILED: &str = "ERR wal append failed";

/// Lock a shared kernel, recovering from poison via [`PoisonError::into_inner`](std::sync::PoisonError::into_inner).
///
/// The domain [`Kernel`] is single-threaded. Process-wide sharing uses
/// `Arc<Mutex<Kernel>>`; each [`Kernel::execute`] runs under this lock and
/// releases it before the next command on the same thread. A poisoned mutex is
/// recovered so a panicked holder does not abort the pedagogical server.
pub fn lock_kernel(mutex: &Mutex<Kernel>) -> MutexGuard<'_, Kernel> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Executes domain commands against a storage engine.
///
/// Storage is type-erased (`Box<dyn StorageEngine>`) so callers inject any
/// plugin behind the same Kernel. Share across threads with
/// `Arc<Mutex<Kernel>>` and [`lock_kernel`].
///
/// When a WAL is configured, successful `Set` / `Delete` / `Expire` mutations
/// are applied to storage first, then appended. If the append fails, the
/// storage mutation is **not** rolled back and the client receives
/// [`ERR_WAL_APPEND_FAILED`](ERR_WAL_APPEND_FAILED).
pub struct Kernel {
    storage: Box<dyn StorageEngine>,
    store: Option<Box<dyn SnapshotStore>>,
    wal: Option<WalWriter>,
}

impl Kernel {
    /// Creates a kernel bound to `storage` with persistence disabled.
    pub fn new(storage: impl StorageEngine + 'static) -> Self {
        Kernel {
            storage: Box::new(storage),
            store: None,
            wal: None,
        }
    }

    /// Creates a kernel with an injected snapshot store (`--dbfile`).
    pub fn with_store(
        storage: impl StorageEngine + 'static,
        store: Box<dyn SnapshotStore>,
    ) -> Self {
        Kernel {
            storage: Box::new(storage),
            store: Some(store),
            wal: None,
        }
    }

    /// Creates a kernel with an append-only WAL (`--wal`).
    pub fn with_wal(storage: impl StorageEngine + 'static, wal: WalWriter) -> Self {
        Kernel {
            storage: Box::new(storage),
            store: None,
            wal: Some(wal),
        }
    }

    /// Mutable access to the bound storage (tests).
    #[cfg(test)]
    pub fn storage_mut(&mut self) -> &mut dyn StorageEngine {
        self.storage.as_mut()
    }

    /// Runs `command` and returns a domain response (no RESP bytes).
    pub fn execute(&mut self, command: &Command<'_>) -> Response {
        match command {
            Command::Set { key, value } => {
                self.storage.set(key, value);
                if let Err(()) = self.append_wal(&WalRecord::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }) {
                    return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                }
                Response::Empty
            }
            Command::Get { key } => {
                Response::Value(self.storage.get(key).map(|v| v.to_vec()))
            }
            Command::Delete { key } => {
                let deleted = self.storage.delete(key);
                if deleted {
                    if let Err(()) = self.append_wal(&WalRecord::Delete {
                        key: key.to_vec(),
                    }) {
                        return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                    }
                }
                Response::Deleted(deleted)
            }
            Command::Expire { key, seconds } => {
                // seconds == 0 → deadline == now → expires on next access (deadline <= now).
                let deadline = self.storage.now() + Duration::from_secs(*seconds);
                let ok = self.storage.expire_at(key, deadline);
                if ok {
                    let deadline_unix_secs = unix_now_secs().saturating_add(*seconds);
                    if let Err(()) = self.append_wal(&WalRecord::Expire {
                        key: key.to_vec(),
                        deadline_unix_secs,
                    }) {
                        return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                    }
                    Response::Integer(1)
                } else {
                    Response::Integer(0)
                }
            }
            Command::Ttl { key } => {
                let code = match self.storage.ttl(key) {
                    Ttl::Missing => -2,
                    Ttl::NoExpiry => -1,
                    Ttl::Remaining(d) => d.as_secs() as i64,
                };
                Response::Integer(code)
            }
            Command::Save => self.save(),
        }
    }

    fn append_wal(&mut self, record: &WalRecord) -> Result<(), ()> {
        let Some(wal) = self.wal.as_mut() else {
            return Ok(());
        };
        wal.append(record).map_err(|_| ())
    }

    fn save(&mut self) -> Response {
        let Some(store) = self.store.as_ref() else {
            return Response::Error(ERR_SAVE_DISABLED.to_string());
        };
        let entries = self.storage.export_snapshot();
        let bytes = snapshot::encode(&entries);
        match store.save(&bytes) {
            Ok(()) => Response::Empty,
            Err(_) => Response::Error(ERR_SAVE_FAILED.to_string()),
        }
    }
}
