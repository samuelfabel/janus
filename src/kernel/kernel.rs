//! Kernel: map domain [`Command`](crate::command::types::Command) to
//! [`Response`](crate::response::types::Response) via a [`StorageEngine`].

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::{
    command::types::Command,
    replication::{ReplicationRecord, ReplicationSink},
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
const ERR_REPLICATION_FAILED: &str = "ERR replication failed";
const ERR_MULTI_NESTED: &str = "ERR MULTI calls can not be nested";
const ERR_EXEC_WITHOUT_MULTI: &str = "ERR EXEC without MULTI";
const ERR_DISCARD_WITHOUT_MULTI: &str = "ERR DISCARD without MULTI";

/// Owned domain command held in a MULTI queue (keys/values copied at enqueue).
#[derive(Debug, Clone, PartialEq, Eq)]
enum QueuedCommand {
    Set { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
    Expire { key: Vec<u8>, seconds: u64 },
    Ttl { key: Vec<u8> },
    Save,
}

impl QueuedCommand {
    fn from_command(command: &Command<'_>) -> Self {
        match command {
            Command::Set { key, value } => QueuedCommand::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            Command::Get { key } => QueuedCommand::Get {
                key: key.to_vec(),
            },
            Command::Delete { key } => QueuedCommand::Delete {
                key: key.to_vec(),
            },
            Command::Expire { key, seconds } => QueuedCommand::Expire {
                key: key.to_vec(),
                seconds: *seconds,
            },
            Command::Ttl { key } => QueuedCommand::Ttl {
                key: key.to_vec(),
            },
            Command::Save => QueuedCommand::Save,
            Command::Multi | Command::Exec | Command::Discard => {
                unreachable!("transaction control commands are not queued")
            }
        }
    }
}

pub fn lock_kernel(mutex: &Mutex<Kernel>) -> MutexGuard<'_, Kernel> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct Kernel {
    storage: Box<dyn StorageEngine>,
    store: Option<Box<dyn SnapshotStore>>,
    wal: Option<WalWriter>,
    replication: Option<Box<dyn ReplicationSink>>,
    in_multi: bool,
    multi_queue: Vec<QueuedCommand>,
}

impl Kernel {
    pub fn new(storage: impl StorageEngine + 'static) -> Self {
        Self::from_boxed(Box::new(storage))
    }
    pub fn from_boxed(storage: Box<dyn StorageEngine>) -> Self {
        Kernel { storage, store: None, wal: None, replication: None, in_multi: false, multi_queue: Vec::new() }
    }
    pub fn with_store(storage: impl StorageEngine + 'static, store: Box<dyn SnapshotStore>) -> Self {
        Self::from_boxed_with_store(Box::new(storage), store)
    }
    pub fn from_boxed_with_store(storage: Box<dyn StorageEngine>, store: Box<dyn SnapshotStore>) -> Self {
        Kernel { storage, store: Some(store), wal: None, replication: None, in_multi: false, multi_queue: Vec::new() }
    }
    pub fn with_wal(storage: impl StorageEngine + 'static, wal: WalWriter) -> Self {
        Self::from_boxed_with_wal(Box::new(storage), wal)
    }
    pub fn from_boxed_with_wal(storage: Box<dyn StorageEngine>, wal: WalWriter) -> Self {
        Kernel { storage, store: None, wal: Some(wal), replication: None, in_multi: false, multi_queue: Vec::new() }
    }
    pub fn with_replica(storage: impl StorageEngine + 'static, sink: Box<dyn ReplicationSink>) -> Self {
        Self::from_boxed_with_replica(Box::new(storage), sink)
    }
    pub fn from_boxed_with_replica(storage: Box<dyn StorageEngine>, sink: Box<dyn ReplicationSink>) -> Self {
        Kernel { storage, store: None, wal: None, replication: Some(sink), in_multi: false, multi_queue: Vec::new() }
    }
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn storage_mut(&mut self) -> &mut dyn StorageEngine { self.storage.as_mut() }
    pub fn execute(&mut self, command: &Command<'_>) -> Response {
        match command {
            Command::Multi => {
                if self.in_multi { return Response::Error(ERR_MULTI_NESTED.to_string()); }
                self.in_multi = true; self.multi_queue.clear(); Response::Empty
            }
            Command::Exec => {
                if !self.in_multi { return Response::Error(ERR_EXEC_WITHOUT_MULTI.to_string()); }
                self.in_multi = false;
                let queued = std::mem::take(&mut self.multi_queue);
                let mut results = Vec::with_capacity(queued.len());
                for item in queued { results.push(self.apply_queued(item)); }
                Response::Array(results)
            }
            Command::Discard => {
                if !self.in_multi { return Response::Error(ERR_DISCARD_WITHOUT_MULTI.to_string()); }
                self.in_multi = false; self.multi_queue.clear(); Response::Empty
            }
            other if self.in_multi => {
                self.multi_queue.push(QueuedCommand::from_command(other)); Response::Queued
            }
            other => self.execute_immediate(other),
        }
    }
    fn apply_queued(&mut self, queued: QueuedCommand) -> Response {
        match queued {
            QueuedCommand::Set { key, value } => self.execute_immediate(&Command::Set { key: key.as_slice(), value: value.as_slice() }),
            QueuedCommand::Get { key } => self.execute_immediate(&Command::Get { key: key.as_slice() }),
            QueuedCommand::Delete { key } => self.execute_immediate(&Command::Delete { key: key.as_slice() }),
            QueuedCommand::Expire { key, seconds } => self.execute_immediate(&Command::Expire { key: key.as_slice(), seconds }),
            QueuedCommand::Ttl { key } => self.execute_immediate(&Command::Ttl { key: key.as_slice() }),
            QueuedCommand::Save => self.execute_immediate(&Command::Save),
        }
    }
    fn execute_immediate(&mut self, command: &Command<'_>) -> Response {
        match command {
            Command::Set { key, value } => {
                self.storage.set(key, value);
                if let Err(()) = self.append_wal(&WalRecord::Set { key: key.to_vec(), value: value.to_vec() }) {
                    return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                }
                if let Err(()) = self.replicate(&ReplicationRecord::Set { key: key.to_vec(), value: value.to_vec() }) {
                    return Response::Error(ERR_REPLICATION_FAILED.to_string());
                }
                Response::Empty
            }
            Command::Get { key } => Response::Value(self.storage.get(key).map(|v| v.to_vec())),
            Command::Delete { key } => {
                let deleted = self.storage.delete(key);
                if deleted {
                    if let Err(()) = self.append_wal(&WalRecord::Delete { key: key.to_vec() }) {
                        return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                    }
                    if let Err(()) = self.replicate(&ReplicationRecord::Delete { key: key.to_vec() }) {
                        return Response::Error(ERR_REPLICATION_FAILED.to_string());
                    }
                }
                Response::Deleted(deleted)
            }
            Command::Expire { key, seconds } => {
                let deadline = self.storage.now() + Duration::from_secs(*seconds);
                let ok = self.storage.expire_at(key, deadline);
                if ok {
                    let deadline_unix_secs = unix_now_secs().saturating_add(*seconds);
                    if let Err(()) = self.append_wal(&WalRecord::Expire { key: key.to_vec(), deadline_unix_secs }) {
                        return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                    }
                    if let Err(()) = self.replicate(&ReplicationRecord::Expire { key: key.to_vec(), seconds: *seconds }) {
                        return Response::Error(ERR_REPLICATION_FAILED.to_string());
                    }
                    Response::Integer(1)
                } else { Response::Integer(0) }
            }
            Command::Ttl { key } => {
                let code = match self.storage.ttl(key) {
                    Ttl::Missing => -2, Ttl::NoExpiry => -1, Ttl::Remaining(d) => d.as_secs() as i64,
                };
                Response::Integer(code)
            }
            Command::Save => self.save(),
            Command::Multi | Command::Exec | Command::Discard => unreachable!("transaction control handled in execute"),
        }
    }
    fn append_wal(&mut self, record: &WalRecord) -> Result<(), ()> {
        let Some(wal) = self.wal.as_mut() else { return Ok(()); };
        wal.append(record).map_err(|_| ())
    }
    fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()> {
        let Some(sink) = self.replication.as_mut() else { return Ok(()); };
        sink.replicate(record)
    }
    fn save(&mut self) -> Response {
        let Some(store) = self.store.as_ref() else { return Response::Error(ERR_SAVE_DISABLED.to_string()); };
        let entries = self.storage.export_snapshot();
        let bytes = snapshot::encode(&entries);
        match store.save(&bytes) { Ok(()) => Response::Empty, Err(_) => Response::Error(ERR_SAVE_FAILED.to_string()) }
    }
}

pub fn apply_replication_record(kernel: &mut Kernel, record: &ReplicationRecord) -> Response {
    match record {
        ReplicationRecord::Set { key, value } => kernel.execute(&Command::Set { key: key.as_slice(), value: value.as_slice() }),
        ReplicationRecord::Delete { key } => kernel.execute(&Command::Delete { key: key.as_slice() }),
        ReplicationRecord::Expire { key, seconds } => kernel.execute(&Command::Expire { key: key.as_slice(), seconds: *seconds }),
    }
}

#[cfg(test)]
#[path = "kernel_tests.rs"]
mod tests;
