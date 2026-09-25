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

/// SEE_FULL_FILE_AT_/tmp/FINAL_CONTENT.rs — truncated upload attempt marker
