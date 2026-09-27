//! Kernel: map domain [`Command`](crate::command::types::Command) to
//! [`Response`](crate::response::types::Response) via a [`StorageEngine`].

use std::collections::VecDeque;
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
    Info,
    Traces,
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
            Command::Info => QueuedCommand::Info,
            Command::Traces => QueuedCommand::Traces,
            Command::Multi | Command::Exec | Command::Discard => {
                unreachable!("transaction control commands are not queued")
            }
        }
    }
}

fn command_span_name(command: &Command<'_>) -> &'static str {
    match command {
        Command::Set { .. } => "SET",
        Command::Get { .. } => "GET",
        Command::Delete { .. } => "DELETE",
        Command::Expire { .. } => "EXPIRE",
        Command::Ttl { .. } => "TTL",
        Command::Save => "SAVE",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Info => "INFO",
        Command::Traces => "TRACES",
    }
}

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
///
/// When a [`ReplicationSink`] is configured, the same successful mutations are
/// notified after the local apply (and after WAL when both are set). Sink
/// failure returns [`ERR_REPLICATION_FAILED`](ERR_REPLICATION_FAILED) without
/// rolling back the local mutation (same trade-off as WAL).
///
/// After [`Command::Multi`], subsequent domain commands are queued until
/// [`Command::Exec`] (apply in order) or [`Command::Discard`] (abort). Nested
/// `MULTI` is rejected.
///
/// Pedagogical metrics (`total_commands`, `keyspace_hits`, `keyspace_misses`)
/// live on the Kernel under the same `Mutex` as shared access. Commands that
/// only enqueue during MULTI do **not** increment `total_commands`; they count
/// when applied by Exec. [`Command::Info`] returns a text payload via
/// [`Response::Value`].
///
/// Pedagogical tracing uses a fixed-capacity ring buffer of spans (`id` +
/// command name). Queued MULTI commands do **not** record a span until Exec
/// applies them. [`Command::Traces`] returns a `# Traces` text payload via
/// [`Response::Value`].
pub struct Kernel {
    storage: Box<dyn StorageEngine>,
    store: Option<Box<dyn SnapshotStore>>,
    wal: Option<WalWriter>,
    replication: Option<Box<dyn ReplicationSink>>,
    in_multi: bool,
    multi_queue: Vec<QueuedCommand>,
    total_commands: u64,
    keyspace_hits: u64,
    keyspace_misses: u64,
    next_span_id: u64,
    spans: VecDeque<TraceSpan>,
}

/// One pedagogical span: monotonic id + stable ASCII command name.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceSpan {
    id: u64,
    cmd: &'static str,
}

const TRACE_CAPACITY: usize = 64;

impl Kernel {
    /// Creates a kernel bound to `storage` with persistence disabled.
    pub fn new(storage: impl StorageEngine + 'static) -> Self {
        Self::from_boxed(Box::new(storage))
    }

    /// Creates a kernel from an already type-erased storage plugin (composition root).
    pub fn from_boxed(storage: Box<dyn StorageEngine>) -> Self {
        Kernel {
            storage,
            store: None,
            wal: None,
            replication: None,
            in_multi: false,
            multi_queue: Vec::new(),
            total_commands: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            next_span_id: 0,
            spans: VecDeque::new(),
        }
    }

    /// Creates a kernel with an injected snapshot store (`--dbfile`).
    pub fn with_store(
        storage: impl StorageEngine + 'static,
        store: Box<dyn SnapshotStore>,
    ) -> Self {
        Self::from_boxed_with_store(Box::new(storage), store)
    }

    /// Type-erased storage + snapshot store (composition root).
    pub fn from_boxed_with_store(
        storage: Box<dyn StorageEngine>,
        store: Box<dyn SnapshotStore>,
    ) -> Self {
        Kernel {
            storage,
            store: Some(store),
            wal: None,
            replication: None,
            in_multi: false,
            multi_queue: Vec::new(),
            total_commands: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            next_span_id: 0,
            spans: VecDeque::new(),
        }
    }

    /// Creates a kernel with an append-only WAL (`--wal`).
    pub fn with_wal(storage: impl StorageEngine + 'static, wal: WalWriter) -> Self {
        Self::from_boxed_with_wal(Box::new(storage), wal)
    }

    /// Type-erased storage + WAL (composition root).
    pub fn from_boxed_with_wal(storage: Box<dyn StorageEngine>, wal: WalWriter) -> Self {
        Kernel {
            storage,
            store: None,
            wal: Some(wal),
            replication: None,
            in_multi: false,
            multi_queue: Vec::new(),
            total_commands: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            next_span_id: 0,
            spans: VecDeque::new(),
        }
    }

    /// Primary with a replication sink (pedagogical Phase 10).
    pub fn with_replica(
        storage: impl StorageEngine + 'static,
        sink: Box<dyn ReplicationSink>,
    ) -> Self {
        Self::from_boxed_with_replica(Box::new(storage), sink)
    }

    /// Type-erased storage + replication sink.
    pub fn from_boxed_with_replica(
        storage: Box<dyn StorageEngine>,
        sink: Box<dyn ReplicationSink>,
    ) -> Self {
        Kernel {
            storage,
            store: None,
            wal: None,
            replication: Some(sink),
            in_multi: false,
            multi_queue: Vec::new(),
            total_commands: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            next_span_id: 0,
            spans: VecDeque::new(),
        }
    }

    /// Mutable access to the bound storage (tests).
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn storage_mut(&mut self) -> &mut dyn StorageEngine {
        self.storage.as_mut()
    }

    /// Runs `command` and returns a domain response (no RESP bytes).
    pub fn execute(&mut self, command: &Command<'_>) -> Response {
        match command {
            Command::Multi => {
                self.total_commands = self.total_commands.saturating_add(1);
                self.record_span("MULTI");
                if self.in_multi {
                    return Response::Error(ERR_MULTI_NESTED.to_string());
                }
                self.in_multi = true;
                self.multi_queue.clear();
                Response::Empty
            }
            Command::Exec => {
                self.total_commands = self.total_commands.saturating_add(1);
                self.record_span("EXEC");
                if !self.in_multi {
                    return Response::Error(ERR_EXEC_WITHOUT_MULTI.to_string());
                }
                self.in_multi = false;
                let queued = std::mem::take(&mut self.multi_queue);
                let mut results = Vec::with_capacity(queued.len());
                for item in queued {
                    results.push(self.apply_queued(item));
                }
                Response::Array(results)
            }
            Command::Discard => {
                self.total_commands = self.total_commands.saturating_add(1);
                self.record_span("DISCARD");
                if !self.in_multi {
                    return Response::Error(ERR_DISCARD_WITHOUT_MULTI.to_string());
                }
                self.in_multi = false;
                self.multi_queue.clear();
                Response::Empty
            }
            other if self.in_multi => {
                // Queued commands do not bump total_commands until Exec applies them.
                self.multi_queue.push(QueuedCommand::from_command(other));
                Response::Queued
            }
            other => self.execute_immediate(other),
        }
    }

    fn apply_queued(&mut self, queued: QueuedCommand) -> Response {
        match queued {
            QueuedCommand::Set { key, value } => self.execute_immediate(&Command::Set {
                key: key.as_slice(),
                value: value.as_slice(),
            }),
            QueuedCommand::Get { key } => {
                self.execute_immediate(&Command::Get { key: key.as_slice() })
            }
            QueuedCommand::Delete { key } => {
                self.execute_immediate(&Command::Delete { key: key.as_slice() })
            }
            QueuedCommand::Expire { key, seconds } => self.execute_immediate(&Command::Expire {
                key: key.as_slice(),
                seconds,
            }),
            QueuedCommand::Ttl { key } => {
                self.execute_immediate(&Command::Ttl { key: key.as_slice() })
            }
            QueuedCommand::Save => self.execute_immediate(&Command::Save),
            QueuedCommand::Info => self.execute_immediate(&Command::Info),
            QueuedCommand::Traces => self.execute_immediate(&Command::Traces),
        }
    }

    fn execute_immediate(&mut self, command: &Command<'_>) -> Response {
        self.total_commands = self.total_commands.saturating_add(1);
        self.record_span(command_span_name(command));
        match command {
            Command::Set { key, value } => {
                self.storage.set(key, value);
                if let Err(()) = self.append_wal(&WalRecord::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }) {
                    return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                }
                if let Err(()) = self.replicate(&ReplicationRecord::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }) {
                    return Response::Error(ERR_REPLICATION_FAILED.to_string());
                }
                Response::Empty
            }
            Command::Get { key } => {
                let value = self.storage.get(key).map(|v| v.to_vec());
                if value.is_some() {
                    self.keyspace_hits = self.keyspace_hits.saturating_add(1);
                } else {
                    self.keyspace_misses = self.keyspace_misses.saturating_add(1);
                }
                Response::Value(value)
            }
            Command::Delete { key } => {
                let deleted = self.storage.delete(key);
                if deleted {
                    if let Err(()) = self.append_wal(&WalRecord::Delete {
                        key: key.to_vec(),
                    }) {
                        return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                    }
                    if let Err(()) = self.replicate(&ReplicationRecord::Delete {
                        key: key.to_vec(),
                    }) {
                        return Response::Error(ERR_REPLICATION_FAILED.to_string());
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
                    if let Err(()) = self.replicate(&ReplicationRecord::Expire {
                        key: key.to_vec(),
                        seconds: *seconds,
                    }) {
                        return Response::Error(ERR_REPLICATION_FAILED.to_string());
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
            Command::Info => Response::Value(Some(self.format_info())),
            Command::Traces => Response::Value(Some(self.format_traces())),
            Command::Multi | Command::Exec | Command::Discard => {
                unreachable!("transaction control handled in execute")
            }
        }
    }

    /// Stable ASCII metrics payload for [`Command::Info`].
    fn format_info(&self) -> Vec<u8> {
        format!(
            "# Stats\n\
             total_commands:{}\n\
             keyspace_hits:{}\n\
             keyspace_misses:{}\n",
            self.total_commands, self.keyspace_hits, self.keyspace_misses
        )
        .into_bytes()
    }

    /// Stable ASCII span dump for [`Command::Traces`] (`# Traces` + `id cmd` lines).
    fn format_traces(&self) -> Vec<u8> {
        let mut out = String::from("# Traces\n");
        for span in &self.spans {
            out.push_str(&format!("{} {}\n", span.id, span.cmd));
        }
        out.into_bytes()
    }

    fn record_span(&mut self, cmd: &'static str) {
        self.next_span_id = self.next_span_id.saturating_add(1);
        if self.spans.len() == TRACE_CAPACITY {
            self.spans.pop_front();
        }
        self.spans.push_back(TraceSpan {
            id: self.next_span_id,
            cmd,
        });
    }

    fn append_wal(&mut self, record: &WalRecord) -> Result<(), ()> {
        let Some(wal) = self.wal.as_mut() else {
            return Ok(());
        };
        wal.append(record).map_err(|_| ())
    }

    fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()> {
        let Some(sink) = self.replication.as_mut() else {
            return Ok(());
        };
        sink.replicate(record)
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

/// Apply a replication record on a replica kernel (no sink required).
///
/// Maps each [`ReplicationRecord`] to the matching domain [`Command`] and runs
/// [`Kernel::execute`]. Expire deadlines are relative to the replica's
/// `storage.now()` at apply time.
pub fn apply_replication_record(kernel: &mut Kernel, record: &ReplicationRecord) -> Response {
    match record {
        ReplicationRecord::Set { key, value } => kernel.execute(&Command::Set {
            key: key.as_slice(),
            value: value.as_slice(),
        }),
        ReplicationRecord::Delete { key } => {
            kernel.execute(&Command::Delete { key: key.as_slice() })
        }
        ReplicationRecord::Expire { key, seconds } => kernel.execute(&Command::Expire {
            key: key.as_slice(),
            seconds: *seconds,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::{Arc, Barrier, Mutex},
        thread,
    };

    use crate::storage::{
        clock::FakeClock,
        engine::Ttl,
        memory::MemoryStorageEngine,
        snapshot::decode,
        store::{FileSnapshotStore, SnapshotStore, boot_load},
        wal::{WalWriter, boot_wal, replay as replay_wal},
    };

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    fn kernel_with_fake_clock() -> (Kernel, FakeClock) {
        let clock = FakeClock::new();
        let kernel = Kernel::new(MemoryStorageEngine::with_clock(clock.clone()));
        (kernel, clock)
    }

    fn temp_dbfile(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "janus-f302-{}-{}-{name}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        path
    }

    fn temp_wal(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "janus-f402-{}-{}-{label}.wal",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

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

    #[test]
    fn expire_existing_and_missing() {
        let (mut kernel, _clock) = kernel_with_fake_clock();
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 10
            }),
            Response::Integer(0)
        );
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 10
            }),
            Response::Integer(1)
        );
    }

    #[test]
    fn ttl_codes_and_remaining_seconds() {
        let (mut kernel, _clock) = kernel_with_fake_clock();
        assert_eq!(
            kernel.execute(&Command::Ttl { key: KEY }),
            Response::Integer(-2)
        );

        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Ttl { key: KEY }),
            Response::Integer(-1)
        );

        kernel.execute(&Command::Expire {
            key: KEY,
            seconds: 5,
        });
        let ttl = kernel.execute(&Command::Ttl { key: KEY });
        match ttl {
            Response::Integer(n) => assert!((0..=5).contains(&n)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn get_after_deadline_is_none() {
        let (mut kernel, clock) = kernel_with_fake_clock();
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        kernel.execute(&Command::Expire {
            key: KEY,
            seconds: 1,
        });
        clock.advance(Duration::from_secs(2));
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
        assert_eq!(
            kernel.execute(&Command::Ttl { key: KEY }),
            Response::Integer(-2)
        );
    }

    #[test]
    fn expire_zero_seconds_expires_immediately_on_access() {
        let (mut kernel, _clock) = kernel_with_fake_clock();
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 0
            }),
            Response::Integer(1)
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn save_disabled_returns_error() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Save),
            Response::Error(ERR_SAVE_DISABLED.to_string())
        );
    }

    #[test]
    fn save_to_tempfile_ok_and_decodable() {
        let path = temp_dbfile("ok.snap");
        let store = FileSnapshotStore::new(&path);
        let mut kernel =
            Kernel::with_store(MemoryStorageEngine::new(), Box::new(store.clone()));
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(kernel.execute(&Command::Save), Response::Empty);

        let bytes = store.load().unwrap().expect("file written");
        assert!(!bytes.is_empty());
        let entries = decode(&bytes).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, KEY);
        assert_eq!(entries[0].value, VALUE);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn save_impossible_path_returns_error() {
        let path = PathBuf::from("/no/such/dir/janus-f302-save.snap");
        let mut kernel = Kernel::with_store(
            MemoryStorageEngine::new(),
            Box::new(FileSnapshotStore::new(path)),
        );
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Save),
            Response::Error(ERR_SAVE_FAILED.to_string())
        );
    }

    #[test]
    fn set_save_load_into_new_engine_get_hit() {
        let path = temp_dbfile("roundtrip.snap");
        let store = FileSnapshotStore::new(&path);
        let mut kernel =
            Kernel::with_store(MemoryStorageEngine::new(), Box::new(store.clone()));
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(kernel.execute(&Command::Save), Response::Empty);

        let mut engine = MemoryStorageEngine::new();
        boot_load(&mut engine, &store).unwrap();
        let mut restored = Kernel::new(engine);
        assert_eq!(
            restored.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn boot_without_file_leaves_engine_empty() {
        let path = temp_dbfile("missing.snap");
        let _ = std::fs::remove_file(&path);
        let store = FileSnapshotStore::new(&path);
        let mut engine = MemoryStorageEngine::new();
        boot_load(&mut engine, &store).unwrap();
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn set_expire_with_wal_grows_file_and_replay_get_ttl() {
        let path = temp_wal("set-expire");
        let writer = WalWriter::create(&path).unwrap();
        let header_len = std::fs::metadata(&path).unwrap().len();
        let mut kernel = Kernel::with_wal(MemoryStorageEngine::new(), writer);
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 60
            }),
            Response::Integer(1)
        );
        drop(kernel);
        assert!(std::fs::metadata(&path).unwrap().len() > header_len);

        let mut engine = MemoryStorageEngine::new();
        replay_wal(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), Some(VALUE));
        match engine.ttl(KEY) {
            Ttl::Remaining(d) => assert!((1..=60).contains(&d.as_secs())),
            other => panic!("unexpected ttl {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_with_wal_replay_miss() {
        let path = temp_wal("delete");
        let writer = WalWriter::create(&path).unwrap();
        let mut kernel = Kernel::with_wal(MemoryStorageEngine::new(), writer);
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Delete { key: KEY }),
            Response::Deleted(true)
        );
        drop(kernel);

        let mut engine = MemoryStorageEngine::new();
        replay_wal(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn boot_wal_missing_file_leaves_engine_empty() {
        let path = temp_wal("boot-missing");
        assert!(!path.exists());
        let mut engine = MemoryStorageEngine::new();
        let _writer = boot_wal(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), None);
        assert!(path.exists());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn without_wal_set_does_not_create_wal_file() {
        let path = temp_wal("no-wal");
        assert!(!path.exists());
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert!(!path.exists());
    }

    #[test]
    fn expire_miss_does_not_append_wal() {
        let path = temp_wal("expire-miss");
        let writer = WalWriter::create(&path).unwrap();
        let header_len = std::fs::metadata(&path).unwrap().len();
        let mut kernel = Kernel::with_wal(MemoryStorageEngine::new(), writer);
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 10
            }),
            Response::Integer(0)
        );
        drop(kernel);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), header_len);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_miss_does_not_append_wal() {
        let path = temp_wal("delete-miss");
        let writer = WalWriter::create(&path).unwrap();
        let header_len = std::fs::metadata(&path).unwrap().len();
        let mut kernel = Kernel::with_wal(MemoryStorageEngine::new(), writer);
        assert_eq!(
            kernel.execute(&Command::Delete { key: KEY }),
            Response::Deleted(false)
        );
        drop(kernel);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), header_len);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn boot_wal_replays_existing_file() {
        let path = temp_wal("boot-replay");
        {
            let writer = WalWriter::create(&path).unwrap();
            let mut kernel = Kernel::with_wal(MemoryStorageEngine::new(), writer);
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            });
        }
        let mut engine = MemoryStorageEngine::new();
        let _writer = boot_wal(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), Some(VALUE));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn shared_arc_mutex_two_threads_set_get() {
        let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
        let barrier = Arc::new(Barrier::new(2));

        let setter_kernel = Arc::clone(&kernel);
        let setter_barrier = Arc::clone(&barrier);
        let setter = thread::spawn(move || {
            {
                let mut g = lock_kernel(&setter_kernel);
                assert_eq!(
                    g.execute(&Command::Set {
                        key: KEY,
                        value: VALUE,
                    }),
                    Response::Empty
                );
            }
            setter_barrier.wait();
        });

        let getter_kernel = Arc::clone(&kernel);
        let getter_barrier = Arc::clone(&barrier);
        let getter = thread::spawn(move || {
            getter_barrier.wait();
            let mut g = lock_kernel(&getter_kernel);
            assert_eq!(
                g.execute(&Command::Get { key: KEY }),
                Response::Value(Some(VALUE.to_vec()))
            );
        });

        setter.join().expect("setter");
        getter.join().expect("getter");
    }

    #[test]
    fn shared_arc_mutex_interleaved_delete_get_miss() {
        let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
        let after_set = Arc::new(Barrier::new(2));
        let after_delete = Arc::new(Barrier::new(2));

        let writer_kernel = Arc::clone(&kernel);
        let writer_after_set = Arc::clone(&after_set);
        let writer_after_delete = Arc::clone(&after_delete);
        let writer = thread::spawn(move || {
            {
                let mut g = lock_kernel(&writer_kernel);
                g.execute(&Command::Set {
                    key: KEY,
                    value: VALUE,
                });
            }
            writer_after_set.wait();
            writer_after_delete.wait();
            let mut g = lock_kernel(&writer_kernel);
            assert_eq!(
                g.execute(&Command::Get { key: KEY }),
                Response::Value(None)
            );
        });

        let deleter_kernel = Arc::clone(&kernel);
        let deleter_after_set = Arc::clone(&after_set);
        let deleter_after_delete = Arc::clone(&after_delete);
        let deleter = thread::spawn(move || {
            deleter_after_set.wait();
            {
                let mut g = lock_kernel(&deleter_kernel);
                assert_eq!(
                    g.execute(&Command::Delete { key: KEY }),
                    Response::Deleted(true)
                );
            }
            deleter_after_delete.wait();
        });

        writer.join().expect("writer");
        deleter.join().expect("deleter");
    }

    #[test]
    fn shared_arc_mutex_last_write_wins() {
        let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
        let mut handles = Vec::new();
        for id in 0u8..4 {
            let k = Arc::clone(&kernel);
            handles.push(thread::spawn(move || {
                let value = [id];
                for _ in 0..40 {
                    let mut g = lock_kernel(&k);
                    g.execute(&Command::Set {
                        key: KEY,
                        value: &value,
                    });
                }
            }));
        }
        for h in handles {
            h.join().expect("worker");
        }
        let mut g = lock_kernel(&kernel);
        match g.execute(&Command::Get { key: KEY }) {
            Response::Value(Some(v)) => {
                assert_eq!(v.len(), 1);
                assert!((0u8..4).contains(&v[0]), "unexpected last write {v:?}");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    /// Channel sink: primary replicates into a Vec the replica can drain.
    struct ChannelSink {
        records: Arc<Mutex<Vec<ReplicationRecord>>>,
    }

    impl ReplicationSink for ChannelSink {
        fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()> {
            self.records.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    /// Sink that always fails (tests ERR replication failed).
    struct FailingSink;

    impl ReplicationSink for FailingSink {
        fn replicate(&mut self, _record: &ReplicationRecord) -> Result<(), ()> {
            Err(())
        }
    }

    #[test]
    fn primary_sink_apply_replica_get_hit() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let mut primary = Kernel::with_replica(
            MemoryStorageEngine::new(),
            Box::new(ChannelSink {
                records: Arc::clone(&records),
            }),
        );
        assert_eq!(
            primary.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Empty
        );
        assert_eq!(
            primary.execute(&Command::Expire {
                key: KEY,
                seconds: 30
            }),
            Response::Integer(1)
        );

        let mut replica = Kernel::new(MemoryStorageEngine::new());
        for record in records.lock().unwrap().drain(..) {
            apply_replication_record(&mut replica, &record);
        }
        assert_eq!(
            replica.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
        match replica.execute(&Command::Ttl { key: KEY }) {
            Response::Integer(n) => assert!((1..=30).contains(&n)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn primary_delete_replicates_and_replica_miss() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let mut primary = Kernel::with_replica(
            MemoryStorageEngine::new(),
            Box::new(ChannelSink {
                records: Arc::clone(&records),
            }),
        );
        primary.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            primary.execute(&Command::Delete { key: KEY }),
            Response::Deleted(true)
        );

        let mut replica = Kernel::new(MemoryStorageEngine::new());
        for record in records.lock().unwrap().drain(..) {
            apply_replication_record(&mut replica, &record);
        }
        assert_eq!(
            replica.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn get_ttl_save_do_not_replicate() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let mut primary = Kernel::with_replica(
            MemoryStorageEngine::new(),
            Box::new(ChannelSink {
                records: Arc::clone(&records),
            }),
        );
        primary.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        records.lock().unwrap().clear();

        let _ = primary.execute(&Command::Get { key: KEY });
        let _ = primary.execute(&Command::Ttl { key: KEY });
        let _ = primary.execute(&Command::Save);
        assert!(records.lock().unwrap().is_empty());
    }

    #[test]
    fn delete_miss_and_expire_miss_do_not_replicate() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let mut primary = Kernel::with_replica(
            MemoryStorageEngine::new(),
            Box::new(ChannelSink {
                records: Arc::clone(&records),
            }),
        );
        assert_eq!(
            primary.execute(&Command::Delete { key: KEY }),
            Response::Deleted(false)
        );
        assert_eq!(
            primary.execute(&Command::Expire {
                key: KEY,
                seconds: 10
            }),
            Response::Integer(0)
        );
        assert!(records.lock().unwrap().is_empty());
    }

    #[test]
    fn sink_failure_returns_error_without_rollback() {
        let mut primary =
            Kernel::with_replica(MemoryStorageEngine::new(), Box::new(FailingSink));
        assert_eq!(
            primary.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Error(ERR_REPLICATION_FAILED.to_string())
        );
        // Local mutation kept (same trade-off as WAL append fail).
        assert_eq!(
            primary.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
    }

    #[test]
    fn apply_replication_record_set_delete_expire() {
        let mut replica = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            apply_replication_record(
                &mut replica,
                &ReplicationRecord::Set {
                    key: KEY.to_vec(),
                    value: VALUE.to_vec(),
                }
            ),
            Response::Empty
        );
        assert_eq!(
            apply_replication_record(
                &mut replica,
                &ReplicationRecord::Expire {
                    key: KEY.to_vec(),
                    seconds: 5,
                }
            ),
            Response::Integer(1)
        );
        assert_eq!(
            apply_replication_record(
                &mut replica,
                &ReplicationRecord::Delete {
                    key: KEY.to_vec(),
                }
            ),
            Response::Deleted(true)
        );
        assert_eq!(
            replica.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn multi_set_set_exec_get_hits() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Set {
                key: b"a",
                value: b"1",
            }),
            Response::Queued
        );
        assert_eq!(
            kernel.execute(&Command::Set {
                key: b"b",
                value: b"2",
            }),
            Response::Queued
        );
        assert_eq!(
            kernel.execute(&Command::Exec),
            Response::Array(vec![Response::Empty, Response::Empty])
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"a" }),
            Response::Value(Some(b"1".to_vec()))
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"b" }),
            Response::Value(Some(b"2".to_vec()))
        );
    }

    #[test]
    fn discard_does_not_apply_queued_sets() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Queued
        );
        assert_eq!(kernel.execute(&Command::Discard), Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn nested_multi_and_exec_without_multi_are_errors() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Exec),
            Response::Error(ERR_EXEC_WITHOUT_MULTI.to_string())
        );
        assert_eq!(
            kernel.execute(&Command::Discard),
            Response::Error(ERR_DISCARD_WITHOUT_MULTI.to_string())
        );
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Multi),
            Response::Error(ERR_MULTI_NESTED.to_string())
        );
    }

    #[test]
    fn empty_exec_returns_empty_array() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(kernel.execute(&Command::Exec), Response::Array(vec![]));
    }

    #[test]
    fn info_reports_set_get_hit_and_miss_counters() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"missing" }),
            Response::Value(None)
        );

        let info = kernel.execute(&Command::Info);
        let Response::Value(Some(payload)) = info else {
            panic!("expected Value payload, got {info:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        // SET + GET hit + GET miss + Info = 4
        assert!(
            text.contains("total_commands:4"),
            "unexpected total in {text}"
        );
        assert!(text.contains("keyspace_hits:1"), "unexpected hits in {text}");
        assert!(
            text.contains("keyspace_misses:1"),
            "unexpected misses in {text}"
        );
        assert!(text.starts_with("# Stats\n"), "missing section header");
    }

    #[test]
    fn multi_queued_commands_count_only_on_exec_apply() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty); // +1
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Queued
        ); // no total bump
        assert_eq!(
            kernel.execute(&Command::Exec),
            Response::Array(vec![Response::Empty])
        ); // +1 Exec +1 applied Set

        let info = kernel.execute(&Command::Info); // +1
        let Response::Value(Some(payload)) = info else {
            panic!("expected Value payload, got {info:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        // Multi + Exec + applied Set + Info = 4
        assert!(
            text.contains("total_commands:4"),
            "unexpected total in {text}"
        );
    }

    #[test]
    fn traces_reports_set_get_spans_in_order() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );

        let traces = kernel.execute(&Command::Traces);
        let Response::Value(Some(payload)) = traces else {
            panic!("expected Value payload, got {traces:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        assert!(text.starts_with("# Traces\n"), "missing section header in {text}");
        assert!(text.contains("1 SET\n"), "missing SET span in {text}");
        assert!(text.contains("2 GET\n"), "missing GET span in {text}");
        assert!(text.contains("3 TRACES\n"), "missing TRACES span in {text}");
    }

    #[test]
    fn multi_queued_commands_span_only_on_exec_apply() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Queued
        ); // no SET span while queued
        assert_eq!(
            kernel.execute(&Command::Exec),
            Response::Array(vec![Response::Empty])
        );

        let traces = kernel.execute(&Command::Traces);
        let Response::Value(Some(payload)) = traces else {
            panic!("expected Value payload, got {traces:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        // MULTI, EXEC, applied SET, TRACES — no extra SET between MULTI and EXEC
        assert_eq!(
            text,
            "# Traces\n1 MULTI\n2 EXEC\n3 SET\n4 TRACES\n",
            "unexpected spans in {text}"
        );
    }

    #[test]
    fn traces_ring_buffer_drops_oldest_on_overflow() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        for i in 0..(TRACE_CAPACITY + 1) {
            let key = format!("k{i}");
            kernel.execute(&Command::Set {
                key: key.as_bytes(),
                value: VALUE,
            });
        }
        // After 65 SETs the buffer holds ids 2..=65. TRACES (id 66) drops id 2.
        let traces = kernel.execute(&Command::Traces);
        let Response::Value(Some(payload)) = traces else {
            panic!("expected Value payload, got {traces:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "# Traces");
        assert!(!lines.iter().any(|l| *l == "1 SET"), "oldest dropped: {text}");
        assert!(!lines.iter().any(|l| *l == "2 SET"), "id 2 dropped by TRACES: {text}");
        assert!(lines.iter().any(|l| *l == "3 SET"), "expected id 3: {text}");
        assert!(
            lines
                .iter()
                .any(|l| *l == format!("{} SET", TRACE_CAPACITY + 1)),
            "expected newest SET: {text}"
        );
        assert!(
            lines
                .iter()
                .any(|l| *l == format!("{} TRACES", TRACE_CAPACITY + 2)),
            "expected TRACES: {text}"
        );
        let span_lines = lines.iter().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(span_lines, TRACE_CAPACITY);
    }
}
