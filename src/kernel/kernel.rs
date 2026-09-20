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
/// `Arc<Mutex<Kernel<_>>>`; each [`Kernel::execute`] runs under this lock and
/// releases it before the next command on the same thread. A poisoned mutex is
/// recovered so a panicked holder does not abort the pedagogical server.
pub fn lock_kernel<S: StorageEngine>(
    mutex: &Mutex<Kernel<S>>,
) -> MutexGuard<'_, Kernel<S>> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Executes domain commands against a storage engine.
///
/// The kernel itself is single-threaded. Share across threads with
/// `Arc<Mutex<Kernel<_>>>` and [`lock_kernel`].
///
/// When a WAL is configured, successful `Set` / `Delete` / `Expire` mutations
/// are applied to storage first, then appended. If the append fails, the
/// storage mutation is **not** rolled back and the client receives
/// [`ERR_WAL_APPEND_FAILED`](ERR_WAL_APPEND_FAILED).
pub struct Kernel<S: StorageEngine> {
    storage: S,
    store: Option<Box<dyn SnapshotStore>>,
    wal: Option<WalWriter>,
}

impl<S: StorageEngine> Kernel<S> {
    /// Creates a kernel bound to `storage` with persistence disabled.
    pub fn new(storage: S) -> Self {
        Kernel {
            storage,
            store: None,
            wal: None,
        }
    }

    /// Creates a kernel with an injected snapshot store (`--dbfile`).
    pub fn with_store(storage: S, store: Box<dyn SnapshotStore>) -> Self {
        Kernel {
            storage,
            store: Some(store),
            wal: None,
        }
    }

    /// Creates a kernel with an append-only WAL (`--wal`).
    pub fn with_wal(storage: S, wal: WalWriter) -> Self {
        Kernel {
            storage,
            store: None,
            wal: Some(wal),
        }
    }

    /// Mutable access to the bound storage (tests).
    #[cfg(test)]
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
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

    fn kernel_with_fake_clock() -> Kernel<MemoryStorageEngine<FakeClock>> {
        Kernel::new(MemoryStorageEngine::with_clock(FakeClock::new()))
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
        let mut kernel = kernel_with_fake_clock();
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
        let mut kernel = kernel_with_fake_clock();
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
        let mut kernel = kernel_with_fake_clock();
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        kernel.execute(&Command::Expire {
            key: KEY,
            seconds: 1,
        });
        kernel.storage_mut().clock_mut().advance(Duration::from_secs(2));
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
        let mut kernel = kernel_with_fake_clock();
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
}
