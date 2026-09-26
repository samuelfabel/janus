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
