                if let Err(()) = self.replicate(&ReplicationRecord::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }) {
                    return Response::Error(ERR_REPLICATION_FAILED.to_string());
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
            Command::Multi | Command::Exec | Command::Discard => {
                unreachable!("transaction control handled in execute")
            }
        }
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
