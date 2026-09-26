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
        }
    }

    fn execute_immediate(&mut self, command: &Command<'_>) -> Response {
        self.total_commands = self.total_commands.saturating_add(1);
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

