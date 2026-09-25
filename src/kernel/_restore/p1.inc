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
                if self.in_multi {
                    return Response::Error(ERR_MULTI_NESTED.to_string());
                }
                self.in_multi = true;
                self.multi_queue.clear();
                Response::Empty
            }
            Command::Exec => {
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
                if !self.in_multi {
                    return Response::Error(ERR_DISCARD_WITHOUT_MULTI.to_string());
                }
                self.in_multi = false;
                self.multi_queue.clear();
                Response::Empty
            }
            other if self.in_multi => {
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
        }
    }

    fn execute_immediate(&mut self, command: &Command<'_>) -> Response {
        match command {
            Command::Set { key, value } => {
                self.storage.set(key, value);
                if let Err(()) = self.append_wal(&WalRecord::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }) {
                    return Response::Error(ERR_WAL_APPEND_FAILED.to_string());
                }
