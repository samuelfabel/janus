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
                if self.in_multi {
                    return Response::Error(ERR_MULTI_NESTED.to_string());
                }
                self.in_multi = true;
                self.multi_queue.clear();
                Response::Empty
            }
            Command::Exec => {
                self.total_commands = self.total_commands.saturating_add(1);
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
