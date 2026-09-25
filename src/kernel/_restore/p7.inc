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
