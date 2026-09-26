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
