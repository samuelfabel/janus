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
