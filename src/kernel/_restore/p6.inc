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
