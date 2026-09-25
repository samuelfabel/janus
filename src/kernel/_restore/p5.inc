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
