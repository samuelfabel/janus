        assert_eq!(kernel.execute(&Command::Save), Response::Empty);

        let mut engine = MemoryStorageEngine::new();
        boot_load(&mut engine, &store).unwrap();
        let mut restored = Kernel::new(engine);
        assert_eq!(
            restored.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn boot_without_file_leaves_engine_empty() {
        let path = temp_dbfile("missing.snap");
        let _ = std::fs::remove_file(&path);
        let store = FileSnapshotStore::new(&path);
        let mut engine = MemoryStorageEngine::new();
        boot_load(&mut engine, &store).unwrap();
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn set_expire_with_wal_grows_file_and_replay_get_ttl() {
        let path = temp_wal("set-expire");
        let writer = WalWriter::create(&path).unwrap();
        let header_len = std::fs::metadata(&path).unwrap().len();
        let mut kernel = Kernel::with_wal(MemoryStorageEngine::new(), writer);
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 60
            }),
            Response::Integer(1)
        );
        drop(kernel);
        assert!(std::fs::metadata(&path).unwrap().len() > header_len);

        let mut engine = MemoryStorageEngine::new();
        replay_wal(&path, &mut engine).unwrap();
        assert_eq!(engine.get(KEY), Some(VALUE));
        match engine.ttl(KEY) {
            Ttl::Remaining(d) => assert!((1..=60).contains(&d.as_secs())),
            other => panic!("unexpected ttl {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_with_wal_replay_miss() {
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
