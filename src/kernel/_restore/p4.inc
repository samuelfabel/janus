        clock.advance(Duration::from_secs(2));
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
        assert_eq!(
            kernel.execute(&Command::Ttl { key: KEY }),
            Response::Integer(-2)
        );
    }

    #[test]
    fn expire_zero_seconds_expires_immediately_on_access() {
        let (mut kernel, _clock) = kernel_with_fake_clock();
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 0
            }),
            Response::Integer(1)
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn save_disabled_returns_error() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Save),
            Response::Error(ERR_SAVE_DISABLED.to_string())
        );
    }

    #[test]
    fn save_to_tempfile_ok_and_decodable() {
        let path = temp_dbfile("ok.snap");
        let store = FileSnapshotStore::new(&path);
        let mut kernel =
            Kernel::with_store(MemoryStorageEngine::new(), Box::new(store.clone()));
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(kernel.execute(&Command::Save), Response::Empty);

        let bytes = store.load().unwrap().expect("file written");
        assert!(!bytes.is_empty());
        let entries = decode(&bytes).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, KEY);
        assert_eq!(entries[0].value, VALUE);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn save_impossible_path_returns_error() {
        let path = PathBuf::from("/no/such/dir/janus-f302-save.snap");
        let mut kernel = Kernel::with_store(
            MemoryStorageEngine::new(),
            Box::new(FileSnapshotStore::new(path)),
        );
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Save),
            Response::Error(ERR_SAVE_FAILED.to_string())
        );
    }

    #[test]
    fn set_save_load_into_new_engine_get_hit() {
        let path = temp_dbfile("roundtrip.snap");
        let store = FileSnapshotStore::new(&path);
        let mut kernel =
            Kernel::with_store(MemoryStorageEngine::new(), Box::new(store.clone()));
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
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
