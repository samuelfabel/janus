            Response::Value(None)
        );
    }

    #[test]
    fn delete_miss_returns_false() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Delete { key: KEY }),
            Response::Deleted(false)
        );
    }

    #[test]
    fn expire_existing_and_missing() {
        let (mut kernel, _clock) = kernel_with_fake_clock();
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 10
            }),
            Response::Integer(0)
        );
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Expire {
                key: KEY,
                seconds: 10
            }),
            Response::Integer(1)
        );
    }

    #[test]
    fn ttl_codes_and_remaining_seconds() {
        let (mut kernel, _clock) = kernel_with_fake_clock();
        assert_eq!(
            kernel.execute(&Command::Ttl { key: KEY }),
            Response::Integer(-2)
        );

        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Ttl { key: KEY }),
            Response::Integer(-1)
        );

        kernel.execute(&Command::Expire {
            key: KEY,
            seconds: 5,
        });
        let ttl = kernel.execute(&Command::Ttl { key: KEY });
        match ttl {
            Response::Integer(n) => assert!((0..=5).contains(&n)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn get_after_deadline_is_none() {
        let (mut kernel, clock) = kernel_with_fake_clock();
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        kernel.execute(&Command::Expire {
            key: KEY,
            seconds: 1,
        });
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
