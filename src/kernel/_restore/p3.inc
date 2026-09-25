        store::{FileSnapshotStore, SnapshotStore, boot_load},
        wal::{WalWriter, boot_wal, replay as replay_wal},
    };

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    fn kernel_with_fake_clock() -> (Kernel, FakeClock) {
        let clock = FakeClock::new();
        let kernel = Kernel::new(MemoryStorageEngine::with_clock(clock.clone()));
        (kernel, clock)
    }

    fn temp_dbfile(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "janus-f302-{}-{}-{name}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        path
    }

    fn temp_wal(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "janus-f402-{}-{}-{label}.wal",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn set_then_get_roundtrip() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        let set = kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(set, Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
    }

    #[test]
    fn get_miss_returns_value_none() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(None)
        );
    }

    #[test]
    fn set_overwrites_previous_value() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        let overwritten = kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE2,
        });
        assert_eq!(overwritten, Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE2.to_vec()))
        );
    }

    #[test]
    fn delete_hit_then_get_absent() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        });
        assert_eq!(
            kernel.execute(&Command::Delete { key: KEY }),
            Response::Deleted(true)
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
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
