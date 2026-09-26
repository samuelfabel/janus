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
            kernel.execute(&Command::Exec),
            Response::Error(ERR_EXEC_WITHOUT_MULTI.to_string())
        );
        assert_eq!(
            kernel.execute(&Command::Discard),
            Response::Error(ERR_DISCARD_WITHOUT_MULTI.to_string())
        );
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(
            kernel.execute(&Command::Multi),
            Response::Error(ERR_MULTI_NESTED.to_string())
        );
    }

    #[test]
    fn empty_exec_returns_empty_array() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty);
        assert_eq!(kernel.execute(&Command::Exec), Response::Array(vec![]));
    }

    #[test]
    fn info_reports_set_get_hit_and_miss_counters() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: KEY }),
            Response::Value(Some(VALUE.to_vec()))
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"missing" }),
            Response::Value(None)
        );

        let info = kernel.execute(&Command::Info);
        let Response::Value(Some(payload)) = info else {
            panic!("expected Value payload, got {info:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        // SET + GET hit + GET miss + Info = 4
        assert!(
            text.contains("total_commands:4"),
            "unexpected total in {text}"
        );
        assert!(text.contains("keyspace_hits:1"), "unexpected hits in {text}");
        assert!(
            text.contains("keyspace_misses:1"),
            "unexpected misses in {text}"
        );
        assert!(text.starts_with("# Stats\n"), "missing section header");
    }

    #[test]
    fn multi_queued_commands_count_only_on_exec_apply() {
        let mut kernel = Kernel::new(MemoryStorageEngine::new());
        assert_eq!(kernel.execute(&Command::Multi), Response::Empty); // +1
        assert_eq!(
            kernel.execute(&Command::Set {
                key: KEY,
                value: VALUE,
            }),
            Response::Queued
        ); // no total bump
        assert_eq!(
            kernel.execute(&Command::Exec),
            Response::Array(vec![Response::Empty])
        ); // +1 Exec +1 applied Set

        let info = kernel.execute(&Command::Info); // +1
        let Response::Value(Some(payload)) = info else {
            panic!("expected Value payload, got {info:?}");
        };
