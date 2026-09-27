Deleted(true)), b":1\r\n");
        assert_eq!(s.encode(&Response::Deleted(false)), b":0\r\n");
        assert_eq!(s.encode(&Response::Integer(1)), b":1\r\n");
        assert_eq!(s.encode(&Response::Integer(-1)), b":-1\r\n");
        assert_eq!(s.encode(&Response::Integer(-2)), b":-2\r\n");
        assert_eq!(
            s.encode(&Response::Error("ERR save failed".into())),
            b"-ERR save failed\r\n"
        );
        assert_eq!(
            s.encode(&Response::Value(Some(b"value".to_vec()))),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            s.encode(&Response::Value(Some(b"a\r\nb".to_vec()))),
            b"$4\r\na\r\nb\r\n"
        );
        assert_eq!(
            s.encode(&Response::Array(vec![Response::Empty, Response::Integer(1)])),
            b"*2\r\n+OK\r\n:1\r\n"
        );
    }

    #[test]
    fn decode_expire_fixture() {
        let s = RespSerializer;
        match s.decode_one(EXPIRE_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Expire { key, seconds },
                consumed,
            } => {
                assert_eq!(key, b"key");
                assert_eq!(seconds, 10);
                assert_eq!(consumed, EXPIRE_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
        let lower = b"*3\r\n$6\r\nexpire\r\n$3\r\nkey\r\n$1\r\n0\r\n";
        assert!(matches!(
            s.decode_one(lower),
            DecodeOutcome::Ok {
                command: Command::Expire { seconds: 0, .. },
                ..
            }
        ));
    }

    #[test]
    fn decode_ttl_fixture() {
        let s = RespSerializer;
        match s.decode_one(TTL_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Ttl { key },
                consumed,
            } => {
                assert_eq!(key, b"key");
                assert_eq!(consumed, TTL_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn decode_save_fixture() {
        let s = RespSerializer;
        match s.decode_one(SAVE_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Save,
                consumed,
            } => assert_eq!(consumed, SAVE_FIXTURE.len()),
            other => panic!("unexpected {other:?}"),
        }
        let lower = b"*1\r\n$4\r\nsave\r\n";
        assert!(matches!(
            s.decode_one(lower),
            DecodeOutcome::Ok {
                command: Command::Save,
                ..
            }
        ));
        let mixed = b"*1\r\n$4\r\nSave\r\n";
        assert!(matches!(
            s.decode_one(mixed),
            DecodeOutcome::Ok {
                command: Command::Save,
                ..
            }
        ));
    }

    #[test]
    fn decode_multi_exec_discard_fixtures() {
        let s = RespSerializer;
        match s.decode_one(MULTI_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Multi,
                consumed,
            } => assert_eq!(consumed, MULTI_FIXTURE.len()),
            other => panic!("unexpected {other:?}"),
        }
        match s.decode_one(EXEC_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Exec,
                consumed,
            } => assert_eq!(consumed, EXEC_FIXTURE.len()),
            other => panic!("unexpected {other:?}"),
        }
        match s.decode_one(DISCARD_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Discard,
                consumed,
            } => assert_eq!(consumed, DISCARD_FIXTURE.len()),
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(
            s.decode_one(b"*1\r\n$5\r\nmulti\r\n"),
            DecodeOutcome::Ok {
                command: Command::Multi,
                ..
            }
        ));
        assert!(matches!(
            s.decode_one(b"*1\r\n$4\r\nExec\r\n"),
            DecodeOutcome::Ok {
                command: Command::Exec,
                ..
            }
        ));
        assert!(matches!(
            s.decode_one(b"*1\r\n$7\r\ndiscard\r\n"),
            DecodeOutcome::Ok {
                command: Command::Discard,
                ..
            }
        ));
    }

    #[test]
    fn multi_exec_discard_wrong_arity_is_invalid() {
        let s = RespSerializer;
        assert!(matches!(
            s.decode_one(b"*2\r\n$5\r\nMULTI\r\n$3\r\nkey\r\n"),
            DecodeOutcome::Invalid {
                message: "MULTI arity"
            }
        ));
        assert!(matches!(
            s.decode_one(b"*2\r\n$4\r\nEXEC\r\n$3\r\nkey\r\n"),
            DecodeOutcome::Invalid {
                message: "EXEC arity"
            }
        ));
        assert!(matches!(
            s.decode_one(b"*2\r\n$7\r\nDISCARD\r\n$3\r\nkey\r\n"),
            DecodeOutcome::Invalid {
                message: "DISCARD arity"
            }
        ));
    }

    #[test]
    fn decode_info_fixture() {
        let s = RespSerializer;
        match s.decode_one(INFO_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Info,
                consumed,
            } => assert_eq!(consumed, INFO_FIXTURE.len()),
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(
            s.decode_one(b"*1\r\n$4\r\ninfo\r\n"),
            DecodeOutcome::Ok {
                command: Command::Info,
                ..
            }
        ));
        assert!(matches!(
            s.decode_one(b"*1\r\n$4\r\nInfo\r\n"),
            DecodeOutcome::Ok {
                command: Command::Info,
                ..
            }
        ));
    }

    #[test]
    fn info_wrong_arity_is_invalid() {
        let s = RespSerializer;
        assert!(matches!(
            s.decode_one(b"*2\r\n$4\r\nINFO\r\n$7\r\ndefault\r\n"),
            DecodeOutcome::Invalid {
                message: "INFO arity"
            }
        ));
    }

    #[test]
    fn encode_info_value_is_bulk_string() {
        let s = RespSerializer;
        let payload = b"# Stats\ntotal_commands:1\nkeyspace_hits:0\nkeyspace_misses:0\n";
        let encoded = s.encode(&Response::Value(Some(payload.to_vec())));
        let mut expected = Vec::new();
        expected.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
        expected.extend_from_slice(payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(encoded, expected);
    }

    #[test]
    fn decode_info_kernel_roundtrip_contains_stats() {
        let s = RespSerializer;
        let mut kernel = Kernel::new(MemoryStorageEngine::new());

        let DecodeOutcome::Ok {
            command: set_cmd, ..
        } = s.decode_one(SET_FIXTURE)
        else {
            panic!("set decode");
        };
        assert_eq!(s.encode(&kernel.execute(&set_cmd)), b"+OK\r\n");

        let DecodeOutcome::Ok {
            command: get_cmd, ..
        } = s.decode_one(GET_FIXTURE)
        else {
            panic!("get decode");
        };
        assert_eq!(s.encode(&kernel.execute(&get_cmd)), b"$5\r\nvalue\r\n");

        let DecodeOutcome::Ok {
            command: info_cmd, ..
        } = s.decode_one(INFO_FIXTURE)
        else {
            panic!("info decode");
        };
        let encoded = s.encode(&kernel.execute(&info_cmd));
        assert!(encoded.starts_with(b"$"), "expected bulk s