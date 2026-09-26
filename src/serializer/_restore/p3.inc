                assert_eq!(consumed, GET_FIXTURE.len());
            }
            other => panic!("unexpected {other:?}"),
        }
        match s.decode_one(DEL_FIXTURE) {
            DecodeOutcome::Ok {
                command: Command::Delete { key },
                ..
            } => assert_eq!(key, b"key"),
            other => panic!("unexpected {other:?}"),
        }
        let delete = b"*2\r\n$6\r\nDELETE\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(delete),
            DecodeOutcome::Ok {
                command: Command::Delete { .. },
                ..
            }
        ));
    }

    #[test]
    fn encode_response_variants() {
        let s = RespSerializer;
        assert_eq!(s.encode(&Response::Empty), b"+OK\r\n");
        assert_eq!(s.encode(&Response::Queued), b"+QUEUED\r\n");
        assert_eq!(s.encode(&Response::Value(None)), b"$-1\r\n");
        assert_eq!(s.encode(&Response::Deleted(true)), b":1\r\n");
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
