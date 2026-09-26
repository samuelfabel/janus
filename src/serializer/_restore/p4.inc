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
        assert!(encoded.starts_with(b"$"), "expected bulk string, got {encoded:?}");
        let body = String::from_utf8_lossy(&encoded);
        assert!(body.contains("total_commands:"), "{body}");
        assert!(body.contains("keyspace_hits:1"), "{body}");
        assert!(body.contains("# Stats\n") || body.contains("# Stats\r\n"), "{body}");
    }

    #[test]
    fn encode_queued_and_array_exec_results() {
        let s = RespSerializer;
        assert_eq!(s.encode(&Response::Queued), b"+QUEUED\r\n");
        assert_eq!(s.encode(&Response::Array(vec![])), b"*0\r\n");
        assert_eq!(
            s.encode(&Response::Array(vec![
