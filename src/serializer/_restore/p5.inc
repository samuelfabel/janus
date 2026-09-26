                Response::Empty,
                Response::Value(Some(b"v".to_vec())),
                Response::Integer(1),
            ])),
            b"*3\r\n+OK\r\n$1\r\nv\r\n:1\r\n"
        );
    }

    #[test]
    fn decode_multi_set_exec_kernel_roundtrip() {
        let s = RespSerializer;
        let mut kernel = Kernel::new(MemoryStorageEngine::new());

        let DecodeOutcome::Ok {
            command: multi, ..
        } = s.decode_one(MULTI_FIXTURE)
        else {
            panic!("multi decode");
        };
        assert_eq!(s.encode(&kernel.execute(&multi)), b"+OK\r\n");

        let DecodeOutcome::Ok {
            command: set_cmd, ..
        } = s.decode_one(SET_FIXTURE)
        else {
            panic!("set decode");
        };
        assert_eq!(s.encode(&kernel.execute(&set_cmd)), b"+QUEUED\r\n");

        let DecodeOutcome::Ok {
            command: exec, ..
        } = s.decode_one(EXEC_FIXTURE)
        else {
            panic!("exec decode");
        };
        assert_eq!(
            s.encode(&kernel.execute(&exec)),
            b"*1\r\n+OK\r\n"
        );

        let DecodeOutcome::Ok {
            command: get_cmd, ..
        } = s.decode_one(GET_FIXTURE)
        else {
            panic!("get decode");
        };
        assert_eq!(s.encode(&kernel.execute(&get_cmd)), b"$5\r\nvalue\r\n");
    }

    #[test]
    fn save_wrong_arity_is_invalid() {
        let s = RespSerializer;
        let arity = b"*2\r\n$4\r\nSAVE\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(arity),
            DecodeOutcome::Invalid {
                message: "SAVE arity"
            }
        ));
    }

    #[test]
    fn expire_wrong_arity_and_bad_seconds_are_invalid() {
        let s = RespSerializer;
        let arity = b"*2\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(arity),
            DecodeOutcome::Invalid {
                message: "EXPIRE arity"
            }
        ));
        let bad_secs = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$3\r\nabc\r\n";
        assert!(matches!(
            s.decode_one(bad_secs),
            DecodeOutcome::Invalid {
                message: "EXPIRE seconds parse"
            }
        ));
        let ttl_arity = b"*3\r\n$3\r\nTTL\r\n$3\r\nkey\r\n$1\r\nx\r\n";
        assert!(matches!(
            s.decode_one(ttl_arity),
            DecodeOutcome::Invalid {
                message: "TTL arity"
            }
        ));
    }

    #[test]
    fn truncated_set_is_incomplete() {
        let s = RespSerializer;
        let partial = &SET_FIXTURE[..SET_FIXTURE.len() - 3];
        assert_eq!(s.decode_one(partial), DecodeOutcome::Incomplete);
    }

    #[test]
    fn unknown_verb_is_unknown_command() {
        let s = RespSerializer;
        let input = b"*2\r\n$4\r\nPING\r\n$3\r\nkey\r\n";
        match s.decode_one(input) {
            DecodeOutcome::UnknownCommand { name } => assert_eq!(name, b"PING"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn wrong_arity_is_invalid() {
        let s = RespSerializer;
        // SET with only one bulk after verb → arity 2 instead of 3
        let input = b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n";
        assert!(matches!(
            s.decode_one(input),
            DecodeOutcome::Invalid { message: "SET arity" }
        ));
    }

    #[test]
    fn decode_kernel_encode_roundtrip() {
        let s = RespSerializer;
        let mut kernel = Kernel::new(MemoryStorageEngine::new());

        let DecodeOutcome::Ok {
            command: set_cmd, ..
        } = s.decode_one(SET_FIXTURE)
        else {
            panic!("set decode");
        };
        let set_resp = kernel.execute(&set_cmd);
        assert_eq!(s.encode(&set_resp), b"+OK\r\n");

        let DecodeOutcome::Ok {
            command: get_cmd, ..
        } = s.decode_one(GET_FIXTURE)
        else {
            panic!("get decode");
        };
        let get_resp = kernel.execute(&get_cmd);
        assert_eq!(s.encode(&get_resp), b"$5\r\nvalue\r\n");
    }
}
