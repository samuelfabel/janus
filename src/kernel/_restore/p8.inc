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
}
