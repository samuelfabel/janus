        let text = String::from_utf8(payload).expect("utf8");
        // Multi + Exec + applied Set + Info = 4
        assert!(
            text.contains("total_commands:4"),
            "unexpected total in {text}"
        );
    }
}
