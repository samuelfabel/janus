SETs the buffer holds ids 2..=65. TRACES (id 66) drops id 2.
        let traces = kernel.execute(&Command::Traces);
        let Response::Value(Some(payload)) = traces else {
            panic!("expected Value payload, got {traces:?}");
        };
        let text = String::from_utf8(payload).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "# Traces");
        assert!(!lines.iter().any(|l| *l == "1 SET"), "oldest dropped: {text}");
        assert!(!lines.iter().any(|l| *l == "2 SET"), "id 2 dropped by TRACES: {text}");
        assert!(lines.iter().any(|l| *l == "3 SET"), "expected id 3: {text}");
        assert!(
            lines
                .iter()
                .any(|l| *l == format!("{} SET", TRACE_CAPACITY + 1)),
            "expected newest SET: {text}"
        );
        assert!(
            lines
                .iter()
                .any(|l| *l == format!("{} TRACES", TRACE_CAPACITY + 2)),
            "expected TRACES: {text}"
        );
        let span_lines = lines.iter().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(span_lines, TRACE_CAPACITY);
    }
}
