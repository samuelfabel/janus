mod command;
mod kernel;
mod response;
mod storage;

use std::env;
use std::net::TcpListener;
use std::process;

const DEFAULT_BIND: &str = "0.0.0.0:6380";

fn main() {
    let bind = match resolve_bind(env::args().skip(1).collect()) {
        Ok(addr) => addr,
        Err(code) => process::exit(code),
    };

    let listener = TcpListener::bind(&bind).unwrap_or_else(|err| {
        eprintln!("janus: failed to bind {bind}: {err}");
        process::exit(1);
    });

    // Listen stub until the full TCP+RESP pipeline lands (see project roadmap).
    eprintln!("janus: listening on {bind}");

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                // Accept and close. Protocol handling is intentionally out of scope here.
                drop(stream);
            }
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
}

/// Resolve bind address: `--bind <addr>`, else `JANUS_BIND`, else default.
fn resolve_bind(args: Vec<String>) -> Result<String, i32> {
    let mut bind_from_cli: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => {
                print_help();
                return Err(0);
            }
            "--bind" => {
                let value = args.get(i + 1).cloned().ok_or_else(|| {
                    eprintln!("janus: --bind requires an address (example: 0.0.0.0:6380)");
                    2
                })?;
                bind_from_cli = Some(value);
                i += 2;
            }
            other if other.starts_with("--bind=") => {
                bind_from_cli = Some(other.trim_start_matches("--bind=").to_string());
                i += 1;
            }
            other => {
                eprintln!("janus: unknown argument: {other}");
                print_help();
                return Err(2);
            }
        }
    }

    if let Some(addr) = bind_from_cli {
        return Ok(addr);
    }

    Ok(env::var("JANUS_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string()))
}

fn print_help() {
    eprintln!(
        "\
Janus — modular data kernel (listen stub)

USAGE:
    janus [--bind <ADDR>]

OPTIONS:
    --bind <ADDR>   Listen address (default: {DEFAULT_BIND})
    -h, --help      Show help

ENVIRONMENT:
    JANUS_BIND      Listen address when --bind is not set
"
    );
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn resolve_bind_uses_cli_over_default() {
        let addr = resolve_bind(vec!["--bind".into(), "127.0.0.1:7000".into()]).unwrap();
        assert_eq!(addr, "127.0.0.1:7000");
    }

    #[test]
    fn resolve_bind_supports_equals_form() {
        let addr = resolve_bind(vec!["--bind=127.0.0.1:7001".into()]).unwrap();
        assert_eq!(addr, "127.0.0.1:7001");
    }

    #[test]
    fn resolve_bind_help_exits_zero() {
        assert_eq!(resolve_bind(vec!["--help".into()]), Err(0));
    }
}
