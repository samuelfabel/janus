mod command;
mod kernel;
mod protocol;
pub mod response;
mod serializer;
pub mod shared;
pub mod storage;
mod transport;

use std::{env, path::PathBuf, process};

use transport::tcp::manager;

const DEFAULT_BIND: &str = "0.0.0.0:6380";

#[derive(Debug)]
struct Config {
    bind: String,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
}

fn main() {
    let config = match resolve_config(env::args().skip(1).collect()) {
        Ok(cfg) => cfg,
        Err(code) => process::exit(code),
    };

    if let Err(err) = manager::listen(&config.bind, config.dbfile, config.wal) {
        eprintln!("janus: failed to listen on {}: {err}", config.bind);
        process::exit(1);
    }
}

/// Resolve bind + optional persistence: CLI flags, else env, else defaults.
///
/// `--dbfile` / `JANUS_DBFILE` and `--wal` / `JANUS_WAL` cannot both be set
/// (`ERR conflicting persistence`).
fn resolve_config(args: Vec<String>) -> Result<Config, i32> {
    let mut bind_from_cli: Option<String> = None;
    let mut dbfile_from_cli: Option<PathBuf> = None;
    let mut wal_from_cli: Option<PathBuf> = None;
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
            "--dbfile" => {
                let value = args.get(i + 1).cloned().ok_or_else(|| {
                    eprintln!("janus: --dbfile requires a path");
                    2
                })?;
                dbfile_from_cli = Some(PathBuf::from(value));
                i += 2;
            }
            other if other.starts_with("--dbfile=") => {
                dbfile_from_cli = Some(PathBuf::from(other.trim_start_matches("--dbfile=")));
                i += 1;
            }
            "--wal" => {
                let value = args.get(i + 1).cloned().ok_or_else(|| {
                    eprintln!("janus: --wal requires a path");
                    2
                })?;
                wal_from_cli = Some(PathBuf::from(value));
                i += 2;
            }
            other if other.starts_with("--wal=") => {
                wal_from_cli = Some(PathBuf::from(other.trim_start_matches("--wal=")));
                i += 1;
            }
            other => {
                eprintln!("janus: unknown argument: {other}");
                print_help();
                return Err(2);
            }
        }
    }

    let bind = bind_from_cli
        .or_else(|| env::var("JANUS_BIND").ok())
        .unwrap_or_else(|| DEFAULT_BIND.to_string());

    let dbfile = dbfile_from_cli.or_else(|| env::var("JANUS_DBFILE").ok().map(PathBuf::from));
    let wal = wal_from_cli.or_else(|| env::var("JANUS_WAL").ok().map(PathBuf::from));

    if dbfile.is_some() && wal.is_some() {
        eprintln!("janus: ERR conflicting persistence (--dbfile and --wal)");
        return Err(2);
    }

    Ok(Config {
        bind,
        dbfile,
        wal,
    })
}

fn print_help() {
    eprintln!(
        "\
Janus — modular data kernel

USAGE:
    janus [--bind <ADDR>] [--dbfile <PATH> | --wal <PATH>]

OPTIONS:
    --bind <ADDR>     Listen address (default: {DEFAULT_BIND})
    --dbfile <PATH>   Snapshot file for SAVE / boot load (optional)
    --wal <PATH>      Append-only WAL for mutation replay on boot (optional)
    -h, --help        Show help

ENVIRONMENT:
    JANUS_BIND        Listen address when --bind is not set
    JANUS_DBFILE      Snapshot path when --dbfile is not set
    JANUS_WAL         WAL path when --wal is not set

`--dbfile` and `--wal` are mutually exclusive (ERR conflicting persistence).
Without either, persistence is disabled and SAVE returns an error.
"
    );
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn resolve_config_uses_cli_bind_over_default() {
        let cfg = resolve_config(vec!["--bind".into(), "127.0.0.1:7000".into()]).unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:7000");
        assert!(cfg.dbfile.is_none());
        assert!(cfg.wal.is_none());
    }

    #[test]
    fn resolve_config_supports_equals_forms() {
        let cfg = resolve_config(vec![
            "--bind=127.0.0.1:7001".into(),
            "--dbfile=/tmp/janus.snap".into(),
        ])
        .unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:7001");
        assert_eq!(cfg.dbfile.as_deref(), Some(std::path::Path::new("/tmp/janus.snap")));
        assert!(cfg.wal.is_none());
    }

    #[test]
    fn resolve_config_supports_wal() {
        let cfg = resolve_config(vec!["--wal=/tmp/janus.wal".into()]).unwrap();
        assert_eq!(cfg.wal.as_deref(), Some(std::path::Path::new("/tmp/janus.wal")));
        assert!(cfg.dbfile.is_none());
    }

    #[test]
    fn resolve_config_rejects_dbfile_and_wal() {
        let err = resolve_config(vec![
            "--dbfile=/tmp/janus.snap".into(),
            "--wal=/tmp/janus.wal".into(),
        ])
        .unwrap_err();
        assert_eq!(err, 2);
    }

    #[test]
    fn resolve_config_help_exits_zero() {
        assert_eq!(resolve_config(vec!["--help".into()]).unwrap_err(), 0);
    }
}
