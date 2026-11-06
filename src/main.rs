use std::{env, path::PathBuf, process};
#[cfg(any(feature = "capnp", feature = "grpc"))]
use tokio::net::TcpListener;

const DEFAULT_BIND: &str = "0.0.0.0:6380";
const DEFAULT_CAPNP_BIND: &str = "0.0.0.0:6381";
const DEFAULT_GRPC_BIND: &str = "0.0.0.0:6382";

#[derive(Debug)]
struct Config {
    protocol: String,
    bind: String,
    capnp_bind: String,
    grpc_bind: String,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let config = match resolve_config(env::args().skip(1).collect()) {
        Ok(cfg) => cfg,
        Err(code) => process::exit(code),
    };

    #[cfg(not(feature = "capnp"))]
    if matches!(config.protocol.as_str(), "capnp" | "both" | "all") {
        eprintln!(
            "janus: protocol '{}' requires a build with --features capnp",
            config.protocol,
        );
        process::exit(2);
    }
    #[cfg(not(feature = "grpc"))]
    if matches!(config.protocol.as_str(), "grpc" | "resp+grpc" | "all") {
        eprintln!(
            "janus: protocol '{}' requires a build with --features grpc",
            config.protocol,
        );
        process::exit(2);
    }
    #[cfg(not(any(feature = "capnp", feature = "grpc")))]
    let _ = (&config.capnp_bind, &config.grpc_bind);
    #[cfg(all(feature = "capnp", not(feature = "grpc")))]
    let _ = &config.grpc_bind;
    #[cfg(all(feature = "grpc", not(feature = "capnp")))]
    let _ = &config.capnp_bind;

    let result = match config.protocol.as_str() {
        "resp" => janus::listen(&config.bind, config.dbfile, config.wal).await,
        #[cfg(feature = "capnp")]
        "capnp" => match TcpListener::bind(&config.capnp_bind).await {
            Ok(listener) => {
                eprintln!("janus: Cap'n Proto RPC listening on {}", config.capnp_bind);
                janus::serve_with_capnp(listener, config.dbfile, config.wal).await
            }
            Err(error) => Err(error),
        },
        #[cfg(feature = "capnp")]
        "both" => {
            let resp_listener = bind_or_exit(&config.bind, "RESP").await;
            let capnp_listener = bind_or_exit(&config.capnp_bind, "Cap'n Proto RPC").await;
            eprintln!("janus: RESP listening on {}", config.bind);
            eprintln!("janus: Cap'n Proto RPC listening on {}", config.capnp_bind);
            janus::serve_with_protocols(resp_listener, capnp_listener, config.dbfile, config.wal)
                .await
        }
        #[cfg(feature = "grpc")]
        "grpc" => match TcpListener::bind(&config.grpc_bind).await {
            Ok(listener) => {
                eprintln!("janus: gRPC listening on {}", config.grpc_bind);
                janus::serve_with_grpc(listener, config.dbfile, config.wal).await
            }
            Err(error) => Err(error),
        },
        #[cfg(feature = "grpc")]
        "resp+grpc" => {
            let resp_listener = bind_or_exit(&config.bind, "RESP").await;
            let grpc_listener = bind_or_exit(&config.grpc_bind, "gRPC").await;
            eprintln!("janus: RESP listening on {}", config.bind);
            eprintln!("janus: gRPC listening on {}", config.grpc_bind);
            janus::serve_with_resp_and_grpc(
                resp_listener,
                grpc_listener,
                config.dbfile,
                config.wal,
            )
            .await
        }
        #[cfg(all(feature = "capnp", feature = "grpc"))]
        "all" => {
            let resp_listener = bind_or_exit(&config.bind, "RESP").await;
            let capnp_listener = bind_or_exit(&config.capnp_bind, "Cap'n Proto RPC").await;
            let grpc_listener = bind_or_exit(&config.grpc_bind, "gRPC").await;
            eprintln!("janus: RESP listening on {}", config.bind);
            eprintln!("janus: Cap'n Proto RPC listening on {}", config.capnp_bind);
            eprintln!("janus: gRPC listening on {}", config.grpc_bind);
            janus::serve_with_all_protocols(
                resp_listener,
                capnp_listener,
                grpc_listener,
                config.dbfile,
                config.wal,
            )
            .await
        }
        _ => unreachable!("protocol validated by resolve_config"),
    };

    if let Err(err) = result {
        eprintln!("janus: server failed: {err}");
        process::exit(1);
    }
}

#[cfg(any(feature = "capnp", feature = "grpc"))]
async fn bind_or_exit(address: &str, protocol: &str) -> TcpListener {
    match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("janus: failed to bind {protocol} listener on {address}: {error}");
            process::exit(1);
        }
    }
}

/// Resolve bind + optional persistence: CLI flags, else env, else defaults.
///
/// `--dbfile` / `JANUS_DBFILE` and `--wal` / `JANUS_WAL` cannot both be set
/// (`ERR conflicting persistence`).
fn resolve_config(args: Vec<String>) -> Result<Config, i32> {
    let mut protocol_from_cli: Option<String> = None;
    let mut bind_from_cli: Option<String> = None;
    let mut capnp_bind_from_cli: Option<String> = None;
    let mut grpc_bind_from_cli: Option<String> = None;
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
            "--protocol" => {
                let value = args.get(i + 1).cloned().ok_or_else(|| {
                    eprintln!("janus: --protocol requires resp, capnp, or both");
                    2
                })?;
                protocol_from_cli = Some(value);
                i += 2;
            }
            other if other.starts_with("--protocol=") => {
                protocol_from_cli = Some(other.trim_start_matches("--protocol=").to_string());
                i += 1;
            }
            "--capnp-bind" => {
                let value = args.get(i + 1).cloned().ok_or_else(|| {
                    eprintln!("janus: --capnp-bind requires an address (example: 0.0.0.0:6381)");
                    2
                })?;
                capnp_bind_from_cli = Some(value);
                i += 2;
            }
            other if other.starts_with("--capnp-bind=") => {
                capnp_bind_from_cli = Some(other.trim_start_matches("--capnp-bind=").to_string());
                i += 1;
            }
            "--grpc-bind" => {
                let value = args.get(i + 1).cloned().ok_or_else(|| {
                    eprintln!("janus: --grpc-bind requires an address (example: 0.0.0.0:6382)");
                    2
                })?;
                grpc_bind_from_cli = Some(value);
                i += 2;
            }
            other if other.starts_with("--grpc-bind=") => {
                grpc_bind_from_cli = Some(other.trim_start_matches("--grpc-bind=").to_string());
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

    let protocol = protocol_from_cli
        .or_else(|| env::var("JANUS_PROTOCOL").ok())
        .unwrap_or_else(|| "resp".to_string());
    if !matches!(
        protocol.as_str(),
        "resp" | "capnp" | "grpc" | "both" | "resp+grpc" | "all"
    ) {
        eprintln!("janus: invalid protocol '{protocol}'");
        return Err(2);
    }
    let capnp_bind = capnp_bind_from_cli
        .or_else(|| env::var("JANUS_CAPNP_BIND").ok())
        .unwrap_or_else(|| DEFAULT_CAPNP_BIND.to_string());
    let grpc_bind = grpc_bind_from_cli
        .or_else(|| env::var("JANUS_GRPC_BIND").ok())
        .unwrap_or_else(|| DEFAULT_GRPC_BIND.to_string());

    let dbfile = dbfile_from_cli.or_else(|| env::var("JANUS_DBFILE").ok().map(PathBuf::from));
    let wal = wal_from_cli.or_else(|| env::var("JANUS_WAL").ok().map(PathBuf::from));

    if dbfile.is_some() && wal.is_some() {
        eprintln!("janus: ERR conflicting persistence (--dbfile and --wal)");
        return Err(2);
    }

    Ok(Config {
        protocol,
        bind,
        capnp_bind,
        grpc_bind,
        dbfile,
        wal,
    })
}

fn print_help() {
    eprintln!(
        "\
Janus — modular data kernel

USAGE:
    janus [--protocol <resp|capnp|grpc|both|resp+grpc|all>] [--bind <ADDR>] [--capnp-bind <ADDR>] [--grpc-bind <ADDR>] [--dbfile <PATH> | --wal <PATH>]

OPTIONS:
    --bind <ADDR>     Listen address (default: {DEFAULT_BIND})
    --protocol <MODE> Listener protocol: resp (default), capnp, grpc, both (RESP + Cap'n Proto), resp+grpc, or all
    --capnp-bind <ADDR> Cap'n Proto RPC address (default: {DEFAULT_CAPNP_BIND})
    --grpc-bind <ADDR> gRPC address (default: {DEFAULT_GRPC_BIND})
    --dbfile <PATH>   Snapshot file for SAVE / boot load (optional)
    --wal <PATH>      Append-only WAL for mutation replay on boot (optional)
    -h, --help        Show help

ENVIRONMENT:
    JANUS_BIND        Listen address when --bind is not set
    JANUS_PROTOCOL    Listener protocol when --protocol is not set
    JANUS_CAPNP_BIND   Cap'n Proto RPC address when --capnp-bind is not set
    JANUS_GRPC_BIND    gRPC address when --grpc-bind is not set
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
        assert_eq!(cfg.protocol, "resp");
        assert_eq!(cfg.capnp_bind, DEFAULT_CAPNP_BIND);
        assert_eq!(cfg.grpc_bind, DEFAULT_GRPC_BIND);
        assert!(cfg.dbfile.is_none());
        assert!(cfg.wal.is_none());
    }

    #[test]
    fn resolve_config_accepts_capnp_modes_and_bind() {
        let cfg = resolve_config(vec![
            "--protocol=both".into(),
            "--capnp-bind".into(),
            "127.0.0.1:7001".into(),
        ])
        .unwrap();
        assert_eq!(cfg.protocol, "both");
        assert_eq!(cfg.capnp_bind, "127.0.0.1:7001");
    }

    #[test]
    fn resolve_config_accepts_grpc_modes_and_bind() {
        let cfg = resolve_config(vec![
            "--protocol=resp+grpc".into(),
            "--grpc-bind".into(),
            "127.0.0.1:7002".into(),
        ])
        .unwrap();
        assert_eq!(cfg.protocol, "resp+grpc");
        assert_eq!(cfg.grpc_bind, "127.0.0.1:7002");
    }

    #[test]
    fn resolve_config_rejects_unknown_protocol() {
        assert_eq!(resolve_config(vec!["--protocol=auto".into()]).unwrap_err(), 2);
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
