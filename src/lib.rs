//! Composition root: pick the storage plugin, boot persistence, build the
//! Kernel and hand it to the TCP transport.

use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use janus_http3::quic::Endpoint;
use janus_kernel::Kernel;
use janus_protocol::HttpKvProtocol;
use janus_storage::{
    store::{boot_load, FileSnapshotStore},
    wal::boot_wal,
    MemoryStorageEngine, StorageEngine,
};
use tokio::net::TcpListener;
#[cfg(feature = "capnp")]
use tokio::net::TcpListener as CapnpTcpListener;
#[cfg(feature = "capnp")]
use tokio::task::LocalSet;

/// Default production storage plugin (`MemoryStorageEngine`).
///
/// Tests may substitute `BTreeStorageEngine` (or another implementor) by
/// building the Kernel directly.
pub fn build_storage() -> Box<dyn StorageEngine> {
    Box::new(MemoryStorageEngine::new())
}

/// Inject [`build_storage`] into the Kernel and boot the optional persistence.
///
/// `dbfile` and `wal` are mutually exclusive (checked by CLI). Corrupt WAL
/// fails with an I/O error.
pub fn build_kernel(dbfile: Option<PathBuf>, wal: Option<PathBuf>) -> io::Result<Kernel> {
    let mut engine = build_storage();
    match (dbfile, wal) {
        (Some(_), Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ERR conflicting persistence",
        )),
        (None, Some(path)) => {
            let writer = boot_wal(&path, engine.as_mut())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            Ok(Kernel::from_boxed_with_wal(engine, writer))
        }
        (Some(path), None) => {
            let store = FileSnapshotStore::new(path);
            boot_load(engine.as_mut(), &store)?;
            Ok(Kernel::from_boxed_with_store(engine, Box::new(store)))
        }
        (None, None) => Ok(Kernel::from_boxed(engine)),
    }
}

/// Bind and accept forever (Tokio tasks per connection).
pub async fn listen(bind: &str, dbfile: Option<PathBuf>, wal: Option<PathBuf>) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    eprintln!("janus: listening on {bind}");
    janus_transport::accept_loop(listener, kernel).await
}

/// Accept forever on an already-bound Tokio listener (e2e / tests).
pub async fn serve(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    janus_transport::accept_loop(listener, kernel).await
}

/// Bind and accept forever with a caller-provided protocol factory.
///
/// The factory is called once for each TCP connection and receives the shared
/// Kernel. The default [`listen`] entry point continues to use RESP.
pub async fn listen_with_protocol<P, F>(
    bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
    factory: F,
) -> io::Result<()>
where
    P: janus_transport::Protocol + Send + 'static,
    F: Fn(Arc<Mutex<Kernel>>) -> P + Send + 'static,
{
    let listener = TcpListener::bind(bind).await?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    eprintln!("janus: listening on {bind}");
    janus_transport::accept_loop_with_protocol(listener, kernel, factory).await
}

/// Accept on an already-bound listener with a caller-provided protocol
/// factory. The storage and persistence composition matches [`serve`].
pub async fn serve_with_protocol<P, F>(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
    factory: F,
) -> io::Result<()>
where
    P: janus_transport::Protocol + Send + 'static,
    F: Fn(Arc<Mutex<Kernel>>) -> P + Send + 'static,
{
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    janus_transport::accept_loop_with_protocol(listener, kernel, factory).await
}

/// Serve RESP over TCP and HTTP/3 over a caller-configured QUIC endpoint.
/// Both interfaces share the same Kernel; HTTP/3 remains opt-in and library-only.
pub async fn serve_with_http3(
    tcp_listener: TcpListener,
    endpoint: Endpoint,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    let tcp = janus_transport::accept_loop(tcp_listener, Arc::clone(&kernel));
    let http3 = janus_http3::accept_loop(endpoint, move || {
        HttpKvProtocol::shared(Arc::clone(&kernel))
    });
    tokio::try_join!(tcp, http3)?;
    Ok(())
}

/// Accept Cap'n Proto RPC sessions on an already-bound TCP listener.
///
/// This API is available when the crate is built with the `capnp` feature.
/// RESP remains the default listener and both protocols may be composed with
/// [`serve_with_protocols`].
#[cfg(feature = "capnp")]
pub async fn serve_with_capnp(
    listener: CapnpTcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    LocalSet::new()
        .run_until(janus_capnp_rpc::accept_loop(listener, kernel))
        .await
}

/// Serve RESP and Cap'n Proto RPC on separate TCP listeners with one Kernel.
#[cfg(feature = "capnp")]
pub async fn serve_with_protocols(
    resp_listener: TcpListener,
    capnp_listener: CapnpTcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    LocalSet::new()
        .run_until(async move {
            let resp = janus_transport::accept_loop(resp_listener, Arc::clone(&kernel));
            let capnp = janus_capnp_rpc::accept_loop(capnp_listener, kernel);
            tokio::try_join!(resp, capnp)?;
            Ok(())
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use janus_types::{Command, Response};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[test]
    fn build_storage_default_supports_set_get() {
        let mut kernel = Kernel::from_boxed(build_storage());
        assert_eq!(
            kernel.execute(&Command::Set {
                key: b"k",
                value: b"v",
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"k" }),
            Response::Value(Some(b"v".to_vec()))
        );
    }

    #[tokio::test]
    async fn listen_accepts_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let _ = serve(listener, None, None).await;
        });

        let mut client = TcpStream::connect(&addr).await.expect("connect");
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .await
            .expect("write");
        let mut ok = [0u8; 5];
        client.read_exact(&mut ok).await.expect("read");
        assert_eq!(&ok, b"+OK\r\n");
    }
}
