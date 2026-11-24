//! Composition root: pick the storage plugin, boot persistence, build the
//! Kernel and hand it to the TCP transport (and optional replica wire).

use std::{
    io,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use janus_http3::quic::Endpoint;
use janus_kernel::{replication::ReplicationSink, Kernel};
use janus_protocol::HttpKvProtocol;
use janus_replication::TcpReplicationSink;
use janus_storage::{
    store::{boot_load, FileSnapshotStore},
    wal::boot_wal,
    MemoryStorageEngine, StorageEngine,
};
use tokio::{net::TcpListener, sync::watch};
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

/// Build a primary Kernel that emits mutations to `sink` after local apply.
pub fn build_primary_kernel(
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
    sink: Box<dyn ReplicationSink>,
) -> io::Result<Kernel> {
    Ok(build_kernel(dbfile, wal)?.with_replication(sink))
}

/// Connect a [`TcpReplicationSink`] to `replica_peer` and serve RESP forever.
pub async fn listen_primary(
    bind: &str,
    replica_peer: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    serve_primary(listener, replica_peer, dbfile, wal).await
}

/// Serve RESP as a primary with a TCP replica sink already connecting to `replica_peer`.
///
/// On connect, the primary performs a full-sync handoff (`bootstrap_replica`):
/// domain snapshot over the replica wire, then incremental records. Local SAVE
/// / `--dbfile` is not required for that path.
pub async fn serve_primary(
    listener: TcpListener,
    replica_peer: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let peer = replica_peer.to_string();
    let sink = tokio::task::spawn_blocking(move || TcpReplicationSink::connect(peer))
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))??;
    let mut primary = build_primary_kernel(dbfile, wal, Box::new(sink))?;
    primary
        .bootstrap_replica()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "ERR replication bootstrap failed"))?;
    let kernel = Arc::new(Mutex::new(primary));
    janus_transport::accept_loop(listener, kernel).await
}

/// Bind RESP and the replica channel; apply framed records on the replica Kernel.
pub async fn listen_replica(
    bind: &str,
    replica_bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let resp_listener = TcpListener::bind(bind).await?;
    let replica_listener = TcpListener::bind(replica_bind).await?;
    serve_replica(resp_listener, replica_listener, dbfile, wal).await
}

/// Serve RESP for clients and accept the dedicated replica wire on separate sockets.
///
/// Client writes are denied until [`promote_ha_replica`] (or an equivalent
/// supervisor call) sets `accepts_writes`. Replica-wire apply still works.
pub async fn serve_replica(
    resp_listener: TcpListener,
    replica_listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let (_handles, fut) = serve_replica_ha(resp_listener, replica_listener, dbfile, wal)?;
    fut.await
}

/// Handles for an HA-capable replica: stop stream apply and flip write role.
pub struct HaReplicaHandles {
    /// Shared Kernel (RESP + replica wire).
    pub kernel: Arc<Mutex<Kernel>>,
    /// When false, replica sessions stop applying remote frames.
    pub apply_stream: Arc<AtomicBool>,
    /// Signal the replica accept loop to exit (after promotion).
    pub stop_apply: watch::Sender<bool>,
}

/// Build a replica that can be promoted by the failover supervisor.
///
/// Returns handles plus a future that runs RESP + cancellable replica wire.
/// After promotion stops the wire loop, RESP keeps serving as the new primary.
pub fn serve_replica_ha(
    resp_listener: TcpListener,
    replica_listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<(
    HaReplicaHandles,
    impl std::future::Future<Output = io::Result<()>>,
)> {
    let mut kernel = build_kernel(dbfile, wal)?;
    kernel.set_accepts_writes(false);
    let kernel = Arc::new(Mutex::new(kernel));
    let apply_stream = Arc::new(AtomicBool::new(true));
    let (stop_tx, stop_rx) = watch::channel(false);
    let handles = HaReplicaHandles {
        kernel: Arc::clone(&kernel),
        apply_stream: Arc::clone(&apply_stream),
        stop_apply: stop_tx,
    };
    let fut = async move {
        let resp = janus_transport::accept_loop(resp_listener, Arc::clone(&kernel));
        let replica = janus_replication::accept_loop_until(
            replica_listener,
            kernel,
            apply_stream,
            stop_rx,
        );
        tokio::pin!(resp);
        tokio::pin!(replica);
        tokio::select! {
            result = &mut replica => {
                result?;
                // Wire apply stopped (promotion): keep RESP as the writable primary.
                resp.await
            }
            result = &mut resp => result,
        }
    };
    Ok((handles, fut))
}

/// Promote a replica built with [`serve_replica_ha`]: stop stream apply, accept
/// writes, publish endpoint on `authority`.
pub fn promote_ha_replica(
    handles: &HaReplicaHandles,
    authority: &janus_replication::ConfigAuthority,
    new_primary_endpoint: impl Into<String>,
) -> io::Result<()> {
    janus_replication::promote_replica(
        &handles.kernel,
        &handles.apply_stream,
        &handles.stop_apply,
        authority,
        new_primary_endpoint,
    )
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

/// Accept REST requests on an already-bound listener.
#[cfg(feature = "rest")]
pub async fn serve_with_rest(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    janus_rest::accept_loop(listener, kernel).await
}

/// Serve RESP and REST on separate TCP listeners with one Kernel.
#[cfg(feature = "rest")]
pub async fn serve_with_resp_and_rest(
    resp_listener: TcpListener,
    rest_listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    let resp = janus_transport::accept_loop(resp_listener, Arc::clone(&kernel));
    let rest = janus_rest::accept_loop(rest_listener, kernel);
    tokio::try_join!(resp, rest)?;
    Ok(())
}

/// Accept Cap'n Proto RPC sessions on an already-bound TCP listener.
///
/// This API is available when the crate is built with the `capnp` feature.
/// RESP remains the default listener and both protocols may be composed with
/// [`serve_with_protocols`].
#[cfg(feature = "capnp")]
pub async fn serve_with_capnp(
    listener: TcpListener,
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
    capnp_listener: TcpListener,
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

/// Accept gRPC sessions on an already-bound TCP listener.
#[cfg(feature = "grpc")]
pub async fn serve_with_grpc(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    janus_grpc::rpc::accept_loop(listener, kernel).await
}

/// Serve RESP and gRPC on separate TCP listeners with one Kernel.
#[cfg(feature = "grpc")]
pub async fn serve_with_resp_and_grpc(
    resp_listener: TcpListener,
    grpc_listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    let resp = janus_transport::accept_loop(resp_listener, Arc::clone(&kernel));
    let grpc = janus_grpc::rpc::accept_loop(grpc_listener, kernel);
    tokio::try_join!(resp, grpc)?;
    Ok(())
}

/// Serve RESP, Cap'n Proto RPC and gRPC on separate listeners with one Kernel.
#[cfg(all(feature = "capnp", feature = "grpc", feature = "rest"))]
pub async fn serve_with_all_protocols(
    resp_listener: TcpListener,
    capnp_listener: TcpListener,
    grpc_listener: TcpListener,
    rest_listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    LocalSet::new()
        .run_until(async move {
            let resp = janus_transport::accept_loop(resp_listener, Arc::clone(&kernel));
            let capnp = janus_capnp_rpc::accept_loop(capnp_listener, Arc::clone(&kernel));
            let grpc = janus_grpc::rpc::accept_loop(grpc_listener, Arc::clone(&kernel));
            let rest = janus_rest::accept_loop(rest_listener, kernel);
            tokio::try_join!(resp, capnp, grpc, rest)?;
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
