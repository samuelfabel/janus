//! TCP accept loop and per-connection handlers.

use std::{
    io,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::net::TcpListener;

use crate::{
    kernel::kernel::Kernel,
    protocol::resp::RespProtocol,
    serializer::resp::RespSerializer,
    storage::{
        memory::MemoryStorageEngine,
        store::{FileSnapshotStore, boot_load},
        wal::boot_wal,
    },
    transport::tcp::instance::TcpInstance,
};

/// Bind and accept forever (Tokio tasks per connection).
///
/// Bridge until F6-02 owns `#[tokio::main]`: builds a multi-thread runtime and
/// `block_on`s the async accept loop. `dbfile` and `wal` are mutually exclusive
/// (checked by CLI). Corrupt WAL fails startup with an I/O error.
pub fn listen(
    bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(listen_async(bind, dbfile, wal))
}

async fn listen_async(
    bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    eprintln!("janus: listening on {bind}");
    accept_loop_with_kernel(listener, kernel).await
}

/// Accept connections from an already-bound std listener (empty in-memory store).
///
/// Used by e2e harness: converts to Tokio listener inside a dedicated runtime.
pub fn accept_loop(listener: StdTcpListener) -> io::Result<()> {
    accept_loop_with_dbfile(listener, None)
}

/// Accept connections with an optional snapshot path (boot load + SAVE).
pub fn accept_loop_with_dbfile(
    listener: StdTcpListener,
    dbfile: Option<PathBuf>,
) -> io::Result<()> {
    accept_loop_std(listener, dbfile, None)
}

/// Accept connections with an optional WAL path (boot replay + append).
pub fn accept_loop_with_wal(listener: StdTcpListener, wal: Option<PathBuf>) -> io::Result<()> {
    accept_loop_std(listener, None, wal)
}

fn accept_loop_std(
    listener: StdTcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let listener = TcpListener::from_std(listener)?;
        let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
        accept_loop_with_kernel(listener, kernel).await
    })
}

async fn accept_loop_with_kernel(
    listener: TcpListener,
    kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>,
) -> io::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => spawn_connection(stream, Arc::clone(&kernel)),
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
}

fn build_kernel(
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<Kernel<MemoryStorageEngine>> {
    let mut engine = MemoryStorageEngine::new();
    match (dbfile, wal) {
        (Some(_), Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ERR conflicting persistence",
        )),
        (None, Some(path)) => {
            let writer = boot_wal(&path, &mut engine)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            Ok(Kernel::with_wal(engine, writer))
        }
        (Some(path), None) => {
            let store = FileSnapshotStore::new(path);
            boot_load(&mut engine, &store)?;
            Ok(Kernel::with_store(engine, Box::new(store)))
        }
        (None, None) => Ok(Kernel::new(engine)),
    }
}

fn spawn_connection(
    stream: tokio::net::TcpStream,
    kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>,
) {
    let protocol = RespProtocol::shared(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}
