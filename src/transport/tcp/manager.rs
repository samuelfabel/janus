//! TCP accept loop and per-connection handlers.

use std::{
    io,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex},
};

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

/// Bind and accept forever (one thread per connection, std::net).
///
/// `dbfile` and `wal` are mutually exclusive (checked by CLI). Corrupt WAL
/// fails startup with an I/O error.
pub fn listen(
    bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    eprintln!("janus: listening on {bind}");
    accept_loop_with_kernel(listener, kernel)
}

/// Accept connections from an already-bound listener (empty in-memory store).
pub fn accept_loop(listener: TcpListener) -> io::Result<()> {
    accept_loop_with_dbfile(listener, None)
}

/// Accept connections with an optional snapshot path (boot load + SAVE).
pub fn accept_loop_with_dbfile(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, None)?));
    accept_loop_with_kernel(listener, kernel)
}

/// Accept connections with an optional WAL path (boot replay + append).
pub fn accept_loop_with_wal(listener: TcpListener, wal: Option<PathBuf>) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(None, wal)?));
    accept_loop_with_kernel(listener, kernel)
}

fn accept_loop_with_kernel(
    listener: TcpListener,
    kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>,
) -> io::Result<()> {
    for connection in listener.incoming() {
        match connection {
            Ok(stream) => spawn_connection(stream, Arc::clone(&kernel)),
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
    Ok(())
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

fn spawn_connection(stream: TcpStream, kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>) {
    let protocol = RespProtocol::shared(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}
