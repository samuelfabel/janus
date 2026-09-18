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
    },
    transport::tcp::instance::TcpInstance,
};

/// Bind and accept forever (one thread per connection, std::net).
pub fn listen(bind: &str, dbfile: Option<PathBuf>) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile)?));
    eprintln!("janus: listening on {bind}");
    accept_loop_with_kernel(listener, kernel)
}

/// Accept connections from an already-bound listener (empty in-memory store).
pub fn accept_loop(listener: TcpListener) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
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

fn build_kernel(dbfile: Option<PathBuf>) -> io::Result<Kernel<MemoryStorageEngine>> {
    let mut engine = MemoryStorageEngine::new();
    match dbfile {
        Some(path) => {
            let store = FileSnapshotStore::new(path);
            boot_load(&mut engine, &store)?;
            Ok(Kernel::with_store(engine, Box::new(store)))
        }
        None => Ok(Kernel::new(engine)),
    }
}

fn spawn_connection(stream: TcpStream, kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>) {
    let protocol = RespProtocol::shared(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}
