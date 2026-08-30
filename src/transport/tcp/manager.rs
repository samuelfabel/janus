//! TCP accept loop and per-connection handlers.

use std::{
    io,
    net::{TcpListener, TcpStream},
};

use crate::{
    kernel::kernel::Kernel,
    protocol::resp::RespProtocol,
    serializer::resp::RespSerializer,
    storage::memory::MemoryStorageEngine,
    transport::tcp::instance::TcpInstance,
};

/// Bind and accept forever (one thread per connection, std::net).
pub fn listen(bind: &str) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    eprintln!("janus: listening on {bind}");
    accept_loop(listener)
}

/// Accept connections from an already-bound listener.
pub fn accept_loop(listener: TcpListener) -> io::Result<()> {
    for connection in listener.incoming() {
        match connection {
            Ok(stream) => spawn_connection(stream),
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
    Ok(())
}

fn spawn_connection(stream: TcpStream) {
    let kernel = Kernel::new(MemoryStorageEngine::new());
    let protocol = RespProtocol::new(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}
