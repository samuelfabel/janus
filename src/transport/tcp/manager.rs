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

/// Accept TCP connections and spawn one handler thread per connection.
pub fn listen(bind: &str) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    eprintln!("janus: listening on {bind}");

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
    TcpInstance::new(stream, protocol);
}
