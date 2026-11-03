//! TCP accept loop and per-connection handlers.

use std::{
    io,
    sync::{Arc, Mutex},
};

use janus_kernel::Kernel;
use janus_protocol::RespProtocol;
use janus_serializer::RespSerializer;
use tokio::net::TcpListener;

mod instance;

pub use instance::TcpInstance;

/// Accept forever on an already-bound Tokio listener, sharing one Kernel.
///
/// The Kernel arrives fully composed (storage, snapshot or WAL, passwords);
/// this layer only moves bytes between sockets and the protocol instance.
pub async fn accept_loop(listener: TcpListener, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => spawn_connection(stream, Arc::clone(&kernel)),
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
}

fn spawn_connection(stream: tokio::net::TcpStream, kernel: Arc<Mutex<Kernel>>) {
    let protocol = RespProtocol::shared(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}
