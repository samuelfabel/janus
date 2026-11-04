//! TCP accept loop and per-connection handlers.

use std::{
    io,
    sync::{Arc, Mutex},
};

use janus_kernel::Kernel;
pub use janus_protocol::Protocol;
use janus_protocol::RespProtocol;
use janus_serializer::RespSerializer;
use tokio::net::TcpListener;

mod instance;

pub use instance::TcpInstance;

/// Accept forever on an already-bound Tokio listener, sharing one Kernel and
/// creating the default RESP protocol for each connection.
///
/// The Kernel arrives fully composed (storage, snapshot or WAL, passwords);
/// this layer only moves bytes between sockets and the protocol instance.
pub async fn accept_loop(listener: TcpListener, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    accept_loop_with_protocol(listener, kernel, |kernel| {
        RespProtocol::shared(kernel, RespSerializer)
    })
    .await
}

/// Accept forever and create one protocol instance for every TCP connection.
///
/// The factory receives a clone of the shared, fully composed Kernel. It is
/// called once per accepted connection, so connection-local protocol state
/// must be created inside the factory.
pub async fn accept_loop_with_protocol<P, F>(
    listener: TcpListener,
    kernel: Arc<Mutex<Kernel>>,
    factory: F,
) -> io::Result<()>
where
    P: Protocol + Send + 'static,
    F: Fn(Arc<Mutex<Kernel>>) -> P + Send + 'static,
{
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let protocol = factory(Arc::clone(&kernel));
                TcpInstance::spawn(stream, protocol);
            }
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
}
