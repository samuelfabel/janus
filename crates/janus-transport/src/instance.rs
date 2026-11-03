//! One TCP connection: read → append → protocol.execute → write → compact.

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use janus_protocol::Protocol;

/// Per-connection transport state (bytes only; no RESP knowledge).
pub struct TcpInstance<T: Protocol + Send + 'static> {
    stream: TcpStream,
    protocol: T,
    buffer: Vec<u8>,
}
impl<T: Protocol + Send + 'static> TcpInstance<T> {
    /// Spawn a Tokio task that owns this connection until EOF or error.
    ///
    /// Must be called from within a Tokio runtime (no `std::thread::spawn`).
    pub fn spawn(stream: TcpStream, protocol: T) {
        let mut instance = TcpInstance {
            stream,
            protocol,
            buffer: Vec::new(),
        };
        tokio::spawn(async move {
            instance.run().await;
        });
    }

    /// Drive the connection until the peer closes or a fatal error occurs.
    pub async fn run(&mut self) {
        let mut read_buf = [0u8; 4096];

        loop {
            match self.stream.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => {
                    self.buffer.extend_from_slice(&read_buf[..n]);
                    if self.process().await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    /// Normative transport step: execute (sync) + async writes + compact.
    ///
    /// Responses are collected during `execute` so the Kernel mutex guard
    /// (inside Protocol) does not cross an `.await`.
    async fn process(&mut self) -> Result<(), ()> {
        let mut responses: Vec<Vec<u8>> = Vec::new();
        let offset = match self.protocol.execute(&self.buffer, |response| {
            responses.push(response.to_vec());
        }) {
            Ok(offset) => offset,
            Err(_) => return Err(()),
        };

        for response in &responses {
            if self.stream.write_all(response).await.is_err() {
                return Err(());
            }
        }

        self.buffer.drain(..offset);
        Ok(())
    }
}
