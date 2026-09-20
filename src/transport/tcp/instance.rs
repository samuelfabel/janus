//! One TCP connection: read → append → protocol.execute → write → compact.

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::protocol::Protocol;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kernel::kernel::Kernel, protocol::resp::RespProtocol, serializer::resp::RespSerializer,
        storage::memory::MemoryStorageEngine,
    };
    use tokio::net::TcpListener;

    const SET_KEY_VALUE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    #[tokio::test]
    async fn fragmented_frame_two_reads_completes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let protocol =
                RespProtocol::new(Kernel::new(MemoryStorageEngine::new()), RespSerializer);
            let mut instance = TcpInstance {
                stream,
                protocol,
                buffer: Vec::new(),
            };
            instance.run().await;
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let split = 12;
        client
            .write_all(&SET_KEY_VALUE[..split])
            .await
            .expect("write1");
        // Force a separate server read boundary without requiring tokio `time`.
        std::thread::sleep(std::time::Duration::from_millis(30));
        client
            .write_all(&SET_KEY_VALUE[split..])
            .await
            .expect("write2");

        let mut ok = [0u8; 5];
        client.read_exact(&mut ok).await.expect("read ok");
        assert_eq!(&ok, b"+OK\r\n");

        drop(client);
        let _ = server.await;
    }

    #[tokio::test]
    async fn eof_ends_run_without_panic() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let protocol =
                RespProtocol::new(Kernel::new(MemoryStorageEngine::new()), RespSerializer);
            let mut instance = TcpInstance {
                stream,
                protocol,
                buffer: Vec::new(),
            };
            instance.run().await;
        });

        let client = TcpStream::connect(addr).await.expect("connect");
        drop(client);
        server.await.expect("server");
    }
}
