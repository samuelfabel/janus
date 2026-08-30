//! One TCP connection: read → append → protocol.execute → write → compact.

use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
};

use crate::protocol::Protocol;

/// Per-connection transport state (bytes only; no RESP knowledge).
pub struct TcpInstance<T: Protocol + Send + 'static> {
    stream: TcpStream,
    protocol: T,
    buffer: Vec<u8>,
}

impl<T: Protocol + Send + 'static> TcpInstance<T> {
    /// Spawn a dedicated thread that owns this connection until EOF or error.
    pub fn spawn(stream: TcpStream, protocol: T) {
        let mut instance = TcpInstance {
            stream,
            protocol,
            buffer: Vec::new(),
        };
        thread::spawn(move || instance.run());
    }

    /// Drive the connection until the peer closes or a fatal error occurs.
    pub fn run(&mut self) {
        let mut read_buf = [0u8; 4096];

        loop {
            match self.stream.read(&mut read_buf) {
                Ok(0) => break,
                Ok(n) => {
                    self.buffer.extend_from_slice(&read_buf[..n]);
                    if self.process().is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    /// Normative transport step: execute + compact; write failure closes.
    fn process(&mut self) -> Result<(), ()> {
        let mut write_ok = true;
        let offset = {
            let Self {
                protocol,
                stream,
                buffer,
            } = self;
            match protocol.execute(buffer, |response| {
                if stream.write_all(response).is_err() {
                    write_ok = false;
                }
            }) {
                Ok(offset) => offset,
                Err(_) => return Err(()),
            }
        };

        if !write_ok {
            return Err(());
        }

        self.buffer.drain(..offset);
        Ok(())
    }
}
