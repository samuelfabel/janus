use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
};

use crate::protocol::Protocol;

pub struct TcpInstance<T: Protocol + Send + 'static> {
    instance: TcpStream,
    protocol: T,
    buffer: [u8; 512],
}

impl<T: Protocol + Send + 'static> TcpInstance<T> {
    pub fn new(instance: TcpStream, protocol: T) {
        let mut tcp_instance = TcpInstance {
            instance,
            protocol,
            buffer: [0; 512],
        };

        thread::spawn(move || tcp_instance.run());
    }

    pub fn run(&mut self) {
        let mut leftover = 0;

        loop {
            match self.instance.read(&mut self.buffer[leftover..]) {
                Ok(0) => break,
                Ok(bytes_read) => {
                    let total_available = leftover + bytes_read;

                    match self.read(total_available) {
                        Ok(consumed) => {
                            if consumed < total_available {
                                leftover = total_available - consumed;
                                self.buffer.copy_within(consumed..total_available, 0);
                            } else {
                                leftover = 0;
                            }
                        }
                        Err(()) => break,
                    }
                }
                Err(_) => break,
            };
        }
    }

    pub fn read(&mut self, bytes_read: usize) -> Result<usize, ()> {
        if bytes_read == 0 {
            return Ok(0);
        }

        let protocol = &mut self.protocol;
        let stream = &mut self.instance;
        let chunk = &self.buffer[..bytes_read];

        protocol
            .execute(chunk, |response| {
                let _ = stream.write_all(response);
            })
            .map_err(|_| ())
    }
}
