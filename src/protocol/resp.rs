///! This file contains the implementation of the RESP (REdis Serialization Protocol) protocol.
use crate::{kernel::kernel::Kernel, serializer::Serializer, storage::engine::StorageEngine};

/// RespProtocol implements the RESP (REdis Serialization Protocol) protocol for handling incoming messages and executing commands using the kernel.
pub struct RespProtocol<K, E, S>
where
    E: StorageEngine + Send + 'static,
    K: Kernel<E> + Send + 'static,
    S: Serializer,
{
    /// The kernel to use for executing commands    
    kernel: K,
    /// The serializer to use for encoding and decoding messages
    serializer: S,
}

impl<K, E, S> RespProtocol<K, E, S>
where
    E: StorageEngine + Send + 'static,
    K: Kernel<E> + Send + 'static,
    S: Serializer,
{
    /// * `kernel` - The kernel to use for executing commands
    /// * `serializer` - The serializer to use for encoding and decoding messages
    /// Handle incoming messages and execute commands using the kernel.
    ///
    /// # Arguments
    /// * `message` - A byte slice representing the incoming message
    /// * `on_response` - A closure that takes a byte slice representing the response
    ///
    /// # Returns
    /// The number of bytes consumed from the incoming message    
    fn handle(&mut self, message: &[u8], mut on_response: impl for<'a> FnMut(&'a [u8])) -> usize {
        let mut current = 0;

        for command in self.serializer.decode(message, &mut current) {
            let response = self.kernel.execute(&command);

            on_response(self.serializer.encode(&command, &response));
        }

        current
    }
}

impl<K, E, S> RespProtocol<K, E, S>
where
    E: StorageEngine + Send + 'static,
    K: Kernel<E> + Send + 'static,
    S: Serializer,
{
    /// Create a new instance of the protocol.
    ///
    /// # Arguments
    /// * `kernel` - The kernel to use for executing commands
    /// * `serializer` - The serializer to use for encoding and decoding messages
    ///
    /// # Returns
    /// A new instance of the protocol
    pub fn new(kernel: K, serializer: S) -> Self {
        RespProtocol { kernel, serializer }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        command::types::Command, kernel::kernel::MockKernel, protocol::resp::RespProtocol,
        serializer::MockSerializer, storage::engine::MockStorageEngine,
    };

    #[test]
    fn test_handle_single_complete_command() {
        // Arrange
        let storage_engine = MockStorageEngine::new();
        let kernel = MockKernel::new(storage_engine);
        let mut serializer = MockSerializer::new();

        serializer.expect_decode().returning(|_message, current| {
            *current = 34;

            let commands = vec![Command::Set {
                key: b"key",
                value: b"value",
            }];

            commands.into_iter()
        });

        kernel.expect_execute().returning(|command| {
            match command {
                Command::Set { key, value } => {
                    // Simula a execução do comando SET
                    Response::
                }
                _ => false,
            }
        });

        let mut protocol = RespProtocol { kernel, serializer };

        // Act

        // Assert

        let message = b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let mut responses = Vec::new();

        let bytes_consumed = protocol.handle(message, |response| {
            responses.push(response.to_vec());
        });

        assert_eq!(bytes_consumed, message.len());
        assert_eq!(responses.len(), 1);
    }
}
