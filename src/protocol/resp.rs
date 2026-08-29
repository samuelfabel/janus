//! RESP protocol instance: decode → kernel → encode → callback.
use crate::{
    kernel::kernel::Kernel, protocol::Protocol, serializer::Serializer,
    storage::engine::StorageEngine,
};

/// RespProtocol handles incoming RESP messages using a kernel and serializer.
pub struct RespProtocol<E, S>
where
    E: StorageEngine + Send + 'static,
    S: Serializer,
{
    kernel: Kernel<E>,
    serializer: S,
}

impl<E, S> RespProtocol<E, S>
where
    E: StorageEngine + Send + 'static,
    S: Serializer,
{
    /// Create a new protocol instance.
    pub fn new(kernel: Kernel<E>, serializer: S) -> Self {
        RespProtocol { kernel, serializer }
    }
}

impl<E, S> Protocol for RespProtocol<E, S>
where
    E: StorageEngine + Send + 'static,
    S: Serializer,
{
    fn handle(&mut self, message: &[u8], mut on_response: impl FnMut(&[u8])) -> usize {
        let mut current = 0;

        for command in self.serializer.decode(message, &mut current) {
            let response = self.kernel.execute(&command);
            let encoded = self.serializer.encode(&command, &response);
            on_response(&encoded);
        }

        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{serializer::resp::RespSerializer, storage::memory::MemoryStorageEngine};

    #[test]
    fn handle_set_then_get() {
        let kernel = Kernel::new(MemoryStorageEngine::new());
        let mut protocol = RespProtocol::new(kernel, RespSerializer);

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let mut responses = Vec::new();
        let consumed = protocol.handle(set, |response| {
            responses.push(response.to_vec());
        });
        assert_eq!(consumed, set.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);

        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        responses.clear();
        let consumed = protocol.handle(get, |response| {
            responses.push(response.to_vec());
        });
        assert_eq!(consumed, get.len());
        assert_eq!(responses, vec![b"$5\r\nvalue\r\n".to_vec()]);
    }
}
