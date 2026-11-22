//! Replica-side Tokio accept loop for the dedicated replication channel.

use std::{
    io,
    sync::{Arc, Mutex},
};

use janus_kernel::{apply_replication_record, Kernel};
use janus_types::Response;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};

use crate::framing::{self, DecodeError, MAX_FRAME_LEN};

/// Accept replica-wire connections forever and apply framed records to `kernel`.
///
/// Policy: a framing decode error or an apply that returns [`Response::Error`]
/// closes that session (no Kernel panic). One connection is handled per accepted
/// socket; the primary opens a single peer connection in this milestone.
pub async fn accept_loop(listener: TcpListener, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let kernel = Arc::clone(&kernel);
        tokio::spawn(async move {
            let _ = session(stream, kernel).await;
        });
    }
}

async fn session(mut stream: TcpStream, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    let _ = stream.set_nodelay(true);
    loop {
        let record = match read_record(&mut stream).await {
            Ok(record) => record,
            Err(ReadError::Io(err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(ReadError::Io(err)) => return Err(err),
            Err(ReadError::Decode(err)) => {
                // Framing failure closes the affected session.
                let _ = err;
                return Ok(());
            }
        };
        let response = {
            let mut guard = kernel.lock().expect("replica kernel mutex");
            apply_replication_record(&mut guard, &record)
        };
        if matches!(response, Response::Error(_)) {
            return Ok(());
        }
    }
}

enum ReadError {
    Io(io::Error),
    Decode(DecodeError),
}

impl From<io::Error> for ReadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

async fn read_record(stream: &mut TcpStream) -> Result<janus_kernel::replication::ReplicationRecord, ReadError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(ReadError::Decode(DecodeError::TooLarge));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    framing::decode_payload(&payload).map_err(ReadError::Decode)
}
