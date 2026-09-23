//! TCP accept loop and per-connection handlers.

use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::net::TcpListener;

use crate::{
    kernel::kernel::Kernel,
    protocol::resp::RespProtocol,
    serializer::resp::RespSerializer,
    storage::{
        build_storage,
        store::{FileSnapshotStore, boot_load},
        wal::boot_wal,
    },
    transport::tcp::instance::TcpInstance,
};

/// Bind and accept forever (Tokio tasks per connection).
///
/// `dbfile` and `wal` are mutually exclusive (checked by CLI). Corrupt WAL
/// fails startup with an I/O error.
pub async fn listen(
    bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    eprintln!("janus: listening on {bind}");
    accept_loop_with_kernel(listener, kernel).await
}

/// Accept forever on an already-bound Tokio listener (e2e / tests).
pub async fn serve(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    accept_loop_with_kernel(listener, kernel).await
}

async fn accept_loop_with_kernel(
    listener: TcpListener,
    kernel: Arc<Mutex<Kernel>>,
) -> io::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => spawn_connection(stream, Arc::clone(&kernel)),
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
}

/// Composition root: inject [`build_storage`] (Memory by default) into the Kernel.
fn build_kernel(dbfile: Option<PathBuf>, wal: Option<PathBuf>) -> io::Result<Kernel> {
    let mut engine = build_storage();
    match (dbfile, wal) {
        (Some(_), Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ERR conflicting persistence",
        )),
        (None, Some(path)) => {
            let writer = boot_wal(&path, engine.as_mut())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            Ok(Kernel::from_boxed_with_wal(engine, writer))
        }
        (Some(path), None) => {
            let store = FileSnapshotStore::new(path);
            boot_load(engine.as_mut(), &store)?;
            Ok(Kernel::from_boxed_with_store(engine, Box::new(store)))
        }
        (None, None) => Ok(Kernel::from_boxed(engine)),
    }
}

fn spawn_connection(stream: tokio::net::TcpStream, kernel: Arc<Mutex<Kernel>>) {
    let protocol = RespProtocol::shared(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[tokio::test]
    async fn listen_accepts_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let _ = serve(listener, None, None).await;
        });

        let mut client = TcpStream::connect(&addr).await.expect("connect");
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .await
            .expect("write");
        let mut ok = [0u8; 5];
        client.read_exact(&mut ok).await.expect("read");
        assert_eq!(&ok, b"+OK\r\n");
    }
}
