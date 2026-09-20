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
        memory::MemoryStorageEngine,
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

async fn accept_loop_with_kernel(
    listener: TcpListener,
    kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>,
) -> io::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => spawn_connection(stream, Arc::clone(&kernel)),
            Err(err) => eprintln!("janus: accept error: {err}"),
        }
    }
}

fn build_kernel(
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<Kernel<MemoryStorageEngine>> {
    let mut engine = MemoryStorageEngine::new();
    match (dbfile, wal) {
        (Some(_), Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ERR conflicting persistence",
        )),
        (None, Some(path)) => {
            let writer = boot_wal(&path, &mut engine)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            Ok(Kernel::with_wal(engine, writer))
        }
        (Some(path), None) => {
            let store = FileSnapshotStore::new(path);
            boot_load(&mut engine, &store)?;
            Ok(Kernel::with_store(engine, Box::new(store)))
        }
        (None, None) => Ok(Kernel::new(engine)),
    }
}

fn spawn_connection(
    stream: tokio::net::TcpStream,
    kernel: Arc<Mutex<Kernel<MemoryStorageEngine>>>,
) {
    let protocol = RespProtocol::shared(kernel, RespSerializer);
    TcpInstance::spawn(stream, protocol);
}

/// E2e harness helpers: bridge a `std::net` listener into the Tokio accept loop.
#[cfg(test)]
mod harness {
    use super::*;
    use std::net::TcpListener as StdTcpListener;

    /// Accept connections from an already-bound std listener (empty in-memory store).
    pub fn accept_loop(listener: StdTcpListener) -> io::Result<()> {
        accept_loop_with_dbfile(listener, None)
    }

    /// Accept connections with an optional snapshot path (boot load + SAVE).
    pub fn accept_loop_with_dbfile(
        listener: StdTcpListener,
        dbfile: Option<PathBuf>,
    ) -> io::Result<()> {
        accept_loop_std(listener, dbfile, None)
    }

    /// Accept connections with an optional WAL path (boot replay + append).
    pub fn accept_loop_with_wal(
        listener: StdTcpListener,
        wal: Option<PathBuf>,
    ) -> io::Result<()> {
        accept_loop_std(listener, None, wal)
    }

    fn accept_loop_std(
        listener: StdTcpListener,
        dbfile: Option<PathBuf>,
        wal: Option<PathBuf>,
    ) -> io::Result<()> {
        listener.set_nonblocking(true)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let listener = TcpListener::from_std(listener)?;
            let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
            accept_loop_with_kernel(listener, kernel).await
        })
    }
}

#[cfg(test)]
pub use harness::{accept_loop, accept_loop_with_dbfile, accept_loop_with_wal};

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
        let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));

        tokio::spawn(async move {
            let _ = accept_loop_with_kernel(listener, kernel).await;
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
