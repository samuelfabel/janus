//! Composition root: pick the storage plugin, boot persistence, build the
//! Kernel and hand it to the TCP transport.

use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use janus_kernel::Kernel;
use janus_storage::{
    MemoryStorageEngine, StorageEngine,
    store::{FileSnapshotStore, boot_load},
    wal::boot_wal,
};
use tokio::net::TcpListener;

/// Default production storage plugin (`MemoryStorageEngine`).
///
/// Tests may substitute `BTreeStorageEngine` (or another implementor) by
/// building the Kernel directly.
pub fn build_storage() -> Box<dyn StorageEngine> {
    Box::new(MemoryStorageEngine::new())
}

/// Inject [`build_storage`] into the Kernel and boot the optional persistence.
///
/// `dbfile` and `wal` are mutually exclusive (checked by CLI). Corrupt WAL
/// fails with an I/O error.
pub fn build_kernel(dbfile: Option<PathBuf>, wal: Option<PathBuf>) -> io::Result<Kernel> {
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

/// Bind and accept forever (Tokio tasks per connection).
pub async fn listen(
    bind: &str,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    eprintln!("janus: listening on {bind}");
    janus_transport::accept_loop(listener, kernel).await
}

/// Accept forever on an already-bound Tokio listener (e2e / tests).
pub async fn serve(
    listener: TcpListener,
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> io::Result<()> {
    let kernel = Arc::new(Mutex::new(build_kernel(dbfile, wal)?));
    janus_transport::accept_loop(listener, kernel).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use janus_types::{Command, Response};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[test]
    fn build_storage_default_supports_set_get() {
        let mut kernel = Kernel::from_boxed(build_storage());
        assert_eq!(
            kernel.execute(&Command::Set {
                key: b"k",
                value: b"v",
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"k" }),
            Response::Value(Some(b"v".to_vec()))
        );
    }

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
