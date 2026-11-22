//! Synchronous [`ReplicationSink`] that forwards records over a dedicated TCP writer thread.

use std::{
    io::{self, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::mpsc::{self, SyncSender},
    thread,
    time::Duration,
};

use janus_kernel::replication::{ReplicationRecord, ReplicationSink};

use crate::framing;

enum Job {
    Record {
        record: ReplicationRecord,
        ack: SyncSender<Result<(), ()>>,
    },
    Shutdown,
}

/// Primary-side sink: encodes each record and writes it to a peer over TCP.
///
/// I/O runs on a dedicated OS thread so `replicate` can block safely when the
/// Kernel is invoked from a Tokio worker. A failed write or a dropped peer
/// returns `Err(())`, matching Kernel `ERR replication failed` semantics
/// (local mutation is not rolled back).
pub struct TcpReplicationSink {
    tx: SyncSender<Job>,
}

impl TcpReplicationSink {
    /// Connect to `addr` and start the writer thread.
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let (tx, rx) = mpsc::sync_channel::<Job>(16);
        thread::Builder::new()
            .name("janus-replica-sink".into())
            .spawn(move || {
                let mut stream = stream;
                while let Ok(job) = rx.recv() {
                    match job {
                        Job::Shutdown => break,
                        Job::Record { record, ack } => {
                            let frame = framing::encode(&record);
                            let result = stream
                                .write_all(&frame)
                                .and_then(|_| stream.flush())
                                .map_err(|_| ());
                            let failed = result.is_err();
                            let _ = ack.send(result);
                            if failed {
                                break;
                            }
                        }
                    }
                }
            })?;
        Ok(Self { tx })
    }
}

impl ReplicationSink for TcpReplicationSink {
    fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()> {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.tx
            .send(Job::Record {
                record: record.clone(),
                ack: ack_tx,
            })
            .map_err(|_| ())?;
        ack_rx.recv().map_err(|_| ())?
    }
}

impl Drop for TcpReplicationSink {
    fn drop(&mut self) {
        let _ = self.tx.send(Job::Shutdown);
    }
}

/// Sink that always fails — useful for composition tests of peer-down behavior.
#[derive(Debug, Default)]
pub struct FailingTcpReplicationSink;

impl ReplicationSink for FailingTcpReplicationSink {
    fn replicate(&mut self, _record: &ReplicationRecord) -> Result<(), ()> {
        Err(())
    }
}
