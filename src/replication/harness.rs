//! In-process primary → replica harness (Phase 10 pedagogical).
//!
//! Production `cargo run` stays single-node; this module proves replication
//! without a second RESP wire or multi-process cluster.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        command::types::Command,
        kernel::kernel::{apply_replication_record, Kernel},
        replication::{ReplicationRecord, ReplicationSink},
        response::types::Response,
        storage::memory::MemoryStorageEngine,
    };

    /// Sink that appends records into a shared queue drained by the replica.
    struct ChannelSink {
        records: Arc<Mutex<Vec<ReplicationRecord>>>,
    }

    impl ReplicationSink for ChannelSink {
        fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()> {
            self.records.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    fn drain_to_replica(
        records: &Arc<Mutex<Vec<ReplicationRecord>>>,
        replica: &mut Kernel,
    ) {
        for record in records.lock().unwrap().drain(..) {
            apply_replication_record(replica, &record);
        }
    }

    /// V8-SCOPE: Primary SET → Replica GET hit; Delete and Expire also replicate.
    #[test]
    fn primary_replica_set_get_expire_delete() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let mut primary = Kernel::with_replica(
            MemoryStorageEngine::new(),
            Box::new(ChannelSink {
                records: Arc::clone(&queue),
            }),
        );
        let mut replica = Kernel::new(MemoryStorageEngine::new());

        assert_eq!(
            primary.execute(&Command::Set {
                key: b"k",
                value: b"v",
            }),
            Response::Empty
        );
        drain_to_replica(&queue, &mut replica);
        assert_eq!(
            replica.execute(&Command::Get { key: b"k" }),
            Response::Value(Some(b"v".to_vec()))
        );

        assert_eq!(
            primary.execute(&Command::Expire {
                key: b"k",
                seconds: 60,
            }),
            Response::Integer(1)
        );
        drain_to_replica(&queue, &mut replica);
        match replica.execute(&Command::Ttl { key: b"k" }) {
            Response::Integer(n) => assert!((1..=60).contains(&n)),
            other => panic!("unexpected ttl {other:?}"),
        }

        assert_eq!(
            primary.execute(&Command::Delete { key: b"k" }),
            Response::Deleted(true)
        );
        drain_to_replica(&queue, &mut replica);
        assert_eq!(
            replica.execute(&Command::Get { key: b"k" }),
            Response::Value(None)
        );
    }
}
