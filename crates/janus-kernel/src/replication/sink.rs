//! Sink notified by the primary after successful mutations (F8-02 hooks the Kernel).

use super::record::ReplicationRecord;

/// Destination for replicated mutations (`Send` for future shared use behind `Box<dyn …>`).
pub trait ReplicationSink: Send {
    /// Deliver one mutation record. `Err(())` signals replication failure to the caller.
    fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()>;
}

/// Test sink that appends every successful `replicate` call in order.
#[derive(Debug, Default, Clone)]
pub struct FakeReplicationSink {
    records: Vec<ReplicationRecord>,
    fail_next: bool,
}

impl FakeReplicationSink {
    /// Empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records delivered so far (in order).
    pub fn records(&self) -> &[ReplicationRecord] {
        &self.records
    }

    /// Next `replicate` returns `Err(())` once, then resumes succeeding.
    pub fn fail_next(&mut self) {
        self.fail_next = true;
    }
}

impl ReplicationSink for FakeReplicationSink {
    fn replicate(&mut self, record: &ReplicationRecord) -> Result<(), ()> {
        if self.fail_next {
            self.fail_next = false;
            return Err(());
        }
        self.records.push(record.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_sink_accumulates_sequence() {
        let mut sink = FakeReplicationSink::new();
        sink
            .replicate(&ReplicationRecord::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            })
            .unwrap();
        sink
            .replicate(&ReplicationRecord::Expire {
                key: b"a".to_vec(),
                seconds: 5,
            })
            .unwrap();
        sink
            .replicate(&ReplicationRecord::Delete {
                key: b"a".to_vec(),
            })
            .unwrap();
        assert_eq!(
            sink.records(),
            &[
                ReplicationRecord::Set {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                ReplicationRecord::Expire {
                    key: b"a".to_vec(),
                    seconds: 5,
                },
                ReplicationRecord::Delete {
                    key: b"a".to_vec(),
                },
            ]
        );
    }

    #[test]
    fn fake_sink_fail_next_then_succeeds() {
        let mut sink = FakeReplicationSink::new();
        sink.fail_next();
        assert!(sink
            .replicate(&ReplicationRecord::Delete {
                key: b"x".to_vec(),
            })
            .is_err());
        assert!(sink.records().is_empty());
        sink
            .replicate(&ReplicationRecord::Delete {
                key: b"x".to_vec(),
            })
            .unwrap();
        assert_eq!(sink.records().len(), 1);
    }

    #[test]
    fn dyn_sink_object_safe() {
        let mut boxed: Box<dyn ReplicationSink> = Box::new(FakeReplicationSink::new());
        boxed
            .replicate(&ReplicationRecord::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap();
    }
}
