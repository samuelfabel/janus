//! Pedagogical replication: mutation records and sinks (Phase 10 / F8).
//!
//! Distribution abstraction only — not local WAL persistence.
//! See [`harness`] for the in-process primary → replica integration test.

pub mod harness;
pub mod record;
pub mod sink;

pub use record::ReplicationRecord;
pub use sink::{FakeReplicationSink, ReplicationSink};
