//! Dedicated TCP replica wire for Janus.
//!
//! Frames [`janus_kernel::replication::ReplicationRecord`] values and domain
//! snapshot payloads between a primary [`TcpReplicationSink`] and a replica
//! [`accept_loop`]. The channel is separate from client RESP, Cap'n Proto RPC,
//! local SAVE files, and the WAL file format.
//!
//! Full sync: the primary sends a [`framing::Frame::Snapshot`] (JANUSNAP bytes
//! from `export_snapshot`), then streams incremental records. Partial sync /
//! backlog offsets are out of scope.

pub mod accept;
pub mod framing;
pub mod sink;

pub use accept::accept_loop;
pub use framing::{decode_payload, encode, encode_frame, encode_snapshot, DecodeError, Frame};
pub use sink::{FailingTcpReplicationSink, TcpReplicationSink};
