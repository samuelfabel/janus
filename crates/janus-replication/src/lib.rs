//! Dedicated TCP replica wire for Janus.
//!
//! Frames [`janus_kernel::replication::ReplicationRecord`] values between a
//! primary [`TcpReplicationSink`] and a replica [`accept_loop`]. The channel is
//! separate from client RESP, Cap'n Proto RPC, and the local WAL file format.

pub mod accept;
pub mod framing;
pub mod sink;

pub use accept::accept_loop;
pub use framing::{decode_payload, encode, DecodeError};
pub use sink::{FailingTcpReplicationSink, TcpReplicationSink};
