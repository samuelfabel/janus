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
//!
//! Single-shard HA helpers (`authority`, `health`, `supervisor`) detect a dead
//! primary, promote a replica, and publish the new endpoint — no Raft, gossip,
//! or Sentinel protocol.

pub mod accept;
pub mod authority;
pub mod framing;
pub mod health;
pub mod sink;
pub mod supervisor;

pub use accept::{accept_loop, accept_loop_until};
pub use authority::ConfigAuthority;
pub use framing::{decode_payload, encode, encode_frame, encode_snapshot, DecodeError, Frame};
pub use health::{probe_tcp, wait_until_dead, ProbeResult};
pub use sink::{FailingTcpReplicationSink, TcpReplicationSink};
pub use supervisor::{detect_and_promote, promote_replica, FailoverConfig};
