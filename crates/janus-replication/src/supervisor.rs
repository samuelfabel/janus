//! In-process failover supervisor: detect dead primary → promote replica.
//!
//! Control plane only — does not embed Raft, etcd, or Sentinel wire protocols.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};

use janus_kernel::Kernel;
use tokio::sync::watch;

use crate::{authority::ConfigAuthority, health};

/// Tunables for binary primary death detection.
#[derive(Debug, Clone, Copy)]
pub struct FailoverConfig {
    /// How long the primary must stay unreachable before it is considered dead.
    pub dead_after: Duration,
    /// Per-probe TCP connect timeout.
    pub probe_timeout: Duration,
    /// Delay between probes.
    pub probe_interval: Duration,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            dead_after: Duration::from_millis(200),
            probe_timeout: Duration::from_millis(100),
            probe_interval: Duration::from_millis(50),
        }
    }
}

/// Promote a replica: stop applying the remote stream, accept client writes,
/// and publish the new primary endpoint on the authority.
///
/// Conceptual `REPLICAOF NO ONE` — not Redis Sentinel / byte-compatible.
pub fn promote_replica(
    kernel: &Mutex<Kernel>,
    apply_stream: &AtomicBool,
    stop_apply: &watch::Sender<bool>,
    authority: &ConfigAuthority,
    new_primary_endpoint: impl Into<String>,
) -> io::Result<()> {
    apply_stream.store(false, Ordering::SeqCst);
    let _ = stop_apply.send(true);
    {
        let mut guard = kernel.lock().expect("kernel mutex");
        guard.set_accepts_writes(true);
        // Standalone primary after promote: no outbound replica sink yet.
        guard.clear_replication();
    }
    authority.publish_primary(new_primary_endpoint)?;
    Ok(())
}

/// Wait until `primary_endpoint` is considered dead, then promote.
pub async fn detect_and_promote(
    primary_endpoint: &str,
    config: FailoverConfig,
    kernel: &Mutex<Kernel>,
    apply_stream: &AtomicBool,
    stop_apply: &watch::Sender<bool>,
    authority: &ConfigAuthority,
    new_primary_endpoint: impl Into<String>,
) -> io::Result<()> {
    health::wait_until_dead(
        primary_endpoint,
        config.dead_after,
        config.probe_timeout,
        config.probe_interval,
    )
    .await?;
    promote_replica(
        kernel,
        apply_stream,
        stop_apply,
        authority,
        new_primary_endpoint,
    )
}
