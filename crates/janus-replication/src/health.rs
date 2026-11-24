//! Binary primary health probe (no gossip / PFAIL).

use std::{
    io,
    net::ToSocketAddrs,
    time::{Duration, Instant},
};

use tokio::net::TcpStream;

/// Outcome of a single TCP health probe against the primary endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeResult {
    /// TCP connect succeeded within the timeout.
    Alive,
    /// Connect failed or timed out.
    Dead,
}

/// Probe `endpoint` once with a connect timeout.
pub async fn probe_tcp(endpoint: &str, timeout: Duration) -> ProbeResult {
    match tokio::time::timeout(timeout, TcpStream::connect(endpoint)).await {
        Ok(Ok(_stream)) => ProbeResult::Alive,
        Ok(Err(_)) | Err(_) => ProbeResult::Dead,
    }
}

/// Poll until the primary stays unreachable for `dead_after`, or return early
/// if a connect succeeds (caller keeps polling).
///
/// Detection is binary: sustained connect failure ⇒ primary considered dead.
/// No Redis Cluster gossip, PFAIL/FAIL, or quorum.
pub async fn wait_until_dead(
    endpoint: &str,
    dead_after: Duration,
    probe_timeout: Duration,
    probe_interval: Duration,
) -> io::Result<()> {
    // Resolve once so bad addresses fail fast.
    let _ = endpoint
        .to_socket_addrs()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let mut dead_since: Option<Instant> = None;
    loop {
        match probe_tcp(endpoint, probe_timeout).await {
            ProbeResult::Alive => {
                dead_since = None;
            }
            ProbeResult::Dead => {
                let since = *dead_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= dead_after {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(probe_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn probe_alive_when_listening() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        assert_eq!(
            probe_tcp(&addr, Duration::from_millis(200)).await,
            ProbeResult::Alive
        );
    }

    #[tokio::test]
    async fn probe_dead_when_nothing_listens() {
        // High port unlikely to be open; connect should fail quickly.
        assert_eq!(
            probe_tcp("127.0.0.1:1", Duration::from_millis(100)).await,
            ProbeResult::Dead
        );
    }

    #[tokio::test]
    async fn wait_until_dead_detects_closed_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        drop(listener);
        wait_until_dead(
            &addr,
            Duration::from_millis(30),
            Duration::from_millis(50),
            Duration::from_millis(10),
        )
        .await
        .expect("dead");
    }
}
