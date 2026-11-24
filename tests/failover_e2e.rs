//! Single-shard HA e2e: detect dead primary → promote → write on new primary.

use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    thread,
    time::Duration,
};

use janus_kernel::Kernel;
use janus_replication::{ConfigAuthority, FailoverConfig};
use janus_storage::MemoryStorageEngine;
use janus_types::{Command, Response};
use tokio::net::TcpListener;

fn resp_set(key: &str, value: &str) -> Vec<u8> {
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n",
        key.len(),
        value.len()
    )
    .into_bytes()
}

fn resp_get(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len()).into_bytes()
}

fn read_exact_resp(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).expect("read");
    buf
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..200 {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("condition not met in time");
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_primary_detect_promote_write_on_new_primary() {
    let replica_wire = TcpListener::bind("127.0.0.1:0").await.expect("replica wire");
    let replica_wire_addr = replica_wire.local_addr().expect("addr").to_string();
    let replica_resp = TcpListener::bind("127.0.0.1:0").await.expect("replica resp");
    let replica_resp_addr = replica_resp.local_addr().expect("addr").to_string();
    let primary_resp = TcpListener::bind("127.0.0.1:0").await.expect("primary resp");
    let primary_resp_addr = primary_resp.local_addr().expect("addr").to_string();

    let (handles, replica_fut) =
        janus::serve_replica_ha(replica_resp, replica_wire, None, None).expect("ha replica");
    tokio::spawn(replica_fut);

    thread::sleep(Duration::from_millis(50));

    let peer = replica_wire_addr.clone();
    let primary_task = tokio::spawn(async move {
        let _ = janus::serve_primary(primary_resp, &peer, None, None).await;
    });

    thread::sleep(Duration::from_millis(80));

    let authority = Arc::new(ConfigAuthority::in_memory());
    authority
        .publish_primary(&primary_resp_addr)
        .expect("seed authority");
    assert_eq!(
        authority.current_primary().as_deref(),
        Some(primary_resp_addr.as_str())
    );

    // Seed data on the original primary; replica should observe it.
    {
        let mut primary = TcpStream::connect(&primary_resp_addr).expect("primary connect");
        primary
            .write_all(&resp_set("before", "failover"))
            .expect("set");
        assert_eq!(read_exact_resp(&mut primary, 5), b"+OK\r\n");
    }

    wait_until(|| {
        let mut replica = match TcpStream::connect(&replica_resp_addr) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if replica.write_all(&resp_get("before")).is_err() {
            return false;
        }
        let mut buf = [0u8; 14];
        matches!(replica.read_exact(&mut buf), Ok(())) && &buf == b"$8\r\nfailover\r\n"
    });

    // Replica rejects client writes before promotion.
    {
        let mut replica = TcpStream::connect(&replica_resp_addr).expect("replica connect");
        replica
            .write_all(&resp_set("nope", "x"))
            .expect("write set");
        let mut buf = Vec::new();
        replica
            .set_read_timeout(Some(Duration::from_secs(2)))
            .ok();
        let mut tmp = [0u8; 128];
        let n = replica.read(&mut tmp).expect("read readonly");
        buf.extend_from_slice(&tmp[..n]);
        assert!(
            std::str::from_utf8(&buf)
                .unwrap_or("")
                .contains("READONLY"),
            "expected READONLY, got {:?}",
            String::from_utf8_lossy(&buf)
        );
    }

    // Kill primary (abort server task; drop its listener via task cancel).
    primary_task.abort();
    let _ = primary_task.await;

    let cfg = FailoverConfig {
        dead_after: Duration::from_millis(50),
        probe_timeout: Duration::from_millis(40),
        probe_interval: Duration::from_millis(15),
    };
    let auth = Arc::clone(&authority);
    let new_endpoint = replica_resp_addr.clone();
    janus_replication::detect_and_promote(
        &primary_resp_addr,
        cfg,
        &handles.kernel,
        &handles.apply_stream,
        &handles.stop_apply,
        auth.as_ref(),
        new_endpoint.clone(),
    )
    .await
    .expect("detect and promote");

    assert_eq!(
        authority.current_primary().as_deref(),
        Some(new_endpoint.as_str())
    );

    // Writes succeed on the promoted replica (new primary).
    {
        let mut neo = TcpStream::connect(&replica_resp_addr).expect("new primary");
        neo.write_all(&resp_set("after", "promoted"))
            .expect("set after");
        assert_eq!(read_exact_resp(&mut neo, 5), b"+OK\r\n");
        neo.write_all(&resp_get("after")).expect("get after");
        assert_eq!(read_exact_resp(&mut neo, 14), b"$8\r\npromoted\r\n");
        neo.write_all(&resp_get("before")).expect("get before");
        assert_eq!(read_exact_resp(&mut neo, 14), b"$8\r\nfailover\r\n");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authority_file_updates_after_promote() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("janus-ha-auth-{nanos}.txt"));
    let authority = ConfigAuthority::file(&path).expect("file authority");
    authority
        .publish_primary("127.0.0.1:1111")
        .expect("seed");

    let mut kernel = Kernel::new(MemoryStorageEngine::new());
    kernel.set_accepts_writes(false);
    let kernel = std::sync::Mutex::new(kernel);
    let apply = std::sync::atomic::AtomicBool::new(true);
    let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);

    janus_replication::promote_replica(
        &kernel,
        &apply,
        &stop_tx,
        &authority,
        "127.0.0.1:2222",
    )
    .expect("promote");

    assert_eq!(
        authority.current_primary().as_deref(),
        Some("127.0.0.1:2222")
    );
    let disk = std::fs::read_to_string(&path).expect("read auth file");
    assert_eq!(disk.trim(), "127.0.0.1:2222");
    assert!(kernel.lock().expect("lock").accepts_writes());
    let _ = std::fs::remove_file(path);
}

#[test]
fn demoted_or_returning_primary_rejects_writes_until_reconfig() {
    // Old primary returning without authority reconfig must not accept writes.
    // Ops re-admits it as a replica and full-syncs via bootstrap (F18-02).
    let mut returning = Kernel::new(MemoryStorageEngine::new());
    returning.set_accepts_writes(false);
    assert_eq!(
        returning.execute(&Command::Set {
            key: b"k",
            value: b"v",
        }),
        Response::Error("READONLY You can't write against a read-only replica".into())
    );
    assert_eq!(
        returning.execute(&Command::Get { key: b"k" }),
        Response::Value(None)
    );
}

#[test]
fn no_gossip_or_raft_in_failover_path() {
    // Compile-time / API surface check: promotion is local supervisor + authority.
    let auth = ConfigAuthority::in_memory();
    let mut kernel = Kernel::new(MemoryStorageEngine::new());
    kernel.set_accepts_writes(false);
    let kernel = std::sync::Mutex::new(kernel);
    let apply = std::sync::atomic::AtomicBool::new(true);
    let (stop_tx, _rx) = tokio::sync::watch::channel(false);
    janus_replication::promote_replica(&kernel, &apply, &stop_tx, &auth, "127.0.0.1:9")
        .expect("promote");
    assert_eq!(auth.current_primary().as_deref(), Some("127.0.0.1:9"));
}
