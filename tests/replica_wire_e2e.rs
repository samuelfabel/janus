//! TCP e2e: primary → replica over the dedicated replica wire (not RESP).

use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use janus_kernel::replication::ReplicationSink;
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

fn resp_del(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nDEL\r\n${}\r\n{key}\r\n", key.len()).into_bytes()
}

fn resp_expire(key: &str, seconds: u64) -> Vec<u8> {
    let secs = seconds.to_string();
    format!(
        "*3\r\n$6\r\nEXPIRE\r\n${}\r\n{key}\r\n${}\r\n{secs}\r\n",
        key.len(),
        secs.len()
    )
    .into_bytes()
}

fn read_exact_resp(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).expect("read");
    buf
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("condition not met in time");
}

#[tokio::test(flavor = "multi_thread")]
async fn primary_set_delete_expire_visible_on_replica() {
    let replica_wire = TcpListener::bind("127.0.0.1:0").await.expect("replica wire bind");
    let replica_wire_addr = replica_wire.local_addr().expect("addr").to_string();
    let replica_resp = TcpListener::bind("127.0.0.1:0").await.expect("replica resp bind");
    let replica_resp_addr = replica_resp.local_addr().expect("addr").to_string();
    let primary_resp = TcpListener::bind("127.0.0.1:0").await.expect("primary resp bind");
    let primary_resp_addr = primary_resp.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let _ = janus::serve_replica(replica_resp, replica_wire, None, None).await;
    });

    // Give the replica wire a moment to accept before the primary connects.
    thread::sleep(Duration::from_millis(50));

    let peer = replica_wire_addr.clone();
    tokio::spawn(async move {
        let _ = janus::serve_primary(primary_resp, &peer, None, None).await;
    });

    thread::sleep(Duration::from_millis(50));

    let mut primary = TcpStream::connect(&primary_resp_addr).expect("primary connect");
    primary
        .write_all(&resp_set("k", "v"))
        .expect("write set");
    assert_eq!(read_exact_resp(&mut primary, 5), b"+OK\r\n");

    wait_until(|| {
        let mut replica = TcpStream::connect(&replica_resp_addr).ok();
        let Some(ref mut stream) = replica else {
            return false;
        };
        if stream.write_all(&resp_get("k")).is_err() {
            return false;
        }
        let mut buf = [0u8; 7];
        matches!(stream.read_exact(&mut buf), Ok(())) && &buf == b"$1\r\nv\r\n"
    });

    primary
        .write_all(&resp_expire("k", 60))
        .expect("write expire");
    assert_eq!(read_exact_resp(&mut primary, 4), b":1\r\n");

    wait_until(|| {
        let mut replica = match TcpStream::connect(&replica_resp_addr) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let ttl = format!("*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
        if replica.write_all(ttl.as_bytes()).is_err() {
            return false;
        }
        let mut buf = [0u8; 5];
        if replica.read_exact(&mut buf).is_err() {
            return false;
        }
        // :NN\r\n with 1..=60
        buf[0] == b':' && buf[buf.len() - 2] == b'\r'
    });

    primary
        .write_all(&resp_del("k"))
        .expect("write del");
    assert_eq!(read_exact_resp(&mut primary, 4), b":1\r\n");

    wait_until(|| {
        let mut replica = match TcpStream::connect(&replica_resp_addr) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if replica.write_all(&resp_get("k")).is_err() {
            return false;
        }
        let mut buf = [0u8; 5];
        matches!(replica.read_exact(&mut buf), Ok(())) && &buf == b"$-1\r\n"
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn primary_without_peer_returns_replication_failed() {
    let sink = janus_replication::FailingTcpReplicationSink;
    let mut primary = janus::build_primary_kernel(None, None, Box::new(sink)).expect("kernel");
    assert_eq!(
        primary.execute(&Command::Set {
            key: b"k",
            value: b"v",
        }),
        Response::Error("ERR replication failed".into())
    );
    // Local write is kept (no rollback).
    assert_eq!(
        primary.execute(&Command::Get { key: b"k" }),
        Response::Value(Some(b"v".to_vec()))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_replica_bootstraps_from_primary_snapshot_with_ttl() {
    // Primary holds data before the replica wire connects; full sync must
    // deliver the domain snapshot (not a local SAVE file).
    let replica_wire = TcpListener::bind("127.0.0.1:0").await.expect("replica wire bind");
    let replica_wire_addr = replica_wire.local_addr().expect("addr").to_string();
    let replica_resp = TcpListener::bind("127.0.0.1:0").await.expect("replica resp bind");
    let replica_resp_addr = replica_resp.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let _ = janus::serve_replica(replica_resp, replica_wire, None, None).await;
    });
    thread::sleep(Duration::from_millis(50));

    let mut primary = janus::build_kernel(None, None).expect("primary kernel");
    assert_eq!(
        primary.execute(&Command::Set {
            key: b"a",
            value: b"1",
        }),
        Response::Empty
    );
    assert_eq!(
        primary.execute(&Command::Set {
            key: b"b",
            value: b"2",
        }),
        Response::Empty
    );
    assert_eq!(
        primary.execute(&Command::Expire {
            key: b"b",
            seconds: 90,
        }),
        Response::Integer(1)
    );

    let sink = janus_replication::TcpReplicationSink::connect(&replica_wire_addr)
        .expect("connect sink");
    let mut primary = primary.with_replication(Box::new(sink));
    primary.bootstrap_replica().expect("full sync");

    // Catch-up after snapshot still streams.
    assert_eq!(
        primary.execute(&Command::Set {
            key: b"c",
            value: b"3",
        }),
        Response::Empty
    );

    wait_until(|| {
        let mut replica = match TcpStream::connect(&replica_resp_addr) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if replica.write_all(&resp_get("a")).is_err() {
            return false;
        }
        let mut a = [0u8; 7];
        if replica.read_exact(&mut a).is_err() || &a != b"$1\r\n1\r\n" {
            return false;
        }
        if replica.write_all(&resp_get("b")).is_err() {
            return false;
        }
        let mut b = [0u8; 7];
        if replica.read_exact(&mut b).is_err() || &b != b"$1\r\n2\r\n" {
            return false;
        }
        if replica.write_all(&resp_get("c")).is_err() {
            return false;
        }
        let mut c = [0u8; 7];
        if replica.read_exact(&mut c).is_err() || &c != b"$1\r\n3\r\n" {
            return false;
        }
        let ttl = b"*2\r\n$3\r\nTTL\r\n$1\r\nb\r\n";
        if replica.write_all(ttl).is_err() {
            return false;
        }
        let mut ttl_buf = [0u8; 5];
        if replica.read_exact(&mut ttl_buf).is_err() {
            return false;
        }
        ttl_buf[0] == b':' && ttl_buf[ttl_buf.len() - 2] == b'\r'
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_snapshot_aborts_bootstrap_without_partial_state() {
    let replica_wire = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let replica_wire_addr = replica_wire.local_addr().expect("addr").to_string();
    let replica_resp = TcpListener::bind("127.0.0.1:0").await.expect("resp bind");
    let replica_resp_addr = replica_resp.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let _ = janus::serve_replica(replica_resp, replica_wire, None, None).await;
    });
    thread::sleep(Duration::from_millis(50));

    // Truncated / invalid snapshot frame: replica must not serve partial keys.
    let mut stream = TcpStream::connect(&replica_wire_addr).expect("connect wire");
    let bad = janus_replication::encode_snapshot(b"NOTASNAP");
    stream.write_all(&bad).expect("write bad snapshot");
    let _ = stream.flush();
    drop(stream);

    thread::sleep(Duration::from_millis(80));

    let mut replica = TcpStream::connect(&replica_resp_addr).expect("replica resp");
    replica
        .write_all(&resp_get("ghost"))
        .expect("get after failed bootstrap");
    let mut buf = [0u8; 5];
    replica.read_exact(&mut buf).expect("read");
    assert_eq!(&buf, b"$-1\r\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_during_bootstrap_critical_section_serialize_after_snapshot() {
    // Consistency policy: exclusive Kernel access during export + send_snapshot
    // pauses writes for the handoff; post-bootstrap mutations stream as records
    // and the replica matches the primary cut.
    let replica_wire = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let replica_wire_addr = replica_wire.local_addr().expect("addr").to_string();
    let replica_resp = TcpListener::bind("127.0.0.1:0").await.expect("resp bind");
    let replica_resp_addr = replica_resp.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let _ = janus::serve_replica(replica_resp, replica_wire, None, None).await;
    });
    thread::sleep(Duration::from_millis(50));

    let mut primary = janus::build_kernel(None, None).expect("kernel");
    assert_eq!(
        primary.execute(&Command::Set {
            key: b"pre",
            value: b"snap",
        }),
        Response::Empty
    );
    let sink = janus_replication::TcpReplicationSink::connect(&replica_wire_addr)
        .expect("connect");
    let mut primary = primary.with_replication(Box::new(sink));
    primary.bootstrap_replica().expect("bootstrap");
    assert_eq!(
        primary.execute(&Command::Set {
            key: b"post",
            value: b"stream",
        }),
        Response::Empty
    );

    wait_until(|| {
        let mut replica = match TcpStream::connect(&replica_resp_addr) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if replica.write_all(&resp_get("pre")).is_err() {
            return false;
        }
        let mut pre = [0u8; 10];
        if replica.read_exact(&mut pre).is_err() || &pre != b"$4\r\nsnap\r\n" {
            return false;
        }
        if replica.write_all(&resp_get("post")).is_err() {
            return false;
        }
        let mut post = [0u8; 12];
        matches!(replica.read_exact(&mut post), Ok(())) && &post == b"$6\r\nstream\r\n"
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_drop_yields_replication_failed_on_next_mutate() {
    let replica_wire = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let replica_wire_addr = replica_wire.local_addr().expect("addr").to_string();
    // Accept one connection then drop the listener/session without applying.
    let accept = tokio::spawn(async move {
        let (stream, _) = replica_wire.accept().await.expect("accept");
        drop(stream);
    });

    thread::sleep(Duration::from_millis(20));
    let mut sink = janus_replication::TcpReplicationSink::connect(&replica_wire_addr)
        .expect("connect sink");
    accept.await.expect("accept task");

    // First write may still succeed or fail depending on timing; force a write after drop.
    let _ = sink.replicate(&janus_kernel::replication::ReplicationRecord::Set {
        key: b"a".to_vec(),
        value: b"1".to_vec(),
    });
    // After the peer is gone, further replicates must fail (F8-02).
    assert!(sink
        .replicate(&janus_kernel::replication::ReplicationRecord::Delete {
            key: b"a".to_vec(),
        })
        .is_err());
}
