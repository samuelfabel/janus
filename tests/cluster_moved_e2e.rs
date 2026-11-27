//! RESP e2e: MOVED redirects, CROSSSLOT, and CLUSTER SLOTS discovery.

use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use janus_kernel::{slot, Kernel, SlotOwnership};
use janus_protocol::{RespProtocol, SlotEndpointMap};
use janus_serializer::RespSerializer;
use janus_storage::MemoryStorageEngine;
use janus_transport::accept_loop_with_protocol;
use tokio::net::TcpListener;

fn resp_get(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len()).into_bytes()
}

fn resp_set(key: &str, value: &str) -> Vec<u8> {
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n",
        key.len(),
        value.len()
    )
    .into_bytes()
}

fn read_simple_line(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read");
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n") {
            break;
        }
    }
    String::from_utf8(buf).expect("utf8")
}

fn read_cluster_slots(stream: &mut TcpStream) -> Vec<u8> {
    // First line is `*N\r\n`; then drain until the socket idles briefly.
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read");
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n") {
            break;
        }
    }
    stream
        .set_read_timeout(Some(Duration::from_millis(150)))
        .ok();
    let mut extra = [0u8; 4096];
    loop {
        match stream.read(&mut extra) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&extra[..n]),
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(err) => panic!("read: {err}"),
        }
        if buf.len() > 64 * 1024 {
            break;
        }
    }
    stream.set_read_timeout(None).ok();
    buf
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_key_returns_moved_with_slot_and_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();

    let foo_slot = slot(b"foo");
    let bar_slot = slot(b"bar");
    assert_ne!(foo_slot, bar_slot);

    let kernel = Arc::new(Mutex::new(
        Kernel::new(MemoryStorageEngine::new())
            .with_owned_slots(SlotOwnership::from_slots([bar_slot])),
    ));
    let map = Arc::new(SlotEndpointMap::from_ranges([
        (0u16, 8191, "127.0.0.1:7000"),
        (8192, 16383, "127.0.0.1:7001"),
    ]));
    let map_factory = Arc::clone(&map);

    tokio::spawn(async move {
        let _ = accept_loop_with_protocol(listener, kernel, move |kernel| {
            RespProtocol::shared(kernel, RespSerializer).with_slot_map(Arc::clone(&map_factory))
        })
        .await;
    });

    thread::sleep(Duration::from_millis(40));

    let mut client = TcpStream::connect(&addr).expect("connect");
    client
        .write_all(&resp_get("foo"))
        .expect("write GET foo");
    let err = read_simple_line(&mut client);
    assert_eq!(
        err,
        format!("-MOVED {foo_slot} 127.0.0.1:7001\r\n"),
        "unexpected MOVED: {err:?}"
    );

    client
        .write_all(&resp_set("bar", "ok"))
        .expect("write SET bar");
    assert_eq!(read_simple_line(&mut client), "+OK\r\n");

    client
        .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n")
        .expect("CLUSTER SLOTS");
    let slots = read_cluster_slots(&mut client);
    let body = String::from_utf8_lossy(&slots);
    assert!(body.starts_with("*2\r\n"), "CLUSTER SLOTS: {body:?}");
    assert!(body.contains("127.0.0.1"));
    assert!(body.contains("7000"));
    assert!(body.contains("7001"));
    assert_eq!(map.endpoint_for_slot(foo_slot), Some("127.0.0.1:7001"));
    assert_eq!(map.endpoint_for_slot(bar_slot), Some("127.0.0.1:7000"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_slot_multi_returns_stable_resp_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();

    tokio::spawn(async move {
        let _ = janus::serve(listener, None, None).await;
    });
    thread::sleep(Duration::from_millis(40));

    let mut client = TcpStream::connect(&addr).expect("connect");
    client.write_all(b"*1\r\n$5\r\nMULTI\r\n").expect("MULTI");
    assert_eq!(read_simple_line(&mut client), "+OK\r\n");
    client
        .write_all(&resp_set("foo", "1"))
        .expect("SET foo");
    assert_eq!(read_simple_line(&mut client), "+QUEUED\r\n");
    client
        .write_all(&resp_set("bar", "2"))
        .expect("SET bar");
    assert_eq!(read_simple_line(&mut client), "+QUEUED\r\n");
    client.write_all(b"*1\r\n$4\r\nEXEC\r\n").expect("EXEC");
    assert_eq!(
        read_simple_line(&mut client),
        "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
    );

    client.write_all(b"*1\r\n$5\r\nMULTI\r\n").expect("MULTI");
    assert_eq!(read_simple_line(&mut client), "+OK\r\n");
    client
        .write_all(&resp_set("{user}a", "1"))
        .expect("SET a");
    assert_eq!(read_simple_line(&mut client), "+QUEUED\r\n");
    client
        .write_all(&resp_set("{user}b", "2"))
        .expect("SET b");
    assert_eq!(read_simple_line(&mut client), "+QUEUED\r\n");
    client.write_all(b"*1\r\n$4\r\nEXEC\r\n").expect("EXEC");
    let exec = read_cluster_slots(&mut client);
    let exec = String::from_utf8_lossy(&exec);
    assert!(exec.starts_with("*2\r\n"), "EXEC reply: {exec:?}");
    assert!(exec.contains("+OK"));
}
