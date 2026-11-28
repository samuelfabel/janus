//! RESP e2e: slot migration with `-ASK` / `ASKING` and post-migration `-MOVED`.

use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use janus_kernel::{slot, Kernel, SlotOwnership};
use janus_protocol::{AskEndpointMap, RespProtocol, SlotEndpointMap};
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

fn read_bulk_or_line(stream: &mut TcpStream) -> String {
    let mut first = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read");
        first.push(byte[0]);
        if first.ends_with(b"\r\n") {
            break;
        }
    }
    if first.starts_with(b"$") {
        let header = std::str::from_utf8(&first).expect("utf8");
        let n: isize = header[1..].trim_end().parse().expect("bulk len");
        if n < 0 {
            return header.to_string();
        }
        let mut body = vec![0u8; n as usize + 2];
        stream.read_exact(&mut body).expect("bulk body");
        let mut out = first;
        out.extend_from_slice(&body);
        return String::from_utf8(out).expect("utf8");
    }
    String::from_utf8(first).expect("utf8")
}

async fn serve_node(
    listener: TcpListener,
    kernel: Arc<Mutex<Kernel>>,
    slot_map: Arc<Mutex<SlotEndpointMap>>,
    ask_map: Arc<Mutex<AskEndpointMap>>,
) {
    let _ = accept_loop_with_protocol(listener, kernel, move |kernel| {
        RespProtocol::shared(kernel, RespSerializer)
            .with_slot_map(Arc::clone(&slot_map))
            .with_ask_map(Arc::clone(&ask_map))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resharding_ask_asking_then_moved_to_new_owner() {
    let foo_slot = slot(b"foo");
    let source_ep = "127.0.0.1:7000";
    let dest_ep = "127.0.0.1:7001";

    let source_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind source");
    let dest_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind dest");
    let source_addr = source_listener.local_addr().expect("addr").to_string();
    let dest_addr = dest_listener.local_addr().expect("addr").to_string();

    let mut source_kernel = Kernel::new(MemoryStorageEngine::new())
        .with_owned_slots(SlotOwnership::from_slots([foo_slot]));
    assert_eq!(
        source_kernel.execute(&janus_types::Command::Set {
            key: b"foo",
            value: b"payload",
        }),
        janus_types::Response::Empty
    );

    let dest_kernel =
        Kernel::new(MemoryStorageEngine::new()).with_owned_slots(SlotOwnership::from_slots([]));

    let slot_map = Arc::new(Mutex::new(SlotEndpointMap::from_ranges([(
        0u16,
        16383,
        source_ep,
    )])));
    let ask_map = Arc::new(Mutex::new(AskEndpointMap::new()));

    let source_shared = Arc::new(Mutex::new(source_kernel));
    let dest_shared = Arc::new(Mutex::new(dest_kernel));

    {
        let mut sk = source_shared.lock().expect("source");
        let mut dk = dest_shared.lock().expect("dest");
        sk.begin_migrating(foo_slot);
        dk.begin_importing(foo_slot);
        ask_map
            .lock()
            .expect("ask")
            .set_destination(foo_slot, dest_ep);
        let entries = sk.export_slot(foo_slot);
        dk.import_slot_entries(&entries);
    }

    let sm1 = Arc::clone(&slot_map);
    let am1 = Arc::clone(&ask_map);
    let k1 = Arc::clone(&source_shared);
    tokio::spawn(async move {
        serve_node(source_listener, k1, sm1, am1).await;
    });
    let sm2 = Arc::clone(&slot_map);
    let am2 = Arc::clone(&ask_map);
    let k2 = Arc::clone(&dest_shared);
    tokio::spawn(async move {
        serve_node(dest_listener, k2, sm2, am2).await;
    });
    thread::sleep(Duration::from_millis(40));

    // Absent key on MIGRATING source → -ASK (use a key in the same slot via hash tag).
    // `foo` itself was exported but still present until finish; delete-equivalent: query a
    // missing same-slot key.
    let missing = "{foo}missing";
    assert_eq!(slot(missing.as_bytes()), foo_slot);
    let mut source_client = TcpStream::connect(&source_addr).expect("connect source");
    source_client
        .write_all(&resp_get(missing))
        .expect("GET missing");
    let ask = read_simple_line(&mut source_client);
    assert_eq!(
        ask,
        format!("-ASK {foo_slot} {dest_ep}\r\n"),
        "unexpected ASK: {ask:?}"
    );

    // Without ASKING, destination returns -MOVED to the current stable owner.
    let mut dest_client = TcpStream::connect(&dest_addr).expect("connect dest");
    dest_client
        .write_all(&resp_get("foo"))
        .expect("GET foo no asking");
    let moved = read_simple_line(&mut dest_client);
    assert_eq!(
        moved,
        format!("-MOVED {foo_slot} {source_ep}\r\n"),
        "unexpected MOVED: {moved:?}"
    );

    // ASKING + next query succeeds on the IMPORTING destination.
    dest_client
        .write_all(b"*1\r\n$6\r\nASKING\r\n")
        .expect("ASKING");
    assert_eq!(read_simple_line(&mut dest_client), "+OK\r\n");
    dest_client
        .write_all(&resp_get("foo"))
        .expect("GET foo asking");
    assert_eq!(read_bulk_or_line(&mut dest_client), "$7\r\npayload\r\n");

    // Finish migration: ownership + map update; ASK target cleared.
    {
        let mut sk = source_shared.lock().expect("source");
        let mut dk = dest_shared.lock().expect("dest");
        sk.finish_migrate_source(foo_slot);
        dk.finish_migrate_destination(foo_slot);
        slot_map
            .lock()
            .expect("map")
            .assign_slot(foo_slot, dest_ep);
        ask_map.lock().expect("ask").clear_destination(foo_slot);
    }

    dest_client
        .write_all(&resp_get("foo"))
        .expect("GET foo after");
    assert_eq!(read_bulk_or_line(&mut dest_client), "$7\r\npayload\r\n");

    source_client
        .write_all(&resp_get("foo"))
        .expect("GET foo on old");
    let final_moved = read_simple_line(&mut source_client);
    assert_eq!(
        final_moved,
        format!("-MOVED {foo_slot} {dest_ep}\r\n"),
        "unexpected post-migration MOVED: {final_moved:?}"
    );

    // Stable write on the new owner.
    dest_client
        .write_all(&resp_set("foo", "ok"))
        .expect("SET foo");
    assert_eq!(read_simple_line(&mut dest_client), "+OK\r\n");
}
