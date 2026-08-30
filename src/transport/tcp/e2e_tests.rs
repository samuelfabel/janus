//! TCP e2e harness (no redis-cli): bind ephemeral port, real accept/read/write.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use super::manager;

fn start_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    thread::spawn(move || {
        let _ = manager::accept_loop(listener);
    });
    // Brief yield so accept is ready
    thread::sleep(Duration::from_millis(20));
    addr
}

fn connect(addr: &str) -> TcpStream {
    let mut last = None;
    for _ in 0..50 {
        match TcpStream::connect(addr) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("read timeout");
                s.set_write_timeout(Some(Duration::from_secs(2)))
                    .expect("write timeout");
                return s;
            }
            Err(e) => {
                last = Some(e);
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    panic!("connect failed: {last:?}");
}

fn read_exact(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).expect("read_exact");
    buf
}

const SET_KEY_VALUE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
const GET_KEY: &[u8] = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

/// A2 / V1-SCOPE: SET + GET on the same connection.
#[test]
fn e2e_set_then_get_same_connection() {
    let addr = start_server();
    let mut client = connect(&addr);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 11), b"$5\r\nvalue\r\n");
}

/// A3 / V1-SCOPE sequence: SET a, SET b, GET a, GET b, DEL a, GET a.
#[test]
fn e2e_set_get_delete_sequence() {
    let addr = start_server();
    let mut client = connect(&addr);

    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
    let set_b = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    let get_a = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    let get_b = b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n";
    let del_a = b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n";

    client.write_all(set_a).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(set_b).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(get_a).unwrap();
    assert_eq!(read_exact(&mut client, 7), b"$1\r\n1\r\n");

    client.write_all(get_b).unwrap();
    assert_eq!(read_exact(&mut client, 7), b"$1\r\n2\r\n");

    client.write_all(del_a).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");

    client.write_all(get_a).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"$-1\r\n");
}

/// A4 — several RESP frames in one client write → all answered in order.
#[test]
fn e2e_multi_message_single_write() {
    let addr = start_server();
    let mut client = connect(&addr);

    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
    let set_b = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    let get_a = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    let get_b = b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n";

    let mut payload = Vec::new();
    payload.extend_from_slice(set_a);
    payload.extend_from_slice(set_b);
    payload.extend_from_slice(get_a);
    payload.extend_from_slice(get_b);
    client.write_all(&payload).unwrap();

    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    assert_eq!(read_exact(&mut client, 7), b"$1\r\n1\r\n");
    assert_eq!(read_exact(&mut client, 7), b"$1\r\n2\r\n");
}

/// A5 — one RESP frame split across two client writes.
#[test]
fn e2e_fragmented_frame_two_writes() {
    let addr = start_server();
    let mut client = connect(&addr);

    let split = 12;
    client.write_all(&SET_KEY_VALUE[..split]).unwrap();
    client.flush().unwrap();
    // Force a separate TCP segment / server read boundary.
    thread::sleep(Duration::from_millis(30));
    client.write_all(&SET_KEY_VALUE[split..]).unwrap();

    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 11), b"$5\r\nvalue\r\n");
}

/// A1 — process accepts a TCP connection (smoke).
#[test]
fn e2e_accepts_connection() {
    let addr = start_server();
    let _client = connect(&addr);
}
