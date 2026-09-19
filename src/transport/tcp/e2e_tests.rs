//! TCP e2e harness (no redis-cli): bind ephemeral port, real accept/read/write.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::manager;

fn start_server() -> String {
    start_server_with_dbfile(None)
}

fn start_server_with_dbfile(dbfile: Option<PathBuf>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    thread::spawn(move || {
        let _ = match dbfile {
            None => manager::accept_loop(listener),
            Some(path) => manager::accept_loop_with_dbfile(listener, Some(path)),
        };
    });
    // Brief yield so accept is ready
    thread::sleep(Duration::from_millis(20));
    addr
}

fn start_server_with_wal(wal: Option<PathBuf>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    thread::spawn(move || {
        let _ = manager::accept_loop_with_wal(listener, wal);
    });
    thread::sleep(Duration::from_millis(20));
    addr
}

fn temp_dbfile(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "janus-f304-e2e-{}-{}-{label}.snap",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn temp_wal(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "janus-f403-e2e-{}-{}-{label}.wal",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_file(&path);
    path
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

/// Read one RESP simple line ending in `\r\n` (e.g. `:2\r\n`).
fn read_crlf_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read byte");
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            break;
        }
        assert!(buf.len() < 64, "line too long: {buf:?}");
    }
    buf
}

fn parse_resp_integer(line: &[u8]) -> i64 {
    assert!(line.starts_with(b":") && line.ends_with(b"\r\n"), "{line:?}");
    let body = &line[1..line.len() - 2];
    std::str::from_utf8(body)
        .expect("utf8")
        .parse()
        .expect("integer")
}

const SET_KEY_VALUE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
const GET_KEY: &[u8] = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
const EXPIRE_KEY_2: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n2\r\n";
const EXPIRE_KEY_1: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
const EXPIRE_KEY_30: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n30\r\n";
const TTL_KEY: &[u8] = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
const DEL_KEY: &[u8] = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
const SAVE: &[u8] = b"*1\r\n$4\r\nSAVE\r\n";

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

/// V2-SCOPE: SET → EXPIRE → TTL remaining → GET hit (before deadline).
#[test]
fn e2e_expire_ttl_then_get_before_deadline() {
    let addr = start_server();
    let mut client = connect(&addr);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(EXPIRE_KEY_2).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");

    client.write_all(TTL_KEY).unwrap();
    let ttl = parse_resp_integer(&read_crlf_line(&mut client));
    assert!((1..=2).contains(&ttl), "ttl={ttl}");

    client.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 11), b"$5\r\nvalue\r\n");
}

/// V2-SCOPE: after short TTL elapses, GET is null and TTL is -2.
/// Uses wall-clock EXPIRE 1 + sleep (SystemClock on the server).
#[test]
fn e2e_get_and_ttl_after_expire() {
    let addr = start_server();
    let mut client = connect(&addr);
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(EXPIRE_KEY_1).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");

    thread::sleep(Duration::from_millis(1100));

    client.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"$-1\r\n");

    client.write_all(TTL_KEY).unwrap();
    assert_eq!(parse_resp_integer(&read_crlf_line(&mut client)), -2);
}

/// V3-SCOPE: SET → SAVE → second server boots same dbfile → GET hit.
/// Strategy: two accept loops on different ports, shared tempfile (boot path).
#[test]
fn e2e_save_then_restore_get() {
    let dbfile = temp_dbfile("value");
    let addr1 = start_server_with_dbfile(Some(dbfile.clone()));
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(SAVE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    drop(client);

    let addr2 = start_server_with_dbfile(Some(dbfile.clone()));
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    let _ = std::fs::remove_file(&dbfile);
}

/// V3-SCOPE: SET → EXPIRE → SAVE → restore → GET + TTL > 0.
#[test]
fn e2e_save_restore_preserves_ttl() {
    let dbfile = temp_dbfile("ttl");
    let addr1 = start_server_with_dbfile(Some(dbfile.clone()));
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(EXPIRE_KEY_30).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");

    client.write_all(SAVE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    drop(client);

    let addr2 = start_server_with_dbfile(Some(dbfile.clone()));
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    restored.write_all(TTL_KEY).unwrap();
    let ttl = parse_resp_integer(&read_crlf_line(&mut restored));
    assert!(ttl > 0, "ttl={ttl}");

    let _ = std::fs::remove_file(&dbfile);
}

/// V4-SCOPE: SET (no SAVE) → second server boots same wal → GET hit.
/// Strategy: two accept loops on different ports, shared tempfile (first may stay alive).
#[test]
fn e2e_wal_recovery_get() {
    let wal = temp_wal("value");
    let addr1 = start_server_with_wal(Some(wal.clone()));
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    drop(client);

    let addr2 = start_server_with_wal(Some(wal.clone()));
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    let _ = std::fs::remove_file(&wal);
}

/// V4-SCOPE: SET → EXPIRE → recovery → GET + TTL > 0.
#[test]
fn e2e_wal_recovery_preserves_ttl() {
    let wal = temp_wal("ttl");
    let addr1 = start_server_with_wal(Some(wal.clone()));
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(EXPIRE_KEY_30).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");
    drop(client);

    let addr2 = start_server_with_wal(Some(wal.clone()));
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    restored.write_all(TTL_KEY).unwrap();
    let ttl = parse_resp_integer(&read_crlf_line(&mut restored));
    assert!(ttl > 0, "ttl={ttl}");

    let _ = std::fs::remove_file(&wal);
}

/// V4-SCOPE: SET → DEL → recovery → GET miss.
#[test]
fn e2e_wal_recovery_delete() {
    let wal = temp_wal("delete");
    let addr1 = start_server_with_wal(Some(wal.clone()));
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(DEL_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");
    drop(client);

    let addr2 = start_server_with_wal(Some(wal.clone()));
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 5), b"$-1\r\n");

    let _ = std::fs::remove_file(&wal);
}
