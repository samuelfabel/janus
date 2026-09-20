//! TCP e2e harness (no redis-cli): Tokio accept loop, real read/write.

use std::{
    io::{Read, Write},
    net::TcpStream,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::net::TcpListener;

use super::manager;

async fn start_server() -> String {
    start_server_with_dbfile(None).await
}

async fn start_server_with_dbfile(dbfile: Option<PathBuf>) -> String {
    start_server_with_persistence(dbfile, None).await
}

async fn start_server_with_wal(wal: Option<PathBuf>) -> String {
    start_server_with_persistence(None, wal).await
}

async fn start_server_with_persistence(
    dbfile: Option<PathBuf>,
    wal: Option<PathBuf>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    tokio::spawn(async move {
        let _ = manager::serve(listener, dbfile, wal).await;
    });
    // Brief yield so accept is ready
    tokio::task::yield_now().await;
    std::thread::sleep(Duration::from_millis(20));
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_set_then_get_same_connection() {
    let addr = start_server().await;
    let mut client = connect(&addr);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 11), b"$5\r\nvalue\r\n");
}

/// A3 / V1-SCOPE sequence: SET a, SET b, GET a, GET b, DEL a, GET a.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_set_get_delete_sequence() {
    let addr = start_server().await;
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_multi_message_single_write() {
    let addr = start_server().await;
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_fragmented_frame_two_writes() {
    let addr = start_server().await;
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_accepts_connection() {
    let addr = start_server().await;
    let _client = connect(&addr);
}

/// V2-SCOPE: SET → EXPIRE → TTL remaining → GET hit (before deadline).
#[tokio::test(flavor = "multi_thread")]
async fn e2e_expire_ttl_then_get_before_deadline() {
    let addr = start_server().await;
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_get_and_ttl_after_expire() {
    let addr = start_server().await;
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_save_then_restore_get() {
    let dbfile = temp_dbfile("value");
    let addr1 = start_server_with_dbfile(Some(dbfile.clone())).await;
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(SAVE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    drop(client);

    let addr2 = start_server_with_dbfile(Some(dbfile.clone())).await;
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    let _ = std::fs::remove_file(&dbfile);
}

/// V3-SCOPE: SET → EXPIRE → SAVE → restore → GET + TTL > 0.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_save_restore_preserves_ttl() {
    let dbfile = temp_dbfile("ttl");
    let addr1 = start_server_with_dbfile(Some(dbfile.clone())).await;
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(EXPIRE_KEY_30).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");

    client.write_all(SAVE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    drop(client);

    let addr2 = start_server_with_dbfile(Some(dbfile.clone())).await;
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
#[tokio::test(flavor = "multi_thread")]
async fn e2e_wal_recovery_get() {
    let wal = temp_wal("value");
    let addr1 = start_server_with_wal(Some(wal.clone())).await;
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
    drop(client);

    let addr2 = start_server_with_wal(Some(wal.clone())).await;
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    let _ = std::fs::remove_file(&wal);
}

/// V4-SCOPE: SET → EXPIRE → recovery → GET + TTL > 0.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_wal_recovery_preserves_ttl() {
    let wal = temp_wal("ttl");
    let addr1 = start_server_with_wal(Some(wal.clone())).await;
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(EXPIRE_KEY_30).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");
    drop(client);

    let addr2 = start_server_with_wal(Some(wal.clone())).await;
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 11), b"$5\r\nvalue\r\n");

    restored.write_all(TTL_KEY).unwrap();
    let ttl = parse_resp_integer(&read_crlf_line(&mut restored));
    assert!(ttl > 0, "ttl={ttl}");

    let _ = std::fs::remove_file(&wal);
}

/// V4-SCOPE: SET → DEL → recovery → GET miss.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_wal_recovery_delete() {
    let wal = temp_wal("delete");
    let addr1 = start_server_with_wal(Some(wal.clone())).await;
    let mut client = connect(&addr1);

    client.write_all(SET_KEY_VALUE).unwrap();
    assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");

    client.write_all(DEL_KEY).unwrap();
    assert_eq!(read_exact(&mut client, 4), b":1\r\n");
    drop(client);

    let addr2 = start_server_with_wal(Some(wal.clone())).await;
    let mut restored = connect(&addr2);
    restored.write_all(GET_KEY).unwrap();
    assert_eq!(read_exact(&mut restored, 5), b"$-1\r\n");

    let _ = std::fs::remove_file(&wal);
}

/// V5-SCOPE: client A SET → client B GET on the same server → hit.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_two_clients_set_get_shared_store() {
    let addr = start_server().await;
    let barrier = Arc::new(Barrier::new(2));

    let addr_a = addr.clone();
    let barrier_a = Arc::clone(&barrier);
    let a = thread::spawn(move || {
        let mut client = connect(&addr_a);
        client.write_all(SET_KEY_VALUE).unwrap();
        assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
        barrier_a.wait();
    });

    let addr_b = addr.clone();
    let barrier_b = Arc::clone(&barrier);
    let b = thread::spawn(move || {
        barrier_b.wait();
        let mut client = connect(&addr_b);
        client.write_all(GET_KEY).unwrap();
        assert_eq!(read_exact(&mut client, 11), b"$5\r\nvalue\r\n");
    });

    a.join().expect("client A");
    b.join().expect("client B");
}

/// V5-SCOPE: two clients interleave SET/GET on distinct keys.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_two_clients_interleaved_distinct_keys() {
    let addr = start_server().await;
    let mut handles = Vec::new();
    for (key, value) in [(b"a", b"1"), (b"b", b"2")] {
        let addr = addr.clone();
        handles.push(thread::spawn(move || {
            let set = format!(
                "*3\r\n$3\r\nSET\r\n$1\r\n{}\r\n$1\r\n{}\r\n",
                std::str::from_utf8(key).unwrap(),
                std::str::from_utf8(value).unwrap()
            );
            let get = format!(
                "*2\r\n$3\r\nGET\r\n$1\r\n{}\r\n",
                std::str::from_utf8(key).unwrap()
            );
            let expected = format!("$1\r\n{}\r\n", std::str::from_utf8(value).unwrap());
            let mut client = connect(&addr);
            for _ in 0..25 {
                client.write_all(set.as_bytes()).unwrap();
                assert_eq!(read_exact(&mut client, 5), b"+OK\r\n");
                client.write_all(get.as_bytes()).unwrap();
                assert_eq!(
                    read_exact(&mut client, expected.len()),
                    expected.as_bytes()
                );
            }
        }));
    }
    for h in handles {
        h.join().expect("client");
    }
}
