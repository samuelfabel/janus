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

/// Read one RESP bulk string (`$len\r\n…\r\n`); returns payload bytes (no framing).
fn read_bulk_payload(stream: &mut TcpStream) -> Vec<u8> {
    let header = read_crlf_line(stream);
    assert!(header.starts_with(b"$"), "expected bulk header, got {header:?}");
    let len: usize = std::str::from_utf8(&header[1..header.len() - 2])
        .expect("utf8")
        .parse()
        .expect("bulk len");
    let payload = read_exact(stream, len);
    assert_eq!(read_exact(stream, 2), b"\r\n");
    payload
}

const SET_KEY_VALUE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
const GET_KEY: &[u8] = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
const GET_MISSING: &[u8] = b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n";
const INFO: &[u8] = b"*1\r\n$4\r\nINFO\r\n";
const EXPIRE_KEY_2: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n2\r\n";
const EXPIRE_KEY_1: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
const EXPIRE_KEY_30: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n30\r\n";
const TTL_KEY: &[u8] = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
const DEL_KEY: &[u8] = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
const SAVE: &[u8] = b"*1\r\n$4\r\nSAVE\r\n";
