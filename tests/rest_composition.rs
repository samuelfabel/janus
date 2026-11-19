#![cfg(feature = "rest")]

use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn rest_and_resp_listeners_share_one_kernel() {
    let resp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resp_addr = resp_listener.local_addr().unwrap();
    let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = rest_listener.local_addr().unwrap();

    tokio::spawn(async move {
        janus::serve_with_resp_and_rest(resp_listener, rest_listener, None, None)
            .await
            .unwrap();
    });

    let mut rest = TcpStream::connect(rest_addr).await.unwrap();
    rest.write_all(
        b"PUT /keys/shared HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nvalue",
    )
    .await
    .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), rest.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 204"), "{response:?}");

    let mut resp = TcpStream::connect(resp_addr).await.unwrap();
    resp.write_all(b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n")
        .await
        .unwrap();
    let mut result = [0u8; 11];
    resp.read_exact(&mut result).await.unwrap();
    assert_eq!(&result, b"$5\r\nvalue\r\n");
}
