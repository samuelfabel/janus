use std::{
    future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    task::Poll,
};

use bytes::{Buf, Bytes};
use http::{Method, Request, StatusCode};
use janus_http3::{accept_loop, bind, MAX_REQUEST_BODY};
use janus_kernel::Kernel;
use janus_protocol::HttpKvProtocol;
use janus_storage::MemoryStorageEngine;
use quinn::{crypto::rustls::QuicClientConfig, ClientConfig, Endpoint};
use rustls::{pki_types::PrivatePkcs8KeyDer, ClientConfig as TlsClientConfig, RootCertStore};

#[tokio::test]
async fn serves_key_value_requests_over_http3_on_one_connection() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = certified.cert.der().clone();
    let private_key = PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()).into();
    let server = bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        vec![certificate.clone()],
        private_key,
    )
    .unwrap();
    let server_addr = server.local_addr().unwrap();
    let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
    let server_task = tokio::spawn(accept_loop(server, move || {
        HttpKvProtocol::shared(Arc::clone(&kernel))
    }));

    let mut roots = RootCertStore::empty();
    roots.add(certificate).unwrap();
    let mut tls =
        TlsClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(ClientConfig::new(Arc::new(crypto)));
    let quic = endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut driver, mut sender) = h3::client::builder()
        .build(h3_quinn::Connection::new(quic))
        .await
        .unwrap();
    let driver_task = tokio::spawn(async move {
        future::poll_fn(|cx| match driver.poll_close(cx) {
            Poll::Ready(error) => Poll::Ready(error),
            Poll::Pending => Poll::Pending,
        })
        .await
    });

    let (status, _) = request(
        &mut sender,
        &server_addr,
        Method::PUT,
        "/keys/key",
        b"value",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = request(&mut sender, &server_addr, Method::GET, "/keys/key", b"").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"value");

    let oversized = vec![b'x'; MAX_REQUEST_BODY + 1];
    let (status, _) = request(
        &mut sender,
        &server_addr,
        Method::PUT,
        "/keys/large",
        &oversized,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let (status, _) = request(&mut sender, &server_addr, Method::GET, "/keys/large", b"").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = request(&mut sender, &server_addr, Method::DELETE, "/keys/key", b"").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = request(&mut sender, &server_addr, Method::GET, "/keys/key", b"").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    endpoint.close(0u32.into(), b"test complete");
    server_task.abort();
    driver_task.abort();
}

async fn request<T>(
    sender: &mut h3::client::SendRequest<T, Bytes>,
    server_addr: &SocketAddr,
    method: Method,
    path: &str,
    body: &[u8],
) -> (StatusCode, Vec<u8>)
where
    T: h3::quic::OpenStreams<Bytes>,
{
    let uri = format!("https://localhost:{}{}", server_addr.port(), path);
    let request = Request::builder().method(method).uri(uri).body(()).unwrap();
    let mut stream = sender.send_request(request).await.unwrap();
    if !body.is_empty() {
        stream
            .send_data(Bytes::copy_from_slice(body))
            .await
            .unwrap();
    }
    stream.finish().await.unwrap();
    let response = stream.recv_response().await.unwrap();
    let mut response_body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
        response_body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    (response.status(), response_body)
}
