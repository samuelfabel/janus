use std::{future, net::SocketAddr, sync::Arc, task::Poll};

use bytes::Bytes;
use http::{Method, Request, StatusCode};
use janus::serve_with_http3;
use janus_http3::{
    self,
    quic::{self, crypto::rustls::QuicClientConfig},
};
use rustls::{pki_types::PrivatePkcs8KeyDer, ClientConfig as TlsClientConfig, RootCertStore};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn http3_and_resp_share_the_composed_kernel() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = certified.cert.der().clone();
    let private_key = PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()).into();
    let endpoint = janus_http3::bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        vec![certificate.clone()],
        private_key,
    )
    .unwrap();
    let http3_addr = endpoint.local_addr().unwrap();
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    let server_task = tokio::spawn(serve_with_http3(tcp_listener, endpoint, None, None));

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
    let mut endpoint = quic::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quic::ClientConfig::new(Arc::new(crypto)));
    let quic = endpoint
        .connect(http3_addr, "localhost")
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

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!(
            "https://localhost:{}/keys/shared",
            http3_addr.port()
        ))
        .body(())
        .unwrap();
    let mut stream = sender.send_request(request).await.unwrap();
    stream
        .send_data(Bytes::from_static(b"value"))
        .await
        .unwrap();
    stream.finish().await.unwrap();
    assert_eq!(
        stream.recv_response().await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    let mut tcp = TcpStream::connect(tcp_addr).await.unwrap();
    tcp.write_all(b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n")
        .await
        .unwrap();
    let mut response = [0; 11];
    tcp.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"$5\r\nvalue\r\n");

    endpoint.close(0u32.into(), b"test complete");
    server_task.abort();
    driver_task.abort();
}
