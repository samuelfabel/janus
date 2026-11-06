//! Optional HTTP/3 server edge. The caller supplies TLS certificate material.

use std::{io, net::SocketAddr, sync::Arc};

use bytes::Buf;
use http::{Request, Response, StatusCode};
use janus_protocol::HttpProtocol;
use quinn::{crypto::rustls::QuicServerConfig, Endpoint, ServerConfig};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ServerConfig as TlsServerConfig,
};

pub use quinn as quic;

/// Maximum request body accepted by the server.
pub const MAX_REQUEST_BODY: usize = 1024 * 1024;

/// Bind a QUIC endpoint with HTTP/3 ALPN, TLS 1.3, and early data disabled.
pub fn bind(
    addr: SocketAddr,
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> io::Result<Endpoint> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = TlsServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3 is supported by the selected provider")
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    tls.alpn_protocols = vec![b"h3".to_vec()];
    tls.max_early_data_size = 0;

    let crypto = QuicServerConfig::try_from(tls)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    Endpoint::server(ServerConfig::with_crypto(Arc::new(crypto)), addr)
}

/// Accept connections forever, creating one protocol instance per QUIC connection.
pub async fn accept_loop<P, F>(endpoint: Endpoint, factory: F) -> io::Result<()>
where
    P: HttpProtocol + Send + 'static,
    F: Fn() -> P + Send + Sync + 'static,
{
    let factory = Arc::new(factory);
    while let Some(incoming) = endpoint.accept().await {
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                return;
            };
            let mut protocol = factory();
            let Ok(h3_connection) =
                h3::server::Connection::new(h3_quinn::Connection::new(connection)).await
            else {
                return;
            };
            let _ = serve_connection(h3_connection, &mut protocol).await;
        });
    }
    Ok(())
}

async fn serve_connection<P>(
    mut connection: h3::server::Connection<h3_quinn::Connection, bytes::Bytes>,
    protocol: &mut P,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    P: HttpProtocol,
{
    while let Some(resolver) = connection.accept().await? {
        let (request, mut stream) = resolver.resolve_request().await?;
        let mut body = Vec::new();
        let mut too_large = false;
        while let Some(mut chunk) = stream.recv_data().await? {
            let chunk = chunk.copy_to_bytes(chunk.remaining());
            if body.len().saturating_add(chunk.len()) > MAX_REQUEST_BODY {
                too_large = true;
                break;
            }
            body.extend_from_slice(&chunk);
        }

        if too_large {
            stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
            let response = Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(())
                .expect("valid response");
            stream.send_response(response).await?;
            stream.finish().await?;
            continue;
        }

        let (parts, _) = request.into_parts();
        let request = Request::from_parts(parts, body);
        let response = protocol.handle(request);
        let (parts, body) = response.into_parts();
        let response = Response::from_parts(parts, ());
        stream.send_response(response).await?;
        if !body.is_empty() {
            stream.send_data(bytes::Bytes::from(body)).await?;
        }
        stream.finish().await?;
    }
    Ok(())
}
