//! Optional HTTP/1.1 REST API listener.

use std::{
    io,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use janus_kernel::Kernel;
use janus_protocol::{HttpKvProtocol, HttpProtocol};
use tokio::net::TcpListener;

/// Maximum HTTP request body accepted by the REST listener.
pub const MAX_REQUEST_BODY: usize = 1024 * 1024;

/// Accept HTTP/1.1 requests indefinitely and share one Kernel across clients.
pub async fn accept_loop(listener: TcpListener, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let protocol = Arc::new(Mutex::new(HttpKvProtocol::shared(Arc::clone(&kernel))));
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let protocol = Arc::clone(&protocol);
                async move { Ok::<_, std::convert::Infallible>(handle_request(protocol, request).await) }
            });
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                eprintln!("janus: REST connection failed: {error}");
            }
        });
    }
}

async fn handle_request(
    protocol: Arc<Mutex<HttpKvProtocol>>,
    request: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let (parts, body) = request.into_parts();
    let body = match Limited::new(body, MAX_REQUEST_BODY).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(Full::new(Bytes::new()))
                .expect("valid response");
        }
    };
    let request = Request::from_parts(parts, body.to_vec());
    let response = protocol
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .handle(request);
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Full::new(Bytes::from(body)))
}
