use std::sync::{Arc, Mutex};

use http::{header::CONTENT_TYPE, Method, Request, Response, StatusCode};
use janus_kernel::{lock_kernel, Kernel};
use janus_types::{Command, Response as KernelResponse};

/// Handles one complete HTTP request without owning a socket or transport.
pub trait HttpProtocol {
    fn handle(&mut self, request: Request<Vec<u8>>) -> Response<Vec<u8>>;
}

/// HTTP key/value routes backed by the shared Janus Kernel.
pub struct HttpKvProtocol {
    kernel: Arc<Mutex<Kernel>>,
}

impl HttpKvProtocol {
    pub fn shared(kernel: Arc<Mutex<Kernel>>) -> Self {
        Self { kernel }
    }
}

impl HttpProtocol for HttpKvProtocol {
    fn handle(&mut self, request: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let response = |status| {
            Response::builder()
                .status(status)
                .body(Vec::new())
                .expect("valid status")
        };
        if lock_kernel(&self.kernel).auth_required() {
            return response(StatusCode::UNAUTHORIZED);
        }

        let path = request.uri().path();
        let Some(encoded_key) = path.strip_prefix("/keys/") else {
            if path == "/keys" {
                return response(StatusCode::BAD_REQUEST);
            }
            return response(StatusCode::NOT_FOUND);
        };
        if encoded_key.is_empty() || encoded_key.contains('/') {
            return response(StatusCode::BAD_REQUEST);
        }
        let Some(key) = percent_decode(encoded_key) else {
            return response(StatusCode::BAD_REQUEST);
        };

        match *request.method() {
            Method::GET => match lock_kernel(&self.kernel).execute(&Command::Get { key: &key }) {
                KernelResponse::Value(Some(value)) => Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(value)
                    .expect("valid response"),
                KernelResponse::Value(None) => response(StatusCode::NOT_FOUND),
                _ => response(StatusCode::INTERNAL_SERVER_ERROR),
            },
            Method::PUT => match lock_kernel(&self.kernel).execute(&Command::Set {
                key: &key,
                value: request.body(),
            }) {
                KernelResponse::Empty => response(StatusCode::NO_CONTENT),
                _ => response(StatusCode::INTERNAL_SERVER_ERROR),
            },
            Method::DELETE => {
                match lock_kernel(&self.kernel).execute(&Command::Delete { key: &key }) {
                    KernelResponse::Deleted(true) => response(StatusCode::NO_CONTENT),
                    KernelResponse::Deleted(false) => response(StatusCode::NOT_FOUND),
                    _ => response(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            _ => response(StatusCode::METHOD_NOT_ALLOWED),
        }
    }
}

fn percent_decode(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hi = *bytes.get(index + 1)?;
            let lo = *bytes.get(index + 2)?;
            decoded.push((hex(hi)? << 4) | hex(lo)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Some(decoded)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use janus_storage::MemoryStorageEngine;

    fn protocol() -> HttpKvProtocol {
        HttpKvProtocol::shared(Arc::new(Mutex::new(
            Kernel::new(MemoryStorageEngine::new()),
        )))
    }

    fn request(method: Method, path: &str, body: &[u8]) -> Request<Vec<u8>> {
        Request::builder()
            .method(method)
            .uri(path)
            .body(body.to_vec())
            .unwrap()
    }

    #[test]
    fn routes_get_put_delete_and_percent_encoded_keys() {
        let mut protocol = protocol();
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/keys/a%20b", b""))
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            protocol
                .handle(request(Method::PUT, "/keys/a%20b", b"value"))
                .status(),
            StatusCode::NO_CONTENT
        );
        let get = protocol.handle(request(Method::GET, "/keys/a%20b", b""));
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.body(), b"value");
        assert_eq!(get.headers()[CONTENT_TYPE], "application/octet-stream");
        assert_eq!(
            protocol
                .handle(request(Method::DELETE, "/keys/a%20b", b""))
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            protocol
                .handle(request(Method::DELETE, "/keys/a%20b", b""))
                .status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn rejects_bad_paths_and_methods() {
        let mut protocol = protocol();
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/keys/%GG", b""))
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            protocol.handle(request(Method::GET, "/keys", b"")).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/keys/a/b", b""))
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/other", b""))
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            protocol
                .handle(request(Method::POST, "/keys/a", b""))
                .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }

    #[test]
    fn auth_required_rejects_http_without_credentials() {
        let kernel = Kernel::new(MemoryStorageEngine::new()).with_requirepass(b"secret".to_vec());
        let mut protocol = HttpKvProtocol::shared(Arc::new(Mutex::new(kernel)));
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/keys/a", b""))
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
