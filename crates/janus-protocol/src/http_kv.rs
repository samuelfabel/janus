use std::sync::{Arc, Mutex};

use crate::execute_gated;
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    Method, Request, Response, StatusCode,
};
use janus_kernel::{lock_kernel, Kernel, Role};
use janus_types::{Command, Response as KernelResponse};

/// Handles one complete HTTP request without owning a socket or transport.
pub trait HttpProtocol {
    fn handle(&mut self, request: Request<Vec<u8>>) -> Response<Vec<u8>>;
}

/// HTTP key/value routes backed by the shared Janus Kernel.
pub struct HttpKvProtocol {
    kernel: Arc<Mutex<Kernel>>,
    role: Option<Role>,
}

impl HttpKvProtocol {
    pub fn shared(kernel: Arc<Mutex<Kernel>>) -> Self {
        Self { kernel, role: None }
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
        if let Some(value) = request.headers().get(AUTHORIZATION) {
            let bytes = value.as_bytes();
            let Some(password) = bytes.get(7..).filter(|_| {
                bytes
                    .get(..7)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"Bearer "))
            }) else {
                self.role = None;
                return response(StatusCode::UNAUTHORIZED);
            };
            self.role = lock_kernel(&self.kernel).role_for(password);
        }
        let path = request.uri().path();
        let (command, raw_value) = if let Some(encoded_key) = path.strip_prefix("/keys/") {
            let (encoded_key, ttl) = encoded_key
                .strip_suffix("/ttl")
                .map(|key| (key, true))
                .unwrap_or((encoded_key, false));
            if encoded_key.is_empty() || encoded_key.contains('/') {
                return response(StatusCode::BAD_REQUEST);
            }
            let Some(key) = percent_decode(encoded_key) else {
                return response(StatusCode::BAD_REQUEST);
            };
            let command = match (ttl, request.method().as_str()) {
                (false, "GET") => Command::Get { key: &key },
                (false, "PUT") => Command::Set {
                    key: &key,
                    value: request.body(),
                },
                (false, "DELETE") => Command::Delete { key: &key },
                (true, "GET") => Command::Ttl { key: &key },
                (true, "PUT") => {
                    let Ok(seconds) = std::str::from_utf8(request.body())
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .ok_or(())
                    else {
                        return response(StatusCode::BAD_REQUEST);
                    };
                    Command::Expire { key: &key, seconds }
                }
                _ => return response(StatusCode::METHOD_NOT_ALLOWED),
            };
            let mut kernel = lock_kernel(&self.kernel);
            let result = execute_gated(&mut kernel, &mut self.role, &command);
            return render_response(result, !ttl && request.method() == Method::GET);
        } else {
            match (path, request.method().as_str()) {
                ("/save", "POST") => (Command::Save, false),
                ("/info", "GET") => (Command::Info, false),
                ("/traces", "GET") => (Command::Traces, false),
                ("/transactions", "POST") => {
                    let command = match request.body().as_slice() {
                        b"MULTI" => Command::Multi,
                        b"EXEC" => Command::Exec,
                        b"DISCARD" => Command::Discard,
                        _ => return response(StatusCode::BAD_REQUEST),
                    };
                    (command, false)
                }
                ("/auth", "POST") => (
                    Command::Auth {
                        password: request.body(),
                    },
                    false,
                ),
                ("/keys", _) => return response(StatusCode::BAD_REQUEST),
                ("/save" | "/info" | "/traces" | "/transactions" | "/auth", _) => {
                    return response(StatusCode::METHOD_NOT_ALLOWED)
                }
                _ => return response(StatusCode::NOT_FOUND),
            }
        };
        let result = execute_gated(&mut lock_kernel(&self.kernel), &mut self.role, &command);
        render_response(result, raw_value)
    }
}

fn render_response(result: KernelResponse, raw_value: bool) -> Response<Vec<u8>> {
    let response = |status, body: Vec<u8>, content_type: Option<&'static str>| {
        let mut builder = Response::builder().status(status);
        if let Some(content_type) = content_type {
            builder = builder.header(CONTENT_TYPE, content_type);
        }
        builder.body(body).expect("valid response")
    };
    match result {
        KernelResponse::Empty => response(StatusCode::NO_CONTENT, Vec::new(), None),
        KernelResponse::Queued => response(StatusCode::ACCEPTED, Vec::new(), None),
        KernelResponse::Value(Some(value)) if raw_value => {
            response(StatusCode::OK, value, Some("application/octet-stream"))
        }
        KernelResponse::Value(Some(value)) => {
            response(StatusCode::OK, value, Some("text/plain; charset=utf-8"))
        }
        KernelResponse::Value(None) => response(StatusCode::NOT_FOUND, Vec::new(), None),
        KernelResponse::Deleted(true) => response(StatusCode::NO_CONTENT, Vec::new(), None),
        KernelResponse::Deleted(false) => response(StatusCode::NOT_FOUND, Vec::new(), None),
        KernelResponse::Integer(value) => response(
            StatusCode::OK,
            value.to_string().into_bytes(),
            Some("text/plain; charset=utf-8"),
        ),
        KernelResponse::Array(values) => response(
            StatusCode::OK,
            serde_json::to_vec(&json_response(&KernelResponse::Array(values)))
                .expect("response JSON is serializable"),
            Some("application/json"),
        ),
        KernelResponse::Error(error) => {
            let status = if error.starts_with("NOAUTH") || error == "ERR invalid password" {
                StatusCode::UNAUTHORIZED
            } else if error.starts_with("NOPERM") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_REQUEST
            };
            response(
                status,
                error.into_bytes(),
                Some("text/plain; charset=utf-8"),
            )
        }
    }
}

fn json_response(result: &KernelResponse) -> serde_json::Value {
    use serde_json::json;
    match result {
        KernelResponse::Empty => json!({"type": "empty"}),
        KernelResponse::Value(Some(value)) => json!({"type": "value", "hex": hex_encode(value)}),
        KernelResponse::Value(None) => json!({"type": "value", "hex": null}),
        KernelResponse::Deleted(deleted) => json!({"type": "deleted", "value": deleted}),
        KernelResponse::Integer(value) => json!({"type": "integer", "value": value}),
        KernelResponse::Queued => json!({"type": "queued"}),
        KernelResponse::Array(values) => json!({
            "type": "array",
            "value": values.iter().map(json_response).collect::<Vec<_>>()
        }),
        KernelResponse::Error(error) => json!({"type": "error", "message": error}),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0xf) as usize] as char);
    }
    encoded
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

    #[test]
    fn supports_ttl_transactions_metrics_and_authentication() {
        let kernel = Kernel::new(MemoryStorageEngine::new())
            .with_requirepass(b"secret".to_vec())
            .with_readonly_pass(b"readonly".to_vec());
        let mut protocol = HttpKvProtocol::shared(Arc::new(Mutex::new(kernel)));
        assert_eq!(
            protocol
                .handle(request(Method::POST, "/auth", b"secret"))
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            protocol
                .handle(request(Method::PUT, "/keys/item", b"value"))
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            protocol
                .handle(request(Method::PUT, "/keys/item/ttl", b"60"))
                .body(),
            b"1"
        );
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/keys/item/ttl", b""))
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            protocol
                .handle(request(Method::POST, "/transactions", b"MULTI"))
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            protocol
                .handle(request(Method::PUT, "/keys/queued", b"v"))
                .status(),
            StatusCode::ACCEPTED
        );
        let exec = protocol.handle(request(Method::POST, "/transactions", b"EXEC"));
        assert_eq!(exec.status(), StatusCode::OK);
        assert_eq!(exec.headers()[CONTENT_TYPE], "application/json");
        assert!(std::str::from_utf8(exec.body())
            .unwrap()
            .contains("\"type\":\"array\""));
        assert_eq!(
            protocol.handle(request(Method::GET, "/info", b"")).status(),
            StatusCode::OK
        );
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/traces", b""))
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            protocol
                .handle(request(Method::POST, "/save", b""))
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn read_only_authentication_rejects_writes() {
        let kernel = Kernel::new(MemoryStorageEngine::new())
            .with_requirepass(b"secret".to_vec())
            .with_readonly_pass(b"readonly".to_vec());
        let mut protocol = HttpKvProtocol::shared(Arc::new(Mutex::new(kernel)));
        protocol.handle(request(Method::POST, "/auth", b"readonly"));
        assert_eq!(
            protocol
                .handle(request(Method::PUT, "/keys/item", b"value"))
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            protocol
                .handle(request(Method::GET, "/keys/item", b""))
                .status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn bearer_authentication_applies_to_individual_requests() {
        let kernel = Kernel::new(MemoryStorageEngine::new()).with_requirepass(b"secret".to_vec());
        let kernel = Arc::new(Mutex::new(kernel));
        let mut protocol = HttpKvProtocol::shared(Arc::clone(&kernel));
        let authorized_request = Request::builder()
            .method(Method::PUT)
            .uri("/keys/item")
            .header(AUTHORIZATION, "Bearer secret")
            .body(b"value".to_vec())
            .unwrap();
        assert_eq!(
            protocol.handle(authorized_request).status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            HttpKvProtocol::shared(kernel)
                .handle(request(Method::GET, "/keys/item", b""))
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
