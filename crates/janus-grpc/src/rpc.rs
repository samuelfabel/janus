use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
};

use janus_kernel::{lock_kernel, Kernel};
use janus_protocol::execute_gated;
use janus_types::{Command as DomainCommand, Response as DomainResponse};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status, Streaming};

use crate::janus_v1::{
    command::Command as CommandKind, janus_server::Janus, Array, Command, Empty,
    Response as WireResponse, Value,
};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<WireResponse, Status>> + Send + 'static>>;

/// Accept HTTP/2 gRPC sessions on the supplied TCP listener.
pub async fn accept_loop(listener: TcpListener, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    tonic::transport::Server::builder()
        .add_service(
            crate::janus_v1::janus_server::JanusServer::new(JanusRpcServer { kernel })
                .max_decoding_message_size(8 * 1024 * 1024)
                .max_encoding_message_size(8 * 1024 * 1024),
        )
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
        .map_err(|error| io::Error::other(error.to_string()))
}

struct JanusRpcServer {
    kernel: Arc<Mutex<Kernel>>,
}

#[tonic::async_trait]
impl Janus for JanusRpcServer {
    type ExecuteStream = ResponseStream;

    async fn execute(
        &self,
        request: Request<Streaming<Command>>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let mut incoming = request.into_inner();
        let kernel = Arc::clone(&self.kernel);
        let (sender, receiver) = mpsc::channel(16);

        tokio::spawn(async move {
            let mut role = None;
            loop {
                let message = match incoming.message().await {
                    Ok(Some(command)) => command,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                };
                let result = match read_command(message) {
                    Ok(command) => {
                        let response = {
                            let mut kernel = lock_kernel(&kernel);
                            execute_gated(&mut kernel, &mut role, &command.as_domain())
                        };
                        Ok(write_response(response))
                    }
                    Err(error) => Err(error),
                };
                let stop = result.is_err();
                if sender.send(result).await.is_err() || stop {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

enum OwnedCommand {
    Set { key: Vec<u8>, value: Vec<u8> },
    Get(Vec<u8>),
    Delete(Vec<u8>),
    Expire { key: Vec<u8>, seconds: u64 },
    Ttl(Vec<u8>),
    Save,
    Multi,
    Exec,
    Discard,
    Info,
    Traces,
    Auth(Vec<u8>),
}

impl OwnedCommand {
    fn as_domain(&self) -> DomainCommand<'_> {
        match self {
            Self::Set { key, value } => DomainCommand::Set { key, value },
            Self::Get(key) => DomainCommand::Get { key },
            Self::Delete(key) => DomainCommand::Delete { key },
            Self::Expire { key, seconds } => DomainCommand::Expire {
                key,
                seconds: *seconds,
            },
            Self::Ttl(key) => DomainCommand::Ttl { key },
            Self::Save => DomainCommand::Save,
            Self::Multi => DomainCommand::Multi,
            Self::Exec => DomainCommand::Exec,
            Self::Discard => DomainCommand::Discard,
            Self::Info => DomainCommand::Info,
            Self::Traces => DomainCommand::Traces,
            Self::Auth(password) => DomainCommand::Auth { password },
        }
    }
}

fn read_command(command: Command) -> Result<OwnedCommand, Status> {
    let command = command
        .command
        .ok_or_else(|| Status::invalid_argument("command is required"))?;
    Ok(match command {
        CommandKind::Set(set) => OwnedCommand::Set {
            key: set.key,
            value: set.value,
        },
        CommandKind::Get(key) => OwnedCommand::Get(key),
        CommandKind::Delete(key) => OwnedCommand::Delete(key),
        CommandKind::Expire(expire) => OwnedCommand::Expire {
            key: expire.key,
            seconds: expire.seconds,
        },
        CommandKind::Ttl(key) => OwnedCommand::Ttl(key),
        CommandKind::Save(_) => OwnedCommand::Save,
        CommandKind::Multi(_) => OwnedCommand::Multi,
        CommandKind::Exec(_) => OwnedCommand::Exec,
        CommandKind::Discard(_) => OwnedCommand::Discard,
        CommandKind::Info(_) => OwnedCommand::Info,
        CommandKind::Traces(_) => OwnedCommand::Traces,
        CommandKind::Auth(password) => OwnedCommand::Auth(password),
    })
}

fn write_response(response: DomainResponse) -> WireResponse {
    use crate::janus_v1::response::Response as ResponseKind;

    let response = match response {
        DomainResponse::Empty => ResponseKind::Empty(Empty {}),
        DomainResponse::Value(value) => ResponseKind::Value(Value {
            present: value.is_some(),
            data: value.unwrap_or_default(),
        }),
        DomainResponse::Deleted(deleted) => ResponseKind::Deleted(deleted),
        DomainResponse::Integer(value) => ResponseKind::Integer(value),
        DomainResponse::Queued => ResponseKind::Queued(Empty {}),
        DomainResponse::Array(items) => ResponseKind::Array(Array {
            items: items.into_iter().map(write_response).collect(),
        }),
        DomainResponse::Error(message) => ResponseKind::Error(message),
    };
    WireResponse {
        response: Some(response),
    }
}
