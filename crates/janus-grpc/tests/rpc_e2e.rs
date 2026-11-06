#![cfg(feature = "rpc")]

use std::sync::{Arc, Mutex};

use janus_grpc::janus_v1::{
    command::Command as CommandKind, janus_client::JanusClient, response::Response as ResponseKind,
    Array, Command, Empty, ExpireCommand, Response as WireResponse, SetCommand, Value,
};
use janus_kernel::Kernel;
use janus_storage::memory::MemoryStorageEngine;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_stream::iter;

#[tokio::test]
async fn grpc_sessions_preserve_command_auth_and_shared_kernel_semantics() {
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.expect("gRPC bind");
    let grpc_address = grpc_listener.local_addr().expect("gRPC address");
    let resp_listener = TcpListener::bind("127.0.0.1:0").await.expect("RESP bind");
    let resp_address = resp_listener.local_addr().expect("RESP address");
    let kernel = Arc::new(Mutex::new(
        Kernel::new(MemoryStorageEngine::new())
            .with_requirepass(b"secret".to_vec())
            .with_readonly_pass(b"read".to_vec()),
    ));

    let grpc_kernel = Arc::clone(&kernel);
    tokio::spawn(async move {
        let _ = janus_grpc::rpc::accept_loop(grpc_listener, grpc_kernel).await;
    });
    tokio::spawn(async move {
        let _ = janus_transport::accept_loop(resp_listener, kernel).await;
    });

    let mut client = JanusClient::connect(format!("http://{grpc_address}"))
        .await
        .expect("connect gRPC");
    let commands = vec![
        command(CommandKind::Get(b"shared".to_vec())),
        command(CommandKind::Auth(b"read".to_vec())),
        command(CommandKind::Set(SetCommand {
            key: b"shared".to_vec(),
            value: b"denied".to_vec(),
        })),
        command(CommandKind::Auth(b"secret".to_vec())),
        command(CommandKind::Set(SetCommand {
            key: b"shared".to_vec(),
            value: b"grpc\0value".to_vec(),
        })),
        command(CommandKind::Get(b"shared".to_vec())),
        command(CommandKind::Multi(Empty {})),
        command(CommandKind::Set(SetCommand {
            key: b"transaction".to_vec(),
            value: b"committed".to_vec(),
        })),
        command(CommandKind::Exec(Empty {})),
        command(CommandKind::Get(b"transaction".to_vec())),
        command(CommandKind::Multi(Empty {})),
        command(CommandKind::Set(SetCommand {
            key: b"transaction".to_vec(),
            value: b"discarded".to_vec(),
        })),
        command(CommandKind::Discard(Empty {})),
        command(CommandKind::Get(b"transaction".to_vec())),
        command(CommandKind::Expire(ExpireCommand {
            key: b"shared".to_vec(),
            seconds: 60,
        })),
        command(CommandKind::Ttl(b"shared".to_vec())),
        command(CommandKind::Delete(b"shared".to_vec())),
        command(CommandKind::Info(Empty {})),
        command(CommandKind::Traces(Empty {})),
        command(CommandKind::Save(Empty {})),
    ];
    let response = client
        .execute(iter(commands))
        .await
        .expect("open gRPC session");
    let mut responses = response.into_inner();
    let mut values = Vec::new();
    while let Some(response) = responses.message().await.expect("read gRPC response") {
        values.push(response.response.expect("response value"));
    }

    assert_eq!(
        values[0],
        ResponseKind::Error("NOAUTH Authentication required.".into())
    );
    assert_eq!(values[1], ResponseKind::Empty(Empty {}));
    assert_eq!(
        values[2],
        ResponseKind::Error("NOPERM this connection is read-only".into())
    );
    assert_eq!(values[3], ResponseKind::Empty(Empty {}));
    assert_eq!(values[4], ResponseKind::Empty(Empty {}));
    assert_eq!(
        values[5],
        ResponseKind::Value(Value {
            present: true,
            data: b"grpc\0value".to_vec(),
        })
    );
    assert_eq!(values[6], ResponseKind::Empty(Empty {}));
    assert_eq!(values[7], ResponseKind::Queued(Empty {}));
    assert_eq!(
        values[8],
        ResponseKind::Array(Array {
            items: vec![WireResponse {
                response: Some(ResponseKind::Empty(Empty {}))
            }],
        })
    );
    assert_eq!(
        values[9],
        ResponseKind::Value(Value {
            present: true,
            data: b"committed".to_vec(),
        })
    );
    assert_eq!(values[10], ResponseKind::Empty(Empty {}));
    assert_eq!(values[11], ResponseKind::Queued(Empty {}));
    assert_eq!(values[12], ResponseKind::Empty(Empty {}));
    assert_eq!(
        values[13],
        ResponseKind::Value(Value {
            present: true,
            data: b"committed".to_vec(),
        })
    );
    assert_eq!(values[14], ResponseKind::Integer(1));
    assert!(matches!(values[15], ResponseKind::Integer(remaining) if remaining > 0));
    assert_eq!(values[16], ResponseKind::Deleted(true));
    assert!(matches!(
        values[17],
        ResponseKind::Value(Value { present: true, .. })
    ));
    assert!(matches!(
        values[18],
        ResponseKind::Value(Value { present: true, .. })
    ));
    assert!(matches!(values[19], ResponseKind::Error(_)));

    let mut resp = TcpStream::connect(resp_address)
        .await
        .expect("connect RESP");
    resp.write_all(
        b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*3\r\n$3\r\nSET\r\n$6\r\nfrom-r\r\n$5\r\nvalue\r\n",
    )
    .await
    .expect("write RESP commands");
    let mut replies = [0u8; 10];
    resp.read_exact(&mut replies)
        .await
        .expect("read RESP replies");
    assert_eq!(&replies, b"+OK\r\n+OK\r\n");

    let mut client = JanusClient::connect(format!("http://{grpc_address}"))
        .await
        .expect("reconnect gRPC");
    let response = client
        .execute(iter(vec![
            command(CommandKind::Auth(b"secret".to_vec())),
            command(CommandKind::Get(b"from-r".to_vec())),
        ]))
        .await
        .expect("gRPC cross-protocol session");
    let mut responses = response.into_inner();
    assert_eq!(
        responses.message().await.unwrap().unwrap().response,
        Some(ResponseKind::Empty(Empty {}))
    );
    assert_eq!(
        responses.message().await.unwrap().unwrap().response,
        Some(ResponseKind::Value(Value {
            present: true,
            data: b"value".to_vec(),
        }))
    );
}

fn command(command: CommandKind) -> Command {
    Command {
        command: Some(command),
    }
}
