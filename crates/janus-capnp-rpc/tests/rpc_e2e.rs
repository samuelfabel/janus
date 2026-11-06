#![cfg(feature = "rpc")]

use std::sync::{Arc, Mutex};

use capnp::message::ReaderOptions;
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use janus_capnp_rpc::{accept_loop, janus_capnp};
use janus_kernel::Kernel;
use janus_storage::MemoryStorageEngine;
use janus_transport::accept_loop as resp_accept_loop;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, TcpStream},
    task::LocalSet,
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[derive(Debug, PartialEq, Eq)]
enum RpcResponse {
    Empty,
    Queued,
    Value(Option<Vec<u8>>),
    Deleted(bool),
    Integer(i64),
    Array(Vec<RpcResponse>),
    Error(String),
}

#[derive(Debug)]
enum RpcCommand<'a> {
    Set(&'a [u8], &'a [u8]),
    Get(&'a [u8]),
    Delete(&'a [u8]),
    Expire(&'a [u8], u64),
    Ttl(&'a [u8]),
    Save,
    Multi,
    Exec,
    Discard,
    Info,
    Traces,
    Auth(&'a [u8]),
}

#[tokio::test(flavor = "current_thread")]
async fn capnp_rpc_exposes_the_full_command_surface_and_session_auth() {
    LocalSet::new()
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let address = listener.local_addr().expect("local address");
            let resp_listener = TcpListener::bind("127.0.0.1:0").await.expect("RESP bind");
            let resp_address = resp_listener.local_addr().expect("RESP address");
            let kernel = Arc::new(Mutex::new(
                Kernel::new(MemoryStorageEngine::new())
                    .with_requirepass("secret")
                    .with_readonly_pass("read"),
            ));
            tokio::task::spawn_local(accept_loop(listener, Arc::clone(&kernel)));
            tokio::task::spawn_local(resp_accept_loop(resp_listener, kernel));

            let full = connect(address).await;
            assert_eq!(
                execute(&full, RpcCommand::Get(b"key")).await,
                RpcResponse::Error("NOAUTH Authentication required.".into())
            );
            assert_eq!(
                execute(&full, RpcCommand::Auth(b"wrong")).await,
                RpcResponse::Error("ERR invalid password".into())
            );
            assert_eq!(
                execute(&full, RpcCommand::Auth(b"secret")).await,
                RpcResponse::Empty
            );
            assert_eq!(
                execute(&full, RpcCommand::Set(b"key", b"first")).await,
                RpcResponse::Empty
            );
            assert_eq!(
                execute(&full, RpcCommand::Get(b"key")).await,
                RpcResponse::Value(Some(b"first".to_vec()))
            );
            let mut set_request = full.execute_request();
            write_command(
                &mut set_request.get().init_command(),
                RpcCommand::Set(b"pipelined", b"ready"),
            );
            let set_promise = set_request.send().promise;
            let mut get_request = full.execute_request();
            write_command(
                &mut get_request.get().init_command(),
                RpcCommand::Get(b"pipelined"),
            );
            let get_promise = get_request.send().promise;
            let (set_response, get_response) = tokio::join!(set_promise, get_promise);
            assert_eq!(
                read_response(
                    set_response
                        .expect("pipelined set")
                        .get()
                        .expect("set result")
                        .get_response()
                        .expect("set response"),
                )
                .expect("decode set response"),
                RpcResponse::Empty
            );
            assert_eq!(
                read_response(
                    get_response
                        .expect("pipelined get")
                        .get()
                        .expect("get result")
                        .get_response()
                        .expect("get response"),
                )
                .expect("decode get response"),
                RpcResponse::Value(Some(b"ready".to_vec()))
            );
            assert_eq!(
                execute(&full, RpcCommand::Expire(b"key", 0)).await,
                RpcResponse::Integer(1)
            );
            assert_eq!(
                execute(&full, RpcCommand::Ttl(b"key")).await,
                RpcResponse::Integer(-2)
            );
            assert_eq!(
                execute(&full, RpcCommand::Get(b"key")).await,
                RpcResponse::Value(None)
            );
            assert_eq!(
                execute(&full, RpcCommand::Set(b"key", b"value")).await,
                RpcResponse::Empty
            );
            assert_eq!(
                execute(&full, RpcCommand::Ttl(b"key")).await,
                RpcResponse::Integer(-1)
            );
            assert_eq!(
                execute(&full, RpcCommand::Delete(b"key")).await,
                RpcResponse::Deleted(true)
            );
            assert_eq!(
                execute(&full, RpcCommand::Delete(b"key")).await,
                RpcResponse::Deleted(false)
            );
            assert_eq!(execute(&full, RpcCommand::Multi).await, RpcResponse::Empty);
            assert_eq!(
                execute(&full, RpcCommand::Set(b"tx", b"committed")).await,
                RpcResponse::Queued
            );
            assert_eq!(
                execute(&full, RpcCommand::Exec).await,
                RpcResponse::Array(vec![RpcResponse::Empty])
            );
            assert_eq!(
                execute(&full, RpcCommand::Get(b"tx")).await,
                RpcResponse::Value(Some(b"committed".to_vec()))
            );
            assert_eq!(execute(&full, RpcCommand::Multi).await, RpcResponse::Empty);
            assert_eq!(
                execute(&full, RpcCommand::Set(b"tx", b"discarded")).await,
                RpcResponse::Queued
            );
            assert_eq!(execute(&full, RpcCommand::Discard).await, RpcResponse::Empty);
            assert_eq!(
                execute(&full, RpcCommand::Get(b"tx")).await,
                RpcResponse::Value(Some(b"committed".to_vec()))
            );
            assert!(matches!(execute(&full, RpcCommand::Info).await, RpcResponse::Value(Some(_))));
            assert!(matches!(execute(&full, RpcCommand::Traces).await, RpcResponse::Value(Some(_))));
            assert!(matches!(execute(&full, RpcCommand::Save).await, RpcResponse::Error(_)));

            let read_only = connect(address).await;
            assert_eq!(
                execute(&read_only, RpcCommand::Get(b"tx")).await,
                RpcResponse::Error("NOAUTH Authentication required.".into())
            );
            assert_eq!(
                execute(&read_only, RpcCommand::Auth(b"read")).await,
                RpcResponse::Empty
            );
            assert_eq!(
                execute(&read_only, RpcCommand::Get(b"tx")).await,
                RpcResponse::Value(Some(b"committed".to_vec()))
            );
            assert_eq!(
                execute(&read_only, RpcCommand::Set(b"tx", b"denied")).await,
                RpcResponse::Error("NOPERM this connection is read-only".into())
            );

            let mut resp = TcpStream::connect(resp_address).await.expect("RESP connect");
            resp.write_all(
                b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*3\r\n$3\r\nSET\r\n$9\r\nfrom-resp\r\n$10\r\nresp-value\r\n",
            )
            .await
            .expect("RESP set");
            let mut resp_ok = [0u8; 10];
            resp.read_exact(&mut resp_ok).await.expect("RESP replies");
            assert_eq!(&resp_ok, b"+OK\r\n+OK\r\n");
            assert_eq!(
                execute(&full, RpcCommand::Get(b"from-resp")).await,
                RpcResponse::Value(Some(b"resp-value".to_vec()))
            );
            assert_eq!(
                execute(&full, RpcCommand::Set(b"from-rpc", b"rpc-value")).await,
                RpcResponse::Empty
            );

            let mut resp = TcpStream::connect(resp_address).await.expect("RESP reconnect");
            resp.write_all(
                b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*2\r\n$3\r\nGET\r\n$8\r\nfrom-rpc\r\n",
            )
            .await
            .expect("RESP get");
            let mut resp_value = [0u8; 20];
            resp.read_exact(&mut resp_value).await.expect("RESP replies");
            assert_eq!(&resp_value, b"+OK\r\n$9\r\nrpc-value\r\n");
        })
        .await;
}

async fn connect(address: std::net::SocketAddr) -> janus_capnp::janus::Client {
    let stream = TcpStream::connect(address).await.expect("connect");
    let (reader, writer) = stream.into_split();
    let network = twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        Side::Client,
        ReaderOptions::new(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let root: janus_capnp::janus::Client = rpc.bootstrap(Side::Server);
    tokio::task::spawn_local(rpc);
    root
}

async fn execute(client: &janus_capnp::janus::Client, command: RpcCommand<'_>) -> RpcResponse {
    let mut request = client.execute_request();
    write_command(&mut request.get().init_command(), command);
    let response = request.send().promise.await.expect("RPC execute");
    let response = response
        .get()
        .expect("RPC response")
        .get_response()
        .expect("response union");
    read_response(response).expect("decode response")
}

fn write_command(builder: &mut janus_capnp::command::Builder<'_>, command: RpcCommand<'_>) {
    match command {
        RpcCommand::Set(key, value) => {
            let mut set = builder.init_set();
            set.set_key(key);
            set.set_value(value);
        }
        RpcCommand::Get(key) => builder.set_get(key),
        RpcCommand::Delete(key) => builder.set_delete(key),
        RpcCommand::Expire(key, seconds) => {
            let mut expire = builder.init_expire();
            expire.set_key(key);
            expire.set_seconds(seconds);
        }
        RpcCommand::Ttl(key) => builder.set_ttl(key),
        RpcCommand::Save => builder.set_save(()),
        RpcCommand::Multi => builder.set_multi(()),
        RpcCommand::Exec => builder.set_exec(()),
        RpcCommand::Discard => builder.set_discard(()),
        RpcCommand::Info => builder.set_info(()),
        RpcCommand::Traces => builder.set_traces(()),
        RpcCommand::Auth(password) => builder.set_auth(password),
    }
}

fn read_response(reader: janus_capnp::response::Reader<'_>) -> capnp::Result<RpcResponse> {
    use janus_capnp::response::Which;

    Ok(match reader.which()? {
        Which::Empty(()) => RpcResponse::Empty,
        Which::Queued(()) => RpcResponse::Queued,
        Which::Value(value) => {
            let value = value?;
            if value.get_present() {
                RpcResponse::Value(Some(value.get_data()?.to_vec()))
            } else {
                RpcResponse::Value(None)
            }
        }
        Which::Deleted(deleted) => RpcResponse::Deleted(deleted),
        Which::Integer(value) => RpcResponse::Integer(value),
        Which::Array(items) => {
            let items = items?;
            let mut response = Vec::with_capacity(items.len() as usize);
            for item in items.iter() {
                response.push(read_response(item?)?);
            }
            RpcResponse::Array(response)
        }
        Which::Error(error) => RpcResponse::Error(error?.to_str()?.to_owned()),
    })
}
