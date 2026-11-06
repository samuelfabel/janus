use std::{
    cell::RefCell,
    io,
    rc::Rc,
    sync::{Arc, Mutex},
};

use capnp::message::ReaderOptions;
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use futures::{future, Future};
use janus_kernel::{lock_kernel, Kernel, Role};
use janus_protocol::execute_gated;
use janus_types::{Command, Response};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::janus_capnp;

/// Accept RPC connections inside a Tokio `LocalSet`; capnp-rpc uses local
/// futures because its runtime is built around `Rc` internally.
pub async fn accept_loop(listener: TcpListener, kernel: Arc<Mutex<Kernel>>) -> io::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::task::spawn_local(serve_connection(stream, Arc::clone(&kernel)));
            }
            Err(error) => eprintln!("janus: Cap'n Proto RPC accept error: {error}"),
        }
    }
}

async fn serve_connection(stream: TcpStream, kernel: Arc<Mutex<Kernel>>) {
    let (reader, writer) = stream.into_split();
    let mut reader_options = ReaderOptions::new();
    reader_options.traversal_limit_in_words(Some(8 * 1024 * 1024));
    reader_options.nesting_limit(64);
    let network = twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        Side::Server,
        reader_options,
    );
    let bootstrap: janus_capnp::janus::Client = capnp_rpc::new_client(JanusRpcServer {
        kernel,
        role: RefCell::new(None),
    });
    let rpc_system = RpcSystem::new(Box::new(network), Some(bootstrap.client));

    if let Err(error) = rpc_system.await {
        if error.kind != capnp::ErrorKind::Disconnected {
            eprintln!("janus: Cap'n Proto RPC connection error: {error}");
        }
    }
}

struct JanusRpcServer {
    kernel: Arc<Mutex<Kernel>>,
    role: RefCell<Option<Role>>,
}

impl janus_capnp::janus::Server for JanusRpcServer {
    fn execute(
        self: Rc<Self>,
        params: janus_capnp::janus::ExecuteParams,
        mut results: janus_capnp::janus::ExecuteResults,
    ) -> impl Future<Output = Result<(), capnp::Error>> + 'static {
        let result = (|| {
            let command = read_command(params.get()?.get_command()?)?;
            let response = {
                let mut kernel = lock_kernel(&self.kernel);
                let mut role = self.role.borrow_mut();
                execute_gated(&mut kernel, &mut role, &command.as_domain())
            };
            write_response(&mut results.get().init_response(), &response)
        })();
        future::ready(result)
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
    fn as_domain(&self) -> Command<'_> {
        match self {
            Self::Set { key, value } => Command::Set { key, value },
            Self::Get(key) => Command::Get { key },
            Self::Delete(key) => Command::Delete { key },
            Self::Expire { key, seconds } => Command::Expire {
                key,
                seconds: *seconds,
            },
            Self::Ttl(key) => Command::Ttl { key },
            Self::Save => Command::Save,
            Self::Multi => Command::Multi,
            Self::Exec => Command::Exec,
            Self::Discard => Command::Discard,
            Self::Info => Command::Info,
            Self::Traces => Command::Traces,
            Self::Auth(password) => Command::Auth { password },
        }
    }
}

fn read_command(reader: janus_capnp::command::Reader<'_>) -> capnp::Result<OwnedCommand> {
    use janus_capnp::command::Which;

    Ok(match reader.which()? {
        Which::Set(set) => {
            let set = set?;
            OwnedCommand::Set {
                key: set.get_key()?.to_vec(),
                value: set.get_value()?.to_vec(),
            }
        }
        Which::Get(key) => OwnedCommand::Get(key?.to_vec()),
        Which::Delete(key) => OwnedCommand::Delete(key?.to_vec()),
        Which::Expire(expire) => {
            let expire = expire?;
            OwnedCommand::Expire {
                key: expire.get_key()?.to_vec(),
                seconds: expire.get_seconds(),
            }
        }
        Which::Ttl(key) => OwnedCommand::Ttl(key?.to_vec()),
        Which::Save(()) => OwnedCommand::Save,
        Which::Multi(()) => OwnedCommand::Multi,
        Which::Exec(()) => OwnedCommand::Exec,
        Which::Discard(()) => OwnedCommand::Discard,
        Which::Info(()) => OwnedCommand::Info,
        Which::Traces(()) => OwnedCommand::Traces,
        Which::Auth(password) => OwnedCommand::Auth(password?.to_vec()),
    })
}

fn write_response(
    mut builder: &mut janus_capnp::response::Builder<'_>,
    response: &Response,
) -> capnp::Result<()> {
    match response {
        Response::Empty => builder.set_empty(()),
        Response::Value(value) => {
            let mut value_builder = builder.reborrow().init_value();
            if let Some(value) = value {
                value_builder.set_present(true);
                value_builder.set_data(value);
            } else {
                value_builder.set_present(false);
                value_builder.set_data(&[]);
            }
        }
        Response::Deleted(deleted) => builder.set_deleted(*deleted),
        Response::Integer(value) => builder.set_integer(*value),
        Response::Queued => builder.set_queued(()),
        Response::Array(items) => {
            let mut array = builder.reborrow().init_array(items.len() as u32);
            for (index, item) in items.iter().enumerate() {
                write_response(&mut array.reborrow().get(index as u32), item)?;
            }
        }
        Response::Error(message) => builder.set_error(message)?,
    }
    Ok(())
}
