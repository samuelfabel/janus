//! Optional Cap'n Proto RPC edge for Janus.

#[cfg(feature = "rpc")]
capnp::generated_code!(pub mod janus_capnp);

#[cfg(feature = "rpc")]
mod rpc;

#[cfg(feature = "rpc")]
pub use rpc::accept_loop;
