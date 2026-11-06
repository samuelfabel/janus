//! Optional gRPC edge. The generated protobuf API is available with `rpc`.

#[cfg(feature = "rpc")]
pub mod rpc;

#[cfg(feature = "rpc")]
pub mod janus_v1 {
    tonic::include_proto!("janus.v1");
}
