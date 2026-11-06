fn main() {
    println!("cargo:rerun-if-changed=proto/janus.proto");
    if std::env::var_os("CARGO_FEATURE_RPC").is_some() {
        tonic_prost_build::configure()
            .compile_protos(&["proto/janus.proto"], &["proto"])
            .expect("compile Janus gRPC schema");
    }
}
