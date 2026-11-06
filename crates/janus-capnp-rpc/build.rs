fn main() {
    println!("cargo:rerun-if-changed=schema/janus.capnp");

    if std::env::var_os("CARGO_FEATURE_RPC").is_some() {
        capnpc::CompilerCommand::new()
            .src_prefix("schema")
            .file("schema/janus.capnp")
            .run()
            .expect("Cap'n Proto schema compilation failed; install the capnp compiler");
    }
}
