extern crate tower_grpc_build;

fn main() {
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(true)
        .build(&["proto/placement.proto"], &["proto/"])
        .unwrap_or_else(|e| panic!("Compilation failed :( {}", e));
}
