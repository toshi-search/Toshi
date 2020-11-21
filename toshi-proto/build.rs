fn main() -> std::io::Result<()> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".eraftpb.Message", "raft::eraftpb::Message")
        .compile(&["proto/cluster.proto"], &["proto/"])
}
