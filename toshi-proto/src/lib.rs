pub mod cluster_rpc {
    tonic::include_proto!("clusterrpc");

    pub use index_service_client as client;
    pub use index_service_server as server;
}
