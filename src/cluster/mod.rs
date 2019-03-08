use std::net::SocketAddr;
use std::time::Duration;

use self::placement::{Background, Place};
use failure::Fail;
use futures::{future, Future};
use hyper::client::connect::Connect;
use log::error;
use serde::{Deserialize, Serialize};
use std::io;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::*;

pub use self::consul::Consul;
pub use self::node::*;

pub mod placement_proto {
    use prost_derive::{Enumeration, Message};

    #[cfg(target_family = "unix")]
    include!(concat!(env!("OUT_DIR"), "/placement.rs"));
    #[cfg(target_family = "windows")]
    include!(concat!(env!("OUT_DIR"), "\\placement.rs"));
}

pub mod cluster_rpc {
    use prost_derive::{Enumeration, Message};

    #[cfg(target_family = "unix")]
    include!(concat!(env!("OUT_DIR"), "/cluster_rpc.rs"));
    #[cfg(target_family = "windows")]
    include!(concat!(env!("OUT_DIR"), "\\cluster_rpc.rs"));
}

pub mod consul;
pub mod node;

mod placement;
pub mod remote_handle;
pub mod rpc_server;
pub mod shard;

/// Run the services associated with the cluster
pub fn run(place_addr: SocketAddr, consul: Consul) -> impl Future<Item = (), Error = std::io::Error> {
    future::lazy(move || {
        let (nodes, bg) = Background::new(consul.clone(), Duration::from_secs(2));

        tokio::spawn(bg.map_err(|e| error!("Error in background placement sync: {:?}", e)));

        // TODO: add cluster service et al

        Place::serve(consul, nodes, place_addr)
    })
}

#[derive(Debug, Fail, Serialize, Deserialize)]
pub enum ClusterError {
    #[fail(display = "Node has no ID")]
    MissingNodeID,
    #[fail(display = "Unable to determine cluster ID")]
    MissingClusterID,
    #[fail(display = "Unable to write node ID: {}", _0)]
    FailedWritingNodeID(String),
    #[fail(display = "Failed registering cluster: {}", _0)]
    FailedRegisteringCluster(String),
    #[fail(display = "Failed registering Node: {}", _0)]
    FailedRegisteringNode(String),
    #[fail(display = "Failed reading NodeID: {}", _0)]
    FailedReadingNodeID(String),
    #[fail(display = "Unable to retrieve disk metadata: {}", _0)]
    FailedGettingDirectoryMetadata(String),
    #[fail(display = "Unable to retrieve block device metadata: {}", _0)]
    FailedGettingBlockDeviceMetadata(String),
    #[fail(display = "Unable to find that directory: {}", _0)]
    NoMatchingDirectoryFound(String),
    #[fail(display = "Unable to find that block device: {}", _0)]
    NoMatchingBlockDeviceFound(String),
    #[fail(display = "Unable to read device RAM information: {}", _0)]
    FailedGettingRAMMetadata(String),
    #[fail(display = "Unable to get CPU metadata: {}", _0)]
    FailedGettingCPUMetadata(String),
    #[fail(display = "Unable to read content as UTF-8")]
    UnableToReadUTF8,
    #[fail(display = "Unable to create PrimaryShard: {}", _0)]
    FailedCreatingPrimaryShard(String),
    #[fail(display = "Unable to get index: {}", _0)]
    FailedGettingIndex(String),
    #[fail(display = "Unable to create ReplicaShard: {}", _0)]
    FailedCreatingReplicaShard(String),
    #[fail(display = "Failed to fetch nodes: {}", _0)]
    FailedFetchingNodes(String),
    #[fail(display = "Unable to get index name: {}", _0)]
    UnableToGetIndexName(String),
    #[fail(display = "Error parsing response from Consul: {}", _0)]
    ErrorParsingConsulJSON(String),
    #[fail(display = "Request from Consul returned an error: {}", _0)]
    ErrorInConsulResponse(String),
    #[fail(display = "Unable to get index handle")]
    UnableToGetIndexHandle,
    #[fail(display = "Unable to store services")]
    UnableToStoreServices,
}

pub type BufError = tower_buffer::error::ServiceError;
pub type GrpcError = tower_grpc::Status;

#[derive(Debug, Fail)]
pub enum RPCError {
    #[fail(display = "Error in RPC: {}", _0)]
    RPCError(GrpcError),
    #[fail(display = "Error in RPC Buffer: {}", _0)]
    BufError(BufError),
}

impl From<GrpcError> for RPCError {
    fn from(err: GrpcError) -> Self {
        RPCError::RPCError(err)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DiskType {
    SSD,
    HDD,
}

#[derive(Debug, Clone)]
pub struct GrpcConn(pub SocketAddr);

impl tower_service::Service<()> for GrpcConn {
    type Response = TcpStream;
    type Error = io::Error;
    type Future = ConnectFuture;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    fn call(&mut self, _: ()) -> Self::Future {
        TcpStream::connect(&self.0)
    }
}
