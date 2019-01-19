use std::io;
use std::net::SocketAddr;

use failure::Fail;
use futures::{future, Future};
use log::error;
use serde::{Deserialize, Serialize};
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;

pub use self::consul::Consul;
pub use self::node::*;
use tower_h2::client::ConnectError;

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

pub mod shard;
pub mod remote_handle;
pub mod rpc_server;
mod placement;

use self::placement::{Background, Place};

/// Run the services associated with the cluster
pub fn run(place_addr: SocketAddr, consul: Consul) -> impl Future<Item = (), Error = std::io::Error> {
    future::lazy(move || {
        let (nodes, bg) = Background::new(consul.clone());

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
}

pub type ConnectionError = ConnectError<io::Error>;
pub type BufError = tower_buffer::Error<tower_h2::client::Error>;
pub type GrpcError = tower_grpc::Error<tower_buffer::Error<ConnectionError>>;

#[derive(Debug, Fail)]
pub enum RPCError {
    #[fail(display = "Error in RPC: {}", _0)]
    RPCError(tower_grpc::Error<GrpcError>),
    #[fail(display = "Error in RPC Connect: {}", _0)]
    ConnectError(ConnectionError),
    #[fail(display = "Error in RPC Buffer: {}", _0)]
    BufError(tower_grpc::Error<BufError>),
}

impl From<GrpcError> for RPCError {
    fn from(err: GrpcError) -> Self {
        RPCError::RPCError(tower_grpc::Error::Inner(err))
    }
}

impl From<ConnectionError> for RPCError {
    fn from(err: ConnectError<io::Error>) -> Self {
        RPCError::ConnectError(err)
    }
}

impl From<tower_grpc::Error<BufError>> for RPCError {
    fn from(err: tower_grpc::Error<BufError>) -> Self {
        RPCError::BufError(err)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DiskType {
    SSD,
    HDD,
}

#[derive(Debug, Clone)]
pub struct GrpcConn(pub SocketAddr);

impl tokio_connect::Connect for GrpcConn {
    type Connected = TcpStream;
    type Error = io::Error;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        TcpStream::connect(&self.0)
    }
}
