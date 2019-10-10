use std::io;

use failure::Fail;
use serde::{Deserialize, Serialize};
use tower_hyper::client::ConnectError;

pub use self::node::*;
use toshi_types::error::Error;

pub type BoxError = Box<dyn ::std::error::Error + Send + Sync + 'static>;
pub type ConnectionError = ConnectError<io::Error>;
pub type BufError = tower_buffer::error::ServiceError;
pub type GrpcError = tower_grpc::Status;

pub mod ops;
pub mod node;
pub mod remote_handle;
pub mod rpc_server;
pub mod shard;

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

#[derive(Debug, Fail)]
pub enum RPCError {
    #[fail(display = "Error in RPC: {}", _0)]
    RPCError(tower_grpc::Status),
    #[fail(display = "Error in RPC Buffer: {}", _0)]
    BufError(BufError),
    #[fail(display = "Error in RPC Connect: {}", _0)]
    ConnectError(ConnectionError),
    #[fail(display = "")]
    BoxError(Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[fail(display = "Error in RPC Connect: {}", _0)]
    ToshiError(Error),
}

impl From<ConnectionError> for RPCError {
    fn from(err: ConnectError<io::Error>) -> Self {
        RPCError::ConnectError(err)
    }
}

impl From<BoxError> for RPCError {
    fn from(err: BoxError) -> Self {
        RPCError::BoxError(err)
    }
}

impl From<tower_grpc::Status> for RPCError {
    fn from(err: tower_grpc::Status) -> Self {
        RPCError::RPCError(err)
    }
}
