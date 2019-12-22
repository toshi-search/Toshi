use serde::{Deserialize, Serialize};
use thiserror::Error;

use toshi_types::Error;

pub use self::node::*;

pub type BoxError = Box<dyn ::std::error::Error + Send + Sync + 'static>;

pub type ConnectionError = tonic::transport::Error;

pub mod node;
pub mod ops;
pub mod remote_handle;
pub mod rpc_server;
pub mod shard;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ClusterError {
    #[error("Node has no ID")]
    MissingNodeID,
    #[error("Unable to determine cluster ID")]
    MissingClusterID,
    #[error("Unable to write node ID: {0}")]
    FailedWritingNodeID(String),
    #[error("Failed registering cluster: {0}")]
    FailedRegisteringCluster(String),
    #[error("Failed registering Node: {0}")]
    FailedRegisteringNode(String),
    #[error("Failed reading NodeID: {0}")]
    FailedReadingNodeID(String),
    #[error("Unable to retrieve disk metadata: {0}")]
    FailedGettingDirectoryMetadata(String),
    #[error("Unable to retrieve block device metadata: {0}")]
    FailedGettingBlockDeviceMetadata(String),
    #[error("Unable to find that directory: {0}")]
    NoMatchingDirectoryFound(String),
    #[error("Unable to find that block device: {0}")]
    NoMatchingBlockDeviceFound(String),
    #[error("Unable to read device RAM information: {0}")]
    FailedGettingRAMMetadata(String),
    #[error("Unable to get CPU metadata: {0}")]
    FailedGettingCPUMetadata(String),
    #[error("Unable to read content as UTF-8")]
    UnableToReadUTF8,
    #[error("Unable to create PrimaryShard: {0}")]
    FailedCreatingPrimaryShard(String),
    #[error("Unable to get index: {0}")]
    FailedGettingIndex(String),
    #[error("Unable to create ReplicaShard: {0}")]
    FailedCreatingReplicaShard(String),
    #[error("Failed to fetch nodes: {0}")]
    FailedFetchingNodes(String),
    #[error("Unable to get index name: {0}")]
    UnableToGetIndexName(String),
    #[error("Error parsing response from Consul: {0}")]
    ErrorParsingConsulJSON(String),
    #[error("Request from Consul returned an error: {0}")]
    ErrorInConsulResponse(String),
    #[error("Unable to get index handle")]
    UnableToGetIndexHandle,
    #[error("Unable to store services")]
    UnableToStoreServices,
}

#[derive(Debug, Error)]
pub enum RPCError {
    #[error("Error in RPC: {0}")]
    RPCError(#[from] tonic::Status),

    #[error("Error in RPC Connect: {0}")]
    ConnectError(#[from] ConnectionError),
    #[error("")]
    BoxError(Box<dyn ::std::error::Error + Send + Sync + 'static>),
    #[error("Error in RPC Connect: {0}")]
    ToshiError(Error),
}
