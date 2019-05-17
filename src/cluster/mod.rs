use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use failure::Fail;
use futures::{future, Future};
use log::error;
use serde::{Deserialize, Serialize};
use tower_hyper::client::ConnectError;

use crate::cluster::consul::Hosts;
use crate::settings::Settings;

pub use self::consul::Consul;
pub use self::node::*;
use self::placement::{Background, Place};

pub type BoxError = Box<dyn ::std::error::Error + Send + Sync + 'static>;
pub type ConnectionError = ConnectError<io::Error>;
pub type BufError = tower_buffer::error::ServiceError;
pub type GrpcError = tower_grpc::Status;

pub mod consul;
pub mod node;
pub mod placement;
pub mod remote_handle;
pub mod rpc_server;
pub mod shard;

/// Run the services associated with the cluster
pub fn run(place_addr: SocketAddr, consul: Consul) -> impl Future<Item = (), Error = std::io::Error> {
    future::lazy(move || {
        let (nodes, bg) = Background::new(consul.clone(), Duration::from_secs(2));
        tokio::spawn(bg.map_err(|e| error!("Error in background placement sync: {:?}", e)));
        Place::serve(consul, nodes, place_addr)
    })
}

pub fn connect_to_consul(settings: &Settings) -> impl Future<Item = (), Error = ()> {
    let consul_address = settings.experimental_features.consul_addr.clone();
    let cluster_name = settings.experimental_features.cluster_name.clone();
    let settings_path = settings.path.clone();

    let hostname = hostname::get_hostname().unwrap();
    let hosts = Hosts(vec![format!("{}:{}", hostname, settings.port)]);
    dbg!(serde_json::to_string_pretty(&hosts).unwrap());

    future::lazy(move || {
        let mut consul_client = Consul::builder()
            .with_cluster_name(cluster_name)
            .with_address(consul_address)
            .build()
            .expect("Could not build Consul client.");

        // Build future that will connect to Consul and register the node_id
        consul_client
            .register_cluster()
            .and_then(|_| init_node_id(settings_path))
            .and_then(move |id| {
                consul_client.set_node_id(id);
                consul_client.register_node();
                consul_client.place_node_descriptor(hosts)
            })
            .map_err(|e| error!("Error: {}", e))
    })
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DiskType {
    HDD,
    SSD,
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
