use futures::Future;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::cluster::shard::{PrimaryShard, ReplicaShard, Shard};
use crate::cluster::ClusterError;

pub const SERVICE_NAME: &str = "toshi/";

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub primaries: Vec<PrimaryShard>,
    pub shards: Vec<ReplicaShard>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Hosts(pub Vec<String>);

pub trait ClusterOps {
    type Node: DeserializeOwned;
    type Index: DeserializeOwned;

    fn node_path(&self) -> String;
    fn register_node(&mut self) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn place_node_descriptor(&mut self, host: Hosts) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn register_cluster(&mut self) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn register_shard<S: Shard>(&mut self, shard: &S) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn get_index(&mut self, index: String, recurse: bool) -> Box<dyn Future<Item = Vec<Self::Index>, Error = ClusterError> + Send>;
    fn nodes(&mut self) -> Box<dyn Future<Item = Vec<Self::Node>, Error = ClusterError> + Send>;
}