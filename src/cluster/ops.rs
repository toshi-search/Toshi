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

#[async_trait::async_trait]
pub trait ClusterOps {
    type Node: DeserializeOwned;
    type Index: DeserializeOwned;

    fn node_path(&self) -> String;
    async fn register_node(&mut self) -> Result<(), ClusterError>;
    async fn place_node_descriptor(&mut self, host: Hosts) -> Result<(), ClusterError>;
    async fn register_cluster(&mut self) -> Result<(), ClusterError>;
    async fn register_shard<S: Shard>(&mut self, shard: &S) -> Result<(), ClusterError>;
    async fn get_index(&mut self, index: String, recurse: bool) -> Result<Vec<Self::Index>, ClusterError>;
    async fn nodes(&mut self) -> Result<Vec<Self::Node>, ClusterError>;
}
