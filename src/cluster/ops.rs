use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::cluster::shard::{PrimaryShard, ReplicaShard, Shard};
use toshi_types::Error;

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
    async fn register_node(&mut self) -> Result<(), Error>;
    async fn place_node_descriptor(&mut self, host: Hosts) -> Result<(), Error>;
    async fn register_cluster(&mut self) -> Result<(), Error>;
    async fn register_shard<S: Shard>(&mut self, shard: &S) -> Result<(), Error>;
    async fn get_index(&mut self, index: String, recurse: bool) -> Result<Vec<Self::Index>, Error>;
    async fn nodes(&mut self) -> Result<Vec<Self::Node>, Error>;
}
