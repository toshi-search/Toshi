use serde::{Deserialize, Serialize};

use crate::cluster::shard::{PrimaryShard, ReplicaShard};

pub const SERVICE_NAME: &str = "toshi/";

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub primaries: Vec<PrimaryShard>,
    pub shards: Vec<ReplicaShard>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Hosts(pub Vec<String>);
