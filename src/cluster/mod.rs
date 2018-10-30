//! Contains code related to clustering

use std;

pub mod consul_interface;
pub mod node;

pub use self::consul_interface::ConsulInterface;
pub use self::node::*;

#[derive(Debug, Fail)]
pub enum ClusterError {
    #[fail(display = "Node has no ID")]
    MissingNodeID,
    #[fail(display = "Unable to determine cluster ID")]
    MissingClusterID,
    #[fail(display = "Unable to write node ID: {}", _0)]
    FailedWritingNodeID(std::io::Error),
    #[fail(display = "Failed registering Node")]
    FailedRegisteringNode,
    #[fail(display = "Failed reading NodeID: {}", _0)]
    FailedReadingNodeID(std::io::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DiskType {
    SSD,
    HDD,
}
