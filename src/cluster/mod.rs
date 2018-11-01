//! Contains code related to clustering

use std;
use systemstat;

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
    #[fail(display = "Unable to retrieve disk metadata: {}", _0)]
    FailedGettingDirectoryMetadata(std::io::Error),
    #[fail(display = "Unable to retrieve block device metadata: {}", _0)]
    FailedGettingBlockDeviceMetadata(std::io::Error),
    #[fail(display = "Unable to find that directory: {}", _0)]
    NoMatchingDirectoryFound(String),
    #[fail(display = "Unable to find that block device: {}", _0)]
    NoMatchingBlockDeviceFound(String),
    #[fail(display = "Unable to read device RAM information: {}", _0)]
    FailedGettingRAMMetadata(std::io::Error),
    #[fail(display = "Unable to get CPU metadata: {}", _0)]
    FailedGettingCPUMetadata(std::io::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DiskType {
    SSD,
    HDD,
}
