/// Contains code related to clustering
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
}