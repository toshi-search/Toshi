/// Contains code related to clustering
pub mod consul_interface;
pub mod node;

pub use self::consul_interface::ConsulInterface;
pub use self::node::*;