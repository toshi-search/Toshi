use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use cluster::ClusterError;

static NODE_ID_FILENAME: &'static str = ".node_id.txt";
static CLUSTER_NAME_FILENAME: &'static str = ".cluster_name.txt";

pub fn write_node_id(id: String, _p: &str) -> Result<(), ClusterError> {
    let path = Path::new(&NODE_ID_FILENAME);
    match File::create(path) {
        Ok(_) => Ok(()),
        Err(e) => Err(ClusterError::FailedWritingNodeID(e)),
    }
}

pub fn read_node_id(p: &str) -> Result<String, ClusterError> {
    let path = NODE_ID_FILENAME;
    let path = Path::new(&path);
    let mut contents = String::new();
    let mut handle = File::open(&path).map_err(|e| ClusterError::FailedReadingNodeID(e))?;
    handle
        .read_to_string(&mut contents)
        .map_err(|e| ClusterError::FailedReadingNodeID(e))?;
    Ok(contents)
}

/// Collection of all the metadata we can gather about the node. It is composed of
/// sub-structs, listed below. This will be serialized to JSON and sent to Consul.
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    network: NetworkMetadata,
    cpu: CPUMetadata,
    ram: RAMMetadata,
    disks: Vec<DiskMetadata>,
    directories: Vec<DirectoryMetadata>,
}

/// All network data about the node
#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkMetadata {
    ipv4: String,
    ipv6: String,
    port: u16,
}

/// CPU data about the node. Usage is meant to be 0.0-1.0. Not sure how to calculate it yet.
#[derive(Debug, Serialize, Deserialize)]
pub struct CPUMetadata {
    // Key is "physical" or "logical"
    // Value is how many of each the OS reports
    number: HashMap<String, u32>,
    usage: f32,
}

/// Metadata about the node's RAM. Units are in GB.
#[derive(Debug, Serialize, Deserialize)]
pub struct RAMMetadata {
    total: u64,
    free: u64,
    used: u64,
}

/// Metadata about the block devices on the node
#[derive(Debug, Serialize, Deserialize)]
pub struct DiskMetadata {
    iowait: f64,
    disk_type: String,
    writes_per_second: f64,
    reads_per_second: f64,
    write_latency_ms: f64,
    read_latency_ms: f64,
    device_path: String,
}

/// Metadata about the directories on the node
#[derive(Debug, Serialize, Deserialize)]
pub struct DirectoryMetadata {
    device_path: String,
    directory: String,
    max_size: u64,
    current_usage: u64,
    current_inodes: u64,
    free_inodes: u64,
}
