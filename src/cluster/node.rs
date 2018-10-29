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
