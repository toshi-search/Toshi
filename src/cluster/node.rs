use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use cluster::ClusterError;

static NODE_ID_FILENAME: &'static str = ".node_id.txt";
static CLUSTER_NAME_FILENAME: &'static str = ".cluster_name.txt";

pub fn write_node_id(id: String, _p: &str) -> Result<(), ClusterError> {
    let path = NODE_ID_FILENAME;
    let path = Path::new(&path);
    if let Ok(mut file) = File::create(path) {
        let _ = file.write_all(id.as_bytes());
        Ok(())
    } else {
        Err(ClusterError::FailedWritingNodeID)
    }
    
}

pub fn read_node_id(p: &str) -> Result<String, ClusterError> {
    let path = NODE_ID_FILENAME;
    let path = Path::new(&path);
    let mut contents = String::new();
    match File::open(path) {
        Ok(mut f) => {
            match f.read_to_string(&mut contents) {
                Ok(_) => { 
                    Ok(contents)
                }
                Err(e) => { 
                    error!("Error reading node ID file: {:?}", e);
                    Err(ClusterError::FailedReadingNodeID)
                }
            }
        }
        Err(e) => {
            error!("Error reading node ID: {:?}", e);
            Err(ClusterError::FailedReadingNodeID)
        }
    }
}