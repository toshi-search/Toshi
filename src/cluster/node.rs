use std::path::Path;

use futures::Future;
use tokio::fs::File;
use tokio::io::{read_to_end, write_all};
use uuid::Uuid;

use crate::cluster::{ClusterError};

static NODE_ID_FILENAME: &'static str = ".node_id";

/// Init the node id by reading the node id from path or writing a fresh one if not found
pub fn init_node_id(path: String) -> impl Future<Item = String, Error = ClusterError> {
    read_node_id(path.as_ref()).then(|result| {
        let id = match result {
            Ok(id) => Uuid::parse_str(&id).expect("Parsed node ID is not a UUID."),
            Err(_) => Uuid::new_v4(),
        };
        write_node_id(path, id.to_hyphenated().to_string())
    })
}

/// Write node id to the path `p` provided, this will also append `.node_id`
pub fn write_node_id(p: String, id: String) -> impl Future<Item = String, Error = ClusterError> {
    // Append .node_id to the path provided
    let path = Path::new(&p).join(&NODE_ID_FILENAME);
    // Create and write the id to the file and return the id
    File::create(path)
        .and_then(move |file| write_all(file, id))
        .map(|(_, id)| id)
        .map_err(|e| ClusterError::FailedWritingNodeID(format!("{}", e)))
}

/// Read the node id from the file provided
///
/// Note:This function will try and Read the file as UTF-8
pub fn read_node_id(p: &str) -> impl Future<Item = String, Error = ClusterError> {
    // Append .node_id to the provided path
    let path = Path::new(p).join(&NODE_ID_FILENAME);

    // Open an read the string to the end of the file and try to read it as UTF-8
    File::open(path)
        .and_then(|file| read_to_end(file, Vec::new()))
        .map_err(|e| ClusterError::FailedReadingNodeID(format!("{}", e)))
        .and_then(|(_, bytes)| String::from_utf8(bytes).map_err(|_| ClusterError::UnableToReadUTF8))
}