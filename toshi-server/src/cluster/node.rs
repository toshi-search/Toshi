use std::io;
use std::path::Path;

use tokio::fs::File;
use tokio::prelude::*;
use uuid::Uuid;

static NODE_ID_FILENAME: &str = ".node_id";

/// Init the node id by reading the node id from path or writing a fresh one if not found
pub async fn init_node_id(path: String) -> Result<String, io::Error> {
    let result = read_node_id(path.as_ref()).await;
    let id = match result {
        Ok(id) => Uuid::parse_str(&id).expect("Parsed node ID is not a UUID."),
        Err(_) => Uuid::new_v4(),
    };
    write_node_id(path, id.to_hyphenated().to_string()).await
}

/// Write node id to the path `p` provided, this will also append `.node_id`
pub async fn write_node_id(p: String, id: String) -> Result<String, io::Error> {
    // Append .node_id to the path provided
    let path = Path::new(&p).join(&NODE_ID_FILENAME);
    // Create and write the id to the file and return the id
    let mut file = File::create(path).await?;
    file.write_all(id.as_bytes()).await?;
    Ok(id)
}

/// Read the node id from the file provided
///
/// Note:This function will try and Read the file as UTF-8
pub async fn read_node_id(p: &str) -> Result<String, io::Error> {
    // Append .node_id to the provided path
    let path = Path::new(p).join(&NODE_ID_FILENAME);

    // Open an read the string to the end of the file and try to read it as UTF-8
    let mut file = File::open(path).await?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await?;
    String::from_utf8(contents).map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_id() -> Result<(), io::Error> {
        let write_id = init_node_id("./".to_string()).await?;
        let read_id = read_node_id("./").await?;
        assert_eq!(read_id, write_id);
        std::fs::remove_file(format!("./{}", NODE_ID_FILENAME)).unwrap();
        Ok(())
    }
}
