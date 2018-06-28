use std::path::PathBuf;
use std::fs::create_dir;
use tantivy::schema::Schema;
use tantivy::{Index, Result};


pub fn get_index(path: &str, schema: &Schema) -> Result<Index> {
    let p = PathBuf::from(path);
    if p.exists() {
        Index::open_in_dir(p)
    } else {
        create_dir(p).unwrap();
        Index::create_in_dir(path, schema.clone())
    }
}
