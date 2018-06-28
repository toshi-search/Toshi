use std::{fs::create_dir, path::PathBuf};
use tantivy::{schema::Schema, Index, Result};

pub fn get_index(path: &str, schema: Option<&Schema>) -> Result<Index> {
    let p = PathBuf::from(path);
    if p.exists() {
        Index::open_in_dir(p)
    } else {
        if let Some(s) = schema {
            create_dir(p).unwrap();
            Index::create_in_dir(path, s.clone())
        } else {
            panic!(":(");
        }
    }
}
