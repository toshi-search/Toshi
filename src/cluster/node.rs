use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

static NODE_ID_FILENAME: &'static str = ".node_id.txt";
static CLUSTER_NAME_FILENAME: &'static str = ".cluster_name.txt";

pub fn write_node_id(id: String, _p: &str) {
    let path = NODE_ID_FILENAME;
    let path = Path::new(&path);
    let mut file = File::create(path).unwrap();
    let _ = file.write_all(id.as_bytes());
}

pub fn read_node_id(p: &str) -> Option<String> {
    let path = NODE_ID_FILENAME;
    let path = Path::new(&path);
    let mut contents = String::new();
    match File::open(path) {
        Ok(mut f) => {
            match f.read_to_string(&mut contents) {
                Ok(_) => { return Some(contents); }
                Err(_) => { return None; }
            };
        }
        Err(_) => {
            return None;
        }
    }
}