extern crate gotham;
extern crate pretty_env_logger;
extern crate toshi;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use toshi::index::IndexCatalog;
use toshi::router::router_with_catalog;
use toshi::settings::{HEADER, SETTINGS};

pub fn main() {
    let log_level = SETTINGS.log_level.clone().unwrap_or_else(|| String::from("info"));
    std::env::set_var("RUST_LOG", &log_level);
    pretty_env_logger::init();
    println!("{}", HEADER);

    let catalog = Arc::new(RwLock::new(IndexCatalog::new(PathBuf::from(&SETTINGS.path)).unwrap()));
    let addr = format!("{}:{}", &SETTINGS.host, SETTINGS.port);
    gotham::start(addr, router_with_catalog(&catalog))
}
