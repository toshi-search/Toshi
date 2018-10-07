extern crate gotham;
extern crate pretty_env_logger;
extern crate toshi;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use toshi::commit::IndexWatcher;
use toshi::index::IndexCatalog;
use toshi::router::router_with_catalog;
use toshi::settings::{HEADER, SETTINGS};

pub fn main() {
    let code = runner();
    std::process::exit(code);
}

pub fn runner() -> i32 {
    std::env::set_var("RUST_LOG", &SETTINGS.log_level);
    pretty_env_logger::init();

    let index_catalog = match IndexCatalog::new(PathBuf::from(&SETTINGS.path)) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error Encountered - {}", e.to_string());
            std::process::exit(1);
        }
    };
    let catalog_arc = Arc::new(RwLock::new(index_catalog));
    let commit_watcher = IndexWatcher::new(Arc::clone(&catalog_arc));
    commit_watcher.start();
    let addr = format!("{}:{}", &SETTINGS.host, SETTINGS.port);

    println!("{}", HEADER);
    gotham::start(addr, router_with_catalog(&catalog_arc));

    0
}
