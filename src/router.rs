use gotham::router::builder::*;
use gotham::router::Router;
use handlers::index::IndexHandler;
use handlers::root::RootHandler;
use handlers::search::SearchHandler;
use index::IndexCatalog;
use settings::{SETTINGS, VERSION};
use std::path::PathBuf;

use std::sync::{Arc, Mutex};

pub fn router() -> Router {
    let catalog = Arc::new(Mutex::new(IndexCatalog::new(PathBuf::from(&SETTINGS.path)).unwrap()));

    let search_handler = SearchHandler::new(catalog.clone());
    let index_handler = IndexHandler::new(catalog.clone());
    let handle = RootHandler::new(format!("Toshi Search, Version: {}", VERSION));

    build_simple_router(|route| {
        route.associate("/", |r| {
            r.get().to_new_handler(handle);
            r.put().to_new_handler(index_handler);
            r.post().to_new_handler(search_handler);
        });
        //route.post("/:index/create").to(||);
    })
}
