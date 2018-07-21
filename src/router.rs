use gotham::router::builder::*;
use gotham::router::Router;
use hyper::{Get, Post, Put};

use handlers::index::IndexHandler;
use handlers::root::RootHandler;
use handlers::search::SearchHandler;
use handlers::{IndexPath, QueryOptions};
use index::IndexCatalog;
use settings::VERSION;

use handlers::bulk::BulkHandler;
use std::sync::Arc;
use std::sync::RwLock;

pub fn router_with_catalog(catalog: &Arc<RwLock<IndexCatalog>>) -> Router {
    let search_handler = SearchHandler::new(Arc::clone(catalog));
    let index_handler = IndexHandler::new(Arc::clone(catalog));
    let bulk_handler = BulkHandler::new(Arc::clone(catalog));
    let root_handler = RootHandler::new(format!("Toshi Search, Version: {}", VERSION));

    build_simple_router(|route| {
        route.get("/").to_new_handler(root_handler);

        route
            .request(vec![Post, Get], "/:index")
            .with_path_extractor::<IndexPath>()
            .with_query_string_extractor::<QueryOptions>()
            .to_new_handler(search_handler);

        route
            .request(vec![Put], "/:index")
            .with_path_extractor::<IndexPath>()
            .with_query_string_extractor::<QueryOptions>()
            .to_new_handler(index_handler);

        route
            .post("/:index/_bulk")
            .with_path_extractor::<IndexPath>()
            .to_new_handler(bulk_handler);
    })
}
