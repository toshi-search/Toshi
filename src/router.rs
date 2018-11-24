use gotham::router::builder::*;
use gotham::router::Router;
use hyper::Method;

use handlers::*;
use index::IndexCatalog;
use settings::VERSION;

use std::sync::{Arc, RwLock};

macro_rules! router_builder {
    ($r:ident, $m:expr, $p:expr, $h:ident) => {
        $r.request($m, $p)
            .with_path_extractor::<IndexPath>()
            .with_query_string_extractor::<QueryOptions>()
            .to_new_handler($h);
    };
}

pub fn router_with_catalog(catalog: &Arc<RwLock<IndexCatalog>>) -> Router {
    let search_handler = SearchHandler::new(Arc::clone(catalog));
    let index_handler = IndexHandler::new(Arc::clone(catalog));
    let bulk_handler = BulkHandler::new(Arc::clone(catalog));
    let summary_handler = SummaryHandler::new(Arc::clone(catalog));
    let root_handler = RootHandler::new(VERSION.into());

    build_simple_router(|route| {
        route.get("/").to_new_handler(root_handler);
        router_builder!(route, vec![Method::POST, Method::GET], "/:index", search_handler);
        router_builder!(route, vec![Method::PUT, Method::DELETE], "/:index", index_handler);
        router_builder!(route, vec![Method::POST], "/:index/_bulk", bulk_handler);
        router_builder!(route, vec![Method::GET], "/:index/_summary", summary_handler);
    })
}
