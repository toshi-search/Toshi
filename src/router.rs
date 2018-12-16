use hyper::Method;

use handlers::*;
use index::IndexCatalog;
use settings::VERSION;

use std::sync::{Arc, RwLock};

use tower_web::ServiceBuilder;
use tokio::prelude::*;

pub fn router_with_catalog(catalog: &Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> {
    let search_handler = SearchHandler::new(Arc::clone(catalog));
    let index_handler = IndexHandler::new(Arc::clone(catalog));
    let bulk_handler = BulkHandler::new(Arc::clone(catalog));
    let summary_handler = SummaryHandler::new(Arc::clone(catalog));
    let root_handler = RootHandler::new(VERSION.into());

    ServiceBuilder::new()
      .resource(search_handler)
      .resource(index_handler)
      .resource(bulk_handler)
      .resource(index_handler)
      .resource(root_handler)
      .serve(root_handler)
}
