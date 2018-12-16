use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio::prelude::*;
use tokio::net::TcpListener;
use tower_web::ServiceBuilder;

use handlers::*;
use index::IndexCatalog;
use settings::VERSION;

pub fn router_with_catalog(addr: &SocketAddr, catalog: &Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> {
    let search_handler = SearchHandler::new(Arc::clone(catalog));
//    let index_handler = IndexHandler::new(Arc::clone(catalog));
    let bulk_handler = BulkHandler::new(Arc::clone(catalog));
    let summary_handler = SummaryHandler::new(Arc::clone(catalog));
    let root_handler = RootHandler::new(VERSION.into());
    let listener = TcpListener::bind(addr).unwrap().incoming();

    ServiceBuilder::new()
      .resource(search_handler)
//      .resource(index_handler)
      .resource(bulk_handler)
      .resource(summary_handler)
      .resource(root_handler)
      .serve(listener)
}
