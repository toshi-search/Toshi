use futures::future;
use serde::Serialize;
use std::sync::{Arc, RwLock};

use crate::error::Error;
use crate::handlers::ResponseFuture;
use crate::index::IndexCatalog;
use crate::router::with_body;

#[derive(Clone, Serialize)]
pub struct SummaryResponse {
    summaries: serde_json::Value,
}

#[derive(Clone)]
pub struct SummaryHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl SummaryHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        SummaryHandler { catalog }
    }

    pub fn summary(&self, index: String) -> ResponseFuture {
        let index_lock = self.catalog.read().unwrap();
        if index_lock.exists(&index) {
            let index = index_lock.get_index(&index).unwrap();
            let metas = index.get_index().load_metas().unwrap();
            let value = serde_json::to_value(&metas).unwrap();
            let summary = SummaryResponse { summaries: value };
            Box::new(future::ok(with_body(summary)))
        } else {
            Box::new(future::err(Error::IOError(format!("Index {} does not exist", index)).into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::tests::*;
    use futures::{Future, Stream};

    #[test]
    fn get_summary_data() {
        let cat = create_test_catalog("test_index");
        let handler = SummaryHandler::new(Arc::clone(&cat));

        let resp = handler.summary("test_index".into()).wait().unwrap().into_body().concat2().wait();
        assert_eq!(resp.is_ok(), true)
    }

}
