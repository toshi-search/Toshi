use futures::future;
use http::Response;
use serde::Serialize;

use crate::error::Error;
use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::router::with_body;

#[derive(Clone, Serialize)]
pub struct SummaryResponse {
    summaries: serde_json::Value,
}

#[derive(Clone)]
pub struct SummaryHandler {
    catalog: SharedCatalog,
}

impl SummaryHandler {
    pub fn new(catalog: SharedCatalog) -> Self {
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
            let err = Error::IOError(format!("Index {} does not exist", index));
            let resp = Response::from(err);
            Box::new(future::ok(resp))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use futures::{Future, Stream};

    use crate::index::tests::*;

    use super::*;

    #[test]
    fn get_summary_data() {
        let cat = create_test_catalog("test_index");
        let handler = SummaryHandler::new(Arc::clone(&cat));

        let resp = handler.summary("test_index".into()).wait().unwrap().into_body().concat2().wait();
        assert_eq!(resp.is_ok(), true)
    }

}
