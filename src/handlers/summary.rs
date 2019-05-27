use std::sync::Arc;

use futures::future;
use http::Response;
use serde::{Deserialize, Serialize};
use tantivy::space_usage::SearcherSpaceUsage;

use crate::error::Error;
use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::router::QueryOptions;
use crate::utils::with_body;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryResponse {
    summaries: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    segment_sizes: Option<SearcherSpaceUsage>,
}

#[derive(Clone)]
pub struct SummaryHandler {
    catalog: SharedCatalog,
}

impl SummaryHandler {
    pub fn new(catalog: SharedCatalog) -> Self {
        SummaryHandler { catalog }
    }

    pub fn summary(&self, index: String, options: QueryOptions) -> ResponseFuture {
        let index_lock = Arc::clone(&self.catalog);
        let fut = future::lazy(move || {
            let index_lock = index_lock.read().unwrap();
            if index_lock.exists(&index) {
                let index = index_lock.get_index(&index).unwrap();
                let metas = index.get_index().load_metas().unwrap();
                let segment_sizes = options.include_sizes.and_then(|b| if b { Some(index.get_space()) } else { None });
                let summaries = serde_json::to_value(&metas).unwrap();

                let summary = SummaryResponse { summaries, segment_sizes };
                future::ok(with_body(summary))
            } else {
                let err = Error::IOError(format!("Index {} does not exist", index));
                let resp = Response::from(err);
                future::ok(resp)
            }
        });

        Box::new(fut)
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

        let resp = handler
            .summary("test_index".into(), QueryOptions::default())
            .wait()
            .unwrap()
            .into_body()
            .concat2()
            .wait();
        let summary: SummaryResponse = serde_json::from_slice(&resp.unwrap()).unwrap();
        println!("{:#?}", summary);
        //      assert_eq!(resp.is_ok(), true)
    }
}
