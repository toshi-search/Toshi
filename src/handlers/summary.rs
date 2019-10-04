use std::sync::Arc;
use std::time::Instant;

use futures::future;
use http::{Response, StatusCode};
use serde::Serialize;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::IndexMeta;
use tracing::{span, Level};
use tracing_futures::Instrument;

use crate::error::Error;
use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::router::QueryOptions;
use crate::utils::{empty_with_code, with_body};

#[derive(Debug, Serialize)]
pub struct SummaryResponse {
    summaries: IndexMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    segment_sizes: Option<SearcherSpaceUsage>,
}

impl SummaryResponse {
    pub fn new(summaries: IndexMeta, segment_sizes: Option<SearcherSpaceUsage>) -> Self {
        Self { summaries, segment_sizes }
    }
}

pub fn summary(catalog: SharedCatalog, index: String, options: QueryOptions) -> ResponseFuture {
    let start = Instant::now();
    let span = span!(Level::INFO, "summary_handler", ?index, ?options);
    let index_lock = Arc::clone(&catalog);
    let fut = future::lazy(move || {
        let index_lock = index_lock.read();
        if index_lock.exists(&index) {
            let index = index_lock.get_index(&index).unwrap();
            let metas = index.get_index().load_metas().unwrap();
            let summary = if options.include_sizes() {
                SummaryResponse::new(metas, Some(index.get_space()))
            } else {
                SummaryResponse::new(metas, None)
            };
            tracing::info!("Took: {:?}", start.elapsed());
            future::ok(with_body(summary))
        } else {
            let err = Error::IOError(format!("Index {} does not exist", index));
            let resp = Response::from(err);
            tracing::info!("Took: {:?}", start.elapsed());
            future::ok(resp)
        }
    })
    .instrument(span);
    Box::new(fut)
}

pub fn flush(catalog: SharedCatalog, index: String) -> ResponseFuture {
    let index_lock = Arc::clone(&catalog);
    let fut = future::lazy(move || {
        let index_lock = index_lock.read();
        if index_lock.exists(&index) {
            let index = index_lock.get_index(&index).unwrap();
            let writer = index.get_writer();
            let mut write = writer.write();
            write.commit().unwrap();
            future::ok(empty_with_code(StatusCode::OK))
        } else {
            future::ok(empty_with_code(StatusCode::NOT_FOUND))
        }
    });
    Box::new(fut)
}

#[cfg(test)]
mod tests {
    use futures::{Future, Stream};

    use toshi_test::get_localhost;

    use crate::router::tests::TEST_SERVER;

    #[test]
    fn get_summary_data() {
        let addr = get_localhost();
        let resp = TEST_SERVER
            .client_with_address(addr)
            .get("http://localhost:8080/test_index/_summary?include_sizes=true")
            .perform()
            .unwrap()
            .into_body()
            .concat2()
            .wait();

        let resp2 = TEST_SERVER
            .client_with_address(addr)
            .get("http://localhost:8080/test_index/_summary")
            .perform()
            .unwrap()
            .into_body()
            .concat2()
            .wait();

        //        let summary: Result<SummaryResponse, serde_json::Error> = serde_json::from_slice(&resp);
        //        let summary2: Result<SummaryResponse, serde_json::Error> = serde_json::from_slice(&resp2);

        assert_eq!(resp.is_ok(), true);
        //        assert_eq!(summary.unwrap().segment_sizes.is_some(), true);
        assert_eq!(resp2.is_ok(), true);
        //        assert_eq!(summary2.unwrap().segment_sizes.is_none(), true);
    }
}
