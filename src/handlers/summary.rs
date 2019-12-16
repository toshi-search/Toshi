use std::sync::Arc;
use std::time::Instant;

use hyper::{Body, Response, StatusCode};
use serde::Serialize;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::IndexMeta;
use tracing::*;

use toshi_types::error::Error;

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

pub async fn index_summary(catalog: SharedCatalog, index: String, options: QueryOptions) -> ResponseFuture {
    let start = Instant::now();
    let span = span!(Level::INFO, "summary_handler", ?index, ?options);
    let _enter = span.enter();

    let index_lock = catalog.lock().await;
    if index_lock.exists(&index) {
        let index = index_lock.get_index(&index).unwrap();
        let metas = index.get_index().load_metas().unwrap();
        let summary = if options.include_sizes() {
            SummaryResponse::new(metas, Some(index.get_space()))
        } else {
            SummaryResponse::new(metas, None)
        };
        tracing::info!("Took: {:?}", start.elapsed());
        Ok(with_body(summary))
    } else {
        let err = Error::IOError(format!("Index {} does not exist", index));
        let resp: Response<Body> = Response::from(err);
        tracing::info!("Took: {:?}", start.elapsed());
        Ok(resp)
    }
}

pub async fn flush(catalog: SharedCatalog, index: String) -> ResponseFuture {
    let span = span!(Level::INFO, "flush_handler", ?index);
    let _enter = span.enter();
    let index_lock = catalog.lock().await;
    if index_lock.exists(&index) {
        let local_index = index_lock.get_index(&index).unwrap();
        let writer = local_index.get_writer();
        let mut write = writer.write();
        write.commit().unwrap();
        info!("Successful commit: {}", index);
        Ok(empty_with_code(StatusCode::OK))
    } else {
        error!("Could not find index: {}", index);
        Ok(empty_with_code(StatusCode::NOT_FOUND))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn get_summary_data() {}
}
