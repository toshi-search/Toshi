use hyper::{Response, StatusCode};
use log::{debug, info};
use serde::Serialize;
use std::time::Instant;

use toshi_types::*;

use crate::handlers::ResponseFuture;
use crate::utils::{empty_with_code, with_body};
use std::sync::Arc;

#[derive(Serialize)]
struct FlushResponse {
    opstamp: u64,
}

pub async fn index_summary<C: Catalog>(catalog: Arc<C>, index: &str, options: QueryOptions) -> ResponseFuture {
    let start = Instant::now();
    if let Ok(index) = catalog.get_index(index) {
        let metas = index.get_index().load_metas().unwrap();
        let summary = if options.include_sizes() {
            SummaryResponse::new(metas, Some(index.get_space()))
        } else {
            SummaryResponse::new(metas, None)
        };
        info!("Took: {:?}", start.elapsed());
        Ok(with_body(summary))
    } else {
        let resp = Response::from(Error::UnknownIndex(index.into()));
        info!("Took: {:?}", start.elapsed());
        Ok(resp)
    }
}

pub async fn flush<C: Catalog>(catalog: Arc<C>, index: &str) -> ResponseFuture {
    if let Ok(local_index) = catalog.get_index(index) {
        let writer = local_index.get_writer();
        let mut write = writer.lock().await;
        let opstamp = write.commit().unwrap();
        info!("Successful commit: {}", index);
        Ok(with_body(FlushResponse { opstamp }))
    } else {
        debug!("Could not find index: {}", index);
        Ok(empty_with_code(StatusCode::NOT_FOUND))
    }
}
