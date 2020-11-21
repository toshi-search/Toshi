use hyper::{Response, StatusCode};
use log::{debug, info};
use serde::Serialize;
use std::time::Instant;

use toshi_types::*;

use crate::handlers::ResponseFuture;
use crate::router::QueryOptions;
use crate::utils::{empty_with_code, with_body};
use std::sync::Arc;

#[derive(Serialize)]
struct FlushResponse {
    opstamp: u64,
}

pub async fn index_summary<C: Catalog>(catalog: Arc<C>, index: &str, options: QueryOptions) -> ResponseFuture {
    let start = Instant::now();
    if catalog.exists(index) {
        let index = catalog.get_index(index).unwrap();
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
    if catalog.exists(index) {
        let local_index = catalog.get_index(index).unwrap();
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use http::Request;
    use hyper::Body;

    use toshi_test::{read_body, TestServer};

    use crate::index::create_test_catalog;
    use crate::router::Router;

    #[tokio::test]
    async fn get_summary_data() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let router = Router::new(catalog, Arc::new(AtomicBool::new(false)));
        let (list, ts) = TestServer::new()?;
        let request = Request::get(ts.uri("/test_index/_summary?include_sizes=true")?).body(Body::empty())?;
        let req = ts.get(request, router.router_from_tcp(list)).await?;
        let _body = read_body(req).await?;
        Ok(())
    }
}
