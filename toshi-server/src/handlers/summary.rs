use std::time::Instant;

use hyper::{Body, Response, StatusCode};
use tracing::*;

use toshi_types::*;

use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::router::QueryOptions;
use crate::utils::{empty_with_code, with_body};

pub async fn index_summary(catalog: SharedCatalog, index: &str, options: QueryOptions) -> ResponseFuture {
    let start = Instant::now();
    let span = span!(Level::INFO, "summary_handler", ?index, ?options);
    let _enter = span.enter();

    if catalog.exists(index) {
        let index = catalog.get_index(index).unwrap();
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

pub async fn flush(catalog: SharedCatalog, index: &str) -> ResponseFuture {
    let span = span!(Level::INFO, "flush_handler", ?index);
    let _enter = span.enter();
    if catalog.exists(index) {
        let local_index = catalog.get_index(index).unwrap();
        let writer = local_index.get_writer();
        let mut write = writer.lock().await;

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
        let router = Router::new(catalog, Arc::new(AtomicBool::new(false)), None);
        let (list, ts) = TestServer::new()?;
        let request = Request::get(ts.uri("/test_index/_summary?include_sizes=true")).body(Body::empty())?;
        let req = ts.get(request, router.router_from_tcp(list)).await?;
        let _body = read_body(req).await?;
        Ok(())
    }
}
