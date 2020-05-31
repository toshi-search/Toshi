use std::str::from_utf8;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, unbounded};
use futures::StreamExt;
use hyper::Body;
use hyper::StatusCode;
use log::*;
use tantivy::{Document, IndexWriter};
use tantivy::schema::Schema;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::Barrier;

use toshi_types::{Catalog, Error};

use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::utils::{empty_with_code, error_response, not_found};

async fn index_documents(iw: Arc<Mutex<IndexWriter>>, dr: Receiver<Document>, wr: Arc<AtomicBool>) -> Result<(), Error> {
    let start = Instant::now();
    while let Ok(doc) = dr.recv_timeout(Duration::from_millis(100)) {
        let w = iw.lock().await;

        w.add_document(doc);
    }

    info!("Piping Documents took: {:?}", start.elapsed());
    wr.store(false, Ordering::SeqCst);
    Ok(())
}

async fn parsing_documents(
    s: Schema,
    ds: Sender<Document>,
    lr: Receiver<Vec<u8>>,
    error_chan: Sender<Error>,
    wait_barrier: Arc<Barrier>,
) -> Result<(), ()> {
    while let Ok(line) = lr.recv_timeout(Duration::from_millis(100)) {
        if !line.is_empty() {
            if let Ok(text) = String::from_utf8(line) {
                match s.parse_document(&text) {
                    Ok(doc) => {
                        ds.send(doc);
                    }
                    Err(e) => {
                        // let text = &text.clone();
                        let err = anyhow::Error::msg("Bad JSON")
                            // .context(text.trim())
                            .context(e);
                        error_chan.send(Error::TantivyError(err));
                    }
                };
            } else {
                error_chan.send(Error::TantivyError(anyhow::Error::msg("Bad UTF-8 Line")));
            }
        }
    }
    wait_barrier.wait().await;
    Ok(())
}

pub async fn bulk_insert(catalog: SharedCatalog, watcher: Arc<AtomicBool>, mut body: Body, index: &str) -> ResponseFuture {
    info!("Starting...{:?}", index);
    if !catalog.exists(index) {
        return not_found().await;
    }
    watcher.store(true, Ordering::SeqCst);
    let index_handle = catalog.get_index(index).unwrap();
    let index = index_handle.get_index();
    let schema = index.schema();
    let (line_sender, line_recv) = catalog.settings.get_channel::<Vec<u8>>();
    let (doc_sender, doc_recv) = unbounded::<Document>();
    let writer = index_handle.get_writer();
    let num_threads = catalog.settings.json_parsing_threads;

    let wait_barrier = Arc::new(Barrier::new(num_threads));
    let (err_snd, err_rcv) = unbounded();
    let watcher_clone = Arc::clone(&watcher);
    let mut parsing_handles = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let schema = schema.clone();
        let doc_sender = doc_sender.clone();
        let line_recv = line_recv.clone();
        parsing_handles.push(tokio::spawn(parsing_documents(
            schema.clone(),
            doc_sender.clone(),
            line_recv.clone(),
            err_snd.clone(),
            Arc::clone(&wait_barrier),
        )));
    }

    let mut buf = Vec::new();
    let mut remaining = Vec::new();
    while let Some(Ok(line)) = body.next().await {
        buf.extend(line);

        let mut split = buf.split(|b| *b == b'\n').peekable();

        while let Some(l) = split.next() {
            if split.peek().is_none() {
                remaining = l.to_vec();
            }
            debug!("Bytes in buf: {}", buf.len());
            line_sender.send(l.to_vec()).expect("Line sender failed.");
        }
    }

    if !remaining.is_empty() {
        line_sender.send(remaining).expect("Line sender failed #2");
    }

    futures::future::join_all(parsing_handles).await;
    if !err_rcv.is_empty() {
        dbg!(err_rcv.recv());
    }

     match index_documents(writer, doc_recv, watcher_clone).await {
        Ok(_) => Ok(empty_with_code(StatusCode::CREATED)),
        Err(err) => Ok(error_response(StatusCode::BAD_REQUEST, err))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::thread::sleep;
    use std::time::Duration;

    use toshi_test::read_body;

    use crate::{SearchResults, setup_logging_from_file};
    use crate::handlers::all_docs;
    use crate::handlers::summary::flush;
    use crate::index::create_test_catalog;

    use super::*;

    #[tokio::test(threaded_scheduler)]
    async fn test_bulk_index() -> Result<(), Box<dyn std::error::Error>> {
        // let log = setup_logging_from_file("")?;
        // let _scope = slog_scope::set_global_logger(log.clone());
        // let _guard = slog_stdlog::init_with_level(log::Level::from_str("INFO")?)?;

        let server = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));

        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf", "test_facet": "/cat/cat4"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index".into()).await?;
        assert_eq!(index_docs.status(), StatusCode::CREATED);
        // sleep(Duration::from_secs_f32(5.0));

        let f = flush(Arc::clone(&server), "test_index").await?;
        flush(Arc::clone(&server), "test_index").await?;

        assert_eq!(f.status(), StatusCode::OK);
        // sleep(Duration::from_secs_f32(1.0));

        let mut attempts: u32 = 0;
        for _ in 0..5 {
            let check_docs = all_docs(Arc::clone(&server), "test_index".into()).await?;
            let body: String = read_body(check_docs).await?;
            let docs: SearchResults = serde_json::from_slice(body.as_bytes())?;
            log::info!("Hits: {}", docs.hits);

            if docs.hits == 9 {
                break;
            }
            attempts += 1;
        }
        assert_eq!(attempts >= 5, false);
        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_bad_json() -> Result<(), Box<dyn std::error::Error>> {
        // let log = setup_logging_from_file("")?;
        // let _scope = slog_scope::set_global_logger(log.clone());
        // let _guard = slog_stdlog::init_with_level(log::Level::from_str("INFO")?)?;

        let server = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));

        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": -901, "test_unindex": "asdf", "test_facet": "/cat/cat4"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index".into()).await?;
        assert_eq!(index_docs.status(), StatusCode::CREATED);
        // sleep(Duration::from_secs_f32(5.0));

        let f = flush(Arc::clone(&server), "test_index").await?;
        flush(Arc::clone(&server), "test_index").await?;

        assert_eq!(f.status(), StatusCode::OK);
        // sleep(Duration::from_secs_f32(1.0));

        let mut attempts: u32 = 0;
        for _ in 0..5 {
            let check_docs = all_docs(Arc::clone(&server), "test_index".into()).await?;
            let body: String = read_body(check_docs).await?;
            let docs: SearchResults = serde_json::from_slice(body.as_bytes())?;
            log::info!("Hits: {}", docs.hits);

            if docs.hits == 9 {
                break;
            }
            attempts += 1;
        }
        assert_eq!(attempts >= 5, false);
        Ok(())
    }
}
