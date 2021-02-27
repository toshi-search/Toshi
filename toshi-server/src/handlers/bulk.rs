use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_channel::{unbounded, Receiver, Sender};
use futures::StreamExt;
use hyper::Body;
use hyper::StatusCode;
use log::*;
use tantivy::schema::Schema;
use tantivy::{Document, IndexWriter};
use tokio::sync::Mutex;

use toshi_types::{Catalog, Error, IndexHandle};

use crate::handlers::ResponseFuture;
use crate::utils::{empty_with_code, error_response, not_found};

async fn index_documents(iw: Arc<Mutex<IndexWriter>>, dr: Receiver<Document>, wr: Arc<AtomicBool>) -> Result<(), Error> {
    let start = Instant::now();

    while let Ok(Ok(doc)) = tokio::time::timeout(Duration::from_millis(100), dr.recv()).await {
        let w = iw.lock().await;

        w.add_document(doc);
    }

    info!("Piping Documents took: {:?}", start.elapsed());
    wr.store(false, Ordering::SeqCst);
    Ok(())
}

async fn parsing_documents(s: Schema, ds: Sender<Document>, lr: Receiver<Vec<u8>>, ec: Sender<Error>) -> Result<(), ()> {
    while let Ok(Ok(line)) = tokio::time::timeout(Duration::from_millis(100), lr.recv()).await {
        if !line.is_empty() {
            if let Ok(text) = String::from_utf8(line) {
                match s.parse_document(&text) {
                    Ok(doc) => {
                        ds.send(doc).await.unwrap_or_else(|e| panic!("Error in network channel: {}", e));
                    }
                    Err(e) => {
                        let err = anyhow::Error::msg("Error parsing document").context(text).context(e);
                        ec.send(Error::TantivyError(err))
                            .await
                            .unwrap_or_else(|e| panic!("Error in network channel: {}", e));
                    }
                };
            } else {
                ec.send(Error::TantivyError(anyhow::Error::msg("Bad UTF-8 Line")))
                    .await
                    .unwrap_or_else(|e| panic!("Error in network channel: {}", e));
            }
        }
    }
    Ok(())
}

pub async fn bulk_insert<C: Catalog>(
    catalog: Arc<C>,
    watcher: Arc<AtomicBool>,
    mut body: Body,
    index: &str,
    num_threads: usize,
) -> ResponseFuture {
    info!("Starting...{:?}", index);
    if !catalog.exists(index) {
        return not_found().await;
    }
    watcher.store(true, Ordering::SeqCst);
    let index_handle = catalog.get_index(index).unwrap();
    let i = index_handle.get_index();
    let schema = i.schema();
    let (line_sender, line_recv) = unbounded::<Vec<u8>>();
    let (doc_sender, doc_recv) = unbounded::<Document>();
    let writer = index_handle.get_writer();

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
            line_sender.send(l.to_vec()).await.expect("Line sender failed.");
        }
    }

    if !remaining.is_empty() {
        line_sender.send(remaining).await.expect("Line sender failed #2");
    }

    futures::future::join_all(parsing_handles).await;
    if !err_rcv.is_empty() {
        let mut iw = writer.lock().await;
        iw.rollback()
            .unwrap_or_else(|e| panic!("Error rolling back index: {}, this should be reported as a bug. {}", index, e));
        match err_rcv.recv().await {
            Ok(err) => return Ok(error_response(StatusCode::BAD_REQUEST, err)),
            Err(err) => panic!("Panic receiving error: {:?}", err),
        }
    }

    match index_documents(writer, doc_recv, watcher_clone).await {
        Ok(_) => Ok(empty_with_code(StatusCode::CREATED)),
        Err(err) => Ok(error_response(StatusCode::BAD_REQUEST, err)),
    }
}

#[cfg(test)]
mod tests {

    use crate::handlers::all_docs;
    use crate::handlers::summary::flush;
    use crate::index::create_test_catalog;
    use crate::SearchResults;

    use super::*;
    use crate::commit::tests::read_body;
    use std::time::Duration;

    #[tokio::test]
    async fn test_bulk_index() -> Result<(), Box<dyn std::error::Error>> {
        let server = create_test_catalog("test_index_bulk");
        let lock = Arc::new(AtomicBool::new(false));

        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf", "test_facet": "/cat/cat4"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index_bulk", 2).await?;
        assert_eq!(index_docs.status(), StatusCode::CREATED);

        let f = flush(Arc::clone(&server), "test_index_bulk").await?;

        assert_eq!(f.status(), StatusCode::OK);

        std::thread::sleep(Duration::from_secs(1));
        let check_docs = all_docs(Arc::clone(&server), "test_index_bulk").await?;
        let body: String = read_body(check_docs).await?;
        let docs: SearchResults = serde_json::from_slice(body.as_bytes())?;

        assert_eq!(docs.hits, 9);
        Ok(())
    }

    #[tokio::test]
    async fn test_errors() -> Result<(), Box<dyn std::error::Error>> {
        let server = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));

        let body: &str = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": -9, "test_unindex": "asdf", "test_facet": "/cat/cat4"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index", 2).await?;
        assert_eq!(index_docs.status(), StatusCode::BAD_REQUEST);

        let body = read_body(index_docs).await?;
        println!("{}", body);
        Ok(())
    }
}
