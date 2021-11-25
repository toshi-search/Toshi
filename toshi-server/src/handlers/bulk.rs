use bytes::BytesMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flume::{unbounded, Receiver, Sender};
use futures::StreamExt;
use hyper::Body;
use hyper::StatusCode;

use log::*;
use tantivy::schema::Schema;
use tantivy::{Document, IndexWriter};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, LinesCodec, LinesCodecError};

use toshi_types::{Catalog, Error, IndexHandle};

use crate::handlers::ResponseFuture;
use crate::utils::{empty_with_code, error_response, not_found};

const DEFAULT_TIMEOUT: Duration = Duration::from_millis(100);

async fn index_documents(iw: Arc<Mutex<IndexWriter>>, dr: Receiver<Document>, wr: Arc<AtomicBool>) -> Result<(), Error> {
    let start = Instant::now();
    while let Ok(Ok(doc)) = timeout(DEFAULT_TIMEOUT, dr.recv_async()).await {
        let w = iw.lock().await;
        w.add_document(doc);
    }

    info!("Piping Documents took: {:?}", start.elapsed());
    wr.store(false, Ordering::SeqCst);
    Ok(())
}

async fn parsing_documents(s: Schema, ds: Sender<Document>, lr: Receiver<String>, ec: Sender<Error>) -> Result<(), ()> {
    while let Ok(Ok(line)) = timeout(DEFAULT_TIMEOUT, lr.recv_async()).await {
        if !line.is_empty() {
            match s.parse_document(&line) {
                Ok(doc) => {
                    info!("Piped document... {}", doc.len());
                    ds.send_async(doc).await;
                }
                Err(e) => {
                    let err = anyhow::Error::msg("Error parsing document").context(line).context(e);
                    ec.send_async(Error::TantivyError(err)).await;
                    break;
                }
            };
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
    max_line_length: usize,
) -> ResponseFuture {
    if !catalog.exists(index) {
        return not_found().await;
    }
    watcher.store(true, Ordering::SeqCst);
    let index_handle = catalog.get_index(index).unwrap();
    let writer = index_handle.get_writer();
    let i = index_handle.get_index();
    let schema = i.schema();

    let (line_sender, line_recv) = unbounded::<String>();
    let (doc_sender, doc_recv) = unbounded::<Document>();
    let (err_snd, err_rcv) = unbounded();

    info!("Spawning {} parsing threads...", num_threads);
    let mut parsing_handles = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let schema = schema.clone();
        let doc_sender = doc_sender.clone();
        let line_recv = line_recv.clone();
        let err_snd = err_snd.clone();
        parsing_handles.push(tokio::spawn(parsing_documents(schema, doc_sender, line_recv, err_snd)));
    }
    info!("Spawned threads finished...");
    let mut buf = BytesMut::new();
    let mut decoder = if max_line_length > 0 {
        LinesCodec::new_with_max_length(max_line_length)
    } else {
        LinesCodec::new()
    };

    while let Some(Ok(line)) = body.next().await {
        buf.extend_from_slice(&line);

        loop {
            match decoder.decode_eof(&mut buf) {
                Ok(Some(l)) if !l.is_empty() => {
                    let l = l.trim();
                    line_sender.send_async(l.into()).await.unwrap();
                }
                Ok(None) | Ok(Some(_)) => break,
                Err(LinesCodecError::MaxLineLengthExceeded) => {
                    let err_txt = format!(
                        "Line exceeded max length of {}, you can increase this with the max_line_length config option",
                        max_line_length
                    );
                    let err_msg = anyhow::Error::msg(err_txt);
                    return Ok(error_response(StatusCode::BAD_REQUEST, Error::TantivyError(err_msg)));
                }
                Err(err) => {
                    let err_msg = anyhow::Error::msg("Error with codec.").context(err);
                    return Ok(error_response(StatusCode::BAD_REQUEST, Error::TantivyError(err_msg)));
                }
            }
        }
    }

    futures::future::join_all(parsing_handles).await;
    if !err_rcv.is_empty() {
        let mut iw = writer.lock().await;
        iw.rollback()
            .unwrap_or_else(|e| panic!("Error rolling back index: {}, this should be reported as a bug. {}", index, e));
        match err_rcv.recv_async().await {
            Ok(err) => return Ok(error_response(StatusCode::BAD_REQUEST, err)),
            Err(err) => panic!("Panic receiving error: {:?}", err),
        }
    }

    match index_documents(writer, doc_recv, Arc::clone(&watcher)).await {
        Ok(_) => Ok(empty_with_code(StatusCode::CREATED)),
        Err(err) => Ok(error_response(StatusCode::BAD_REQUEST, err)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::commit::tests::read_body;
    use crate::handlers::all_docs;
    use crate::handlers::summary::flush;
    use crate::index::create_test_catalog;
    use crate::SearchResults;

    use super::*;

    #[tokio::test]
    async fn test_bulk_index() -> Result<(), Box<dyn std::error::Error>> {
        let server = create_test_catalog("test_index_bulk");
        let lock = Arc::new(AtomicBool::new(false));

        let body = r#"{"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf", "test_facet": "/cat/cat4"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index_bulk", 2, 2048).await?;
        assert_eq!(index_docs.status(), StatusCode::CREATED);

        let f = flush(Arc::clone(&server), "test_index_bulk").await?;

        assert_eq!(f.status(), StatusCode::OK);

        std::thread::sleep(Duration::from_secs(1));
        let check_docs = all_docs(Arc::clone(&server), "test_index_bulk").await?;
        let body: String = read_body(check_docs).await?;
        let docs: SearchResults = serde_json::from_slice(body.as_bytes())?;

        assert_eq!(docs.hits, 8);
        Ok(())
    }

    #[tokio::test]
    async fn test_errors() -> Result<(), Box<dyn std::error::Error>> {
        let server = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));

        let body: &str = r#"{"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf", "test_facet": "/cat/cat4"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": -9, "test_unindex": "asdf", "test_facet": "/cat/cat4"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index", 2, 2048).await?;
        assert_eq!(index_docs.status(), StatusCode::BAD_REQUEST);

        let body = read_body(index_docs).await?;
        println!("{}", body);
        Ok(())
    }
}
