use std::str::from_utf8;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::StreamExt;
use hyper::Body;
use hyper::StatusCode;
use tantivy::schema::Schema;
use tantivy::{Document, IndexWriter};
use tokio::sync::Mutex;
use tracing::*;
use tracing_futures::Instrument;

use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::utils::empty_with_code;

async fn index_documents(iw: Arc<Mutex<IndexWriter>>, dr: Receiver<Document>, wr: Arc<AtomicBool>) -> Result<(), ()> {
    let parsing_span = info_span!("PipingDocuments");
    let _enter = parsing_span.enter();
    let start = Instant::now();
    let w = iw.lock().await;
    for doc in dr {
        w.add_document(doc);
    }

    info!("Piping Documents took: {:?}", start.elapsed());
    wr.store(false, Ordering::SeqCst);
    Ok(())
}

async fn parsing_documents(s: Schema, ds: Sender<Document>, lr: Receiver<Vec<u8>>) -> Result<(), ()> {
    let parsing_span = info_span!("ParsingDocs");
    let _enter = parsing_span.enter();
    for line in lr {
        if !line.is_empty() {
            if let Ok(text) = from_utf8(&line) {
                if let Ok(doc) = s.parse_document(text) {
                    info!("Sending doc: {:?}", &doc);
                    ds.send(doc).unwrap()
                }
            }
        }
    }
    info!("Done parsing docs...");
    Ok(())
}

pub async fn bulk_insert(catalog: SharedCatalog, watcher: Arc<AtomicBool>, mut body: Body, index: String) -> ResponseFuture {
    let span = info_span!("BulkInsert");
    let _enter = span.enter();
    watcher.store(true, Ordering::SeqCst);
    let index_lock = catalog.lock().await;
    let index_handle = index_lock.get_index(&index).unwrap();
    let index = index_handle.get_index();
    let schema = index.schema();
    let (line_sender, line_recv) = index_lock.settings.get_channel::<Vec<u8>>();
    let (doc_sender, doc_recv) = unbounded::<Document>();
    let writer = index_handle.get_writer();
    let num_threads = index_lock.settings.json_parsing_threads;
    let line_sender_clone = line_sender.clone();
    let watcher_clone = Arc::clone(&watcher);

    for _ in 0..num_threads {
        let schema = schema.clone();
        let doc_sender = doc_sender.clone();
        let line_recv = line_recv.clone();

        tokio::spawn(parsing_documents(schema.clone(), doc_sender.clone(), line_recv.clone()).in_current_span());
    }

    let mut buf = Vec::new();
    let mut remaining = vec![];
    while let Some(Ok(line)) = body.next().await {
        buf.extend(line);

        let mut split = buf.split(|b| *b == b'\n').peekable();

        while let Some(l) = split.next() {
            if split.peek().is_none() {
                remaining = l.to_vec();
            }
            debug!("Bytes in buf: {}", buf.len());
            line_sender_clone.send(l.to_vec()).expect("Line sender failed.");
        }
    }

    if !remaining.is_empty() {
        line_sender.send(remaining).expect("Line sender failed #2");
    }
    tokio::spawn(index_documents(writer, doc_recv, watcher_clone).in_current_span());
    Ok(empty_with_code(StatusCode::CREATED))
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use toshi_test::read_body;

    use crate::handlers::all_docs;
    use crate::handlers::summary::flush;
    use crate::index::tests::*;
    use crate::SearchResults;

    use super::*;

    #[tokio::test(threaded_scheduler)]
    async fn test_bulk_index() -> Result<(), Box<dyn std::error::Error>> {
        let server = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));

        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf"}"#;

        let index_docs = bulk_insert(Arc::clone(&server), lock, Body::from(body), "test_index".into()).await?;
        assert_eq!(index_docs.status(), StatusCode::CREATED);
        sleep(Duration::from_secs_f32(0.1));

        let flush = flush(Arc::clone(&server), "test_index".to_string()).await?;
        assert_eq!(flush.status(), StatusCode::OK);
        sleep(Duration::from_secs_f32(0.1));

        let mut attempts = 0;
        for _ in 0..5 {
            let check_docs = all_docs(Arc::clone(&server), "test_index".into()).await?;
            let body = read_body(check_docs).await?;
            let docs: SearchResults = serde_json::from_slice(body.as_bytes())?;
            println!("DOCS = {}", docs.hits);
            if docs.hits == 9 {
                break;
            }
            attempts += 1;
        }
        assert_eq!(attempts >= 5, false);
        Ok(())
    }
}
