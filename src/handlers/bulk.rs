use std::iter::Iterator;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use crossbeam::channel::{unbounded, Receiver, Sender};
use http::StatusCode;
use hyper::Body;
use parking_lot::RwLock;
use tantivy::schema::Schema;
use tantivy::{Document, IndexWriter};
use tokio::prelude::*;
use tracing::*;

use crate::commit::IndexWatcher;
use crate::handlers::ResponseFuture;
use crate::index::IndexCatalog;
use crate::utils::empty_with_code;

#[derive(Clone)]
pub struct BulkHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
    watcher: Arc<IndexWatcher>,
}

impl BulkHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>, watcher: Arc<IndexWatcher>) -> Self {
        BulkHandler { catalog, watcher }
    }

    fn index_documents(
        index_writer: Arc<RwLock<IndexWriter>>,
        doc_receiver: Receiver<Document>,
        watcher: Arc<IndexWatcher>,
    ) -> impl Future<Item = (), Error = ()> {
        future::lazy(move || {
            let start = Instant::now();
            for doc in doc_receiver {
                let w = index_writer.read();
                w.add_document(doc);
            }

            info!("Piping Documents took: {:?}", start.elapsed());
            info!("Unlocking watcher...");
            Ok(watcher.bulk_unlock())
        })
    }

    fn parsing_documents(schema: Schema, doc_sender: Sender<Document>, line_recv: Receiver<Bytes>) -> impl Future<Item = (), Error = ()> {
        future::lazy(move || {
            for line in line_recv {
                if !line.is_empty() {
                    if let Ok(text) = from_utf8(&line) {
                        if let Ok(doc) = schema.parse_document(text) {
                            debug!("Sending doc: {:?}", &doc);
                            doc_sender.send(doc).unwrap()
                        }
                    }
                }
            }
            Ok(())
        })
    }

    pub fn bulk_insert(&self, body: Body, index: String) -> ResponseFuture {
        self.watcher.bulk_lock();
        let index_lock = self.catalog.read();
        let index_handle = index_lock.get_index(&index).unwrap();
        let index = index_handle.get_index();
        let schema = index.schema();
        let (line_sender, line_recv) = index_lock.settings.get_channel::<Bytes>();
        let (doc_sender, doc_recv) = unbounded::<Document>();
        let writer = index_handle.get_writer();
        let num_threads = index_lock.settings.json_parsing_threads;
        let line_sender_clone = line_sender.clone();

        let watcher_clone = Arc::clone(&self.watcher);
        let fut = body
            .fold(Bytes::new(), move |mut buf, line| -> Result<Bytes, hyper::Error> {
                for _ in 0..num_threads {
                    tokio::spawn(BulkHandler::parsing_documents(
                        schema.clone(),
                        doc_sender.clone(),
                        line_recv.clone(),
                    ));
                }

                buf.extend(line);

                let mut split = buf.split(|b| *b == b'\n').peekable();
                while let Some(l) = split.next() {
                    if split.peek().is_none() {
                        return Ok(Bytes::from(l));
                    }
                    debug!("Bytes in buf: {}", buf.len());
                    line_sender_clone.send(Bytes::from(l)).expect("Line sender failed.");
                }
                Ok(buf)
            })
            .and_then(move |left| {
                if !left.is_empty() {
                    line_sender.send(left).expect("Line sender failed #2");
                }
                tokio::spawn(BulkHandler::index_documents(writer, doc_recv, watcher_clone));

                future::ok(empty_with_code(StatusCode::CREATED))
            });

        Box::new(fut)
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use tokio::runtime::Runtime;

    use crate::error::Error;
    use crate::handlers::SearchHandler;
    use crate::index::tests::*;
    use crate::results::SearchResults;

    use super::*;

    #[test]
    fn test_bulk_index() -> Result<(), Error> {
        let mut runtime = Runtime::new().unwrap();
        let server = create_test_catalog("test_index");
        let watcher = Arc::new(IndexWatcher::new(Arc::clone(&server), 1));
        let handler = BulkHandler::new(Arc::clone(&server), Arc::clone(&watcher));
        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf"}"#;

        let index_docs = handler.bulk_insert(Body::from(body), "test_index".into());
        let result = runtime.block_on(index_docs);
        assert_eq!(result.is_ok(), true);
        sleep(Duration::from_secs(1));

        let search = SearchHandler::new(Arc::clone(&server));
        let check_docs = search.all_docs("test_index".into()).wait().expect("");
        let body = check_docs.into_body().concat2().wait()?;
        let docs: SearchResults = serde_json::from_slice(&body.into_bytes())?;
        Ok(assert_eq!(docs.hits, 8))
    }
}
