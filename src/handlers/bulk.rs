use std::iter::Iterator;
use std::str::from_utf8;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::stream::iter_ok;
use futures::{Future, Stream};
use tantivy::schema::Schema;
use tantivy::Document;
use tantivy::IndexWriter;
use tower_web::*;

use crate::error::Error;
use crate::handlers::CreatedResponse;
use crate::index::IndexCatalog;

#[derive(Clone)]
pub struct BulkHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl BulkHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        BulkHandler { catalog }
    }

    fn index_documents(index_writer: &Mutex<IndexWriter>, doc_receiver: Receiver<Document>) -> Result<u64, Error> {
        match index_writer.lock() {
            Ok(ref mut w) => {
                for doc in doc_receiver {
                    w.add_document(doc);
                }
                match w.commit() {
                    Ok(c) => Ok(c),
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    fn parsing_documents(schema: Schema, doc_sender: Sender<Document>, line_recv: Receiver<Vec<u8>>) -> Result<(), Error> {
        for line in line_recv {
            if !line.is_empty() {
                if let Ok(text) = from_utf8(&line) {
                    if let Ok(doc) = schema.parse_document(text) {
                        log::debug!("Sending doc: {:?}", &doc);
                        doc_sender.send(doc)?
                    }
                }
            }
        }
        Ok(())
    }

    fn inner_handle(&self, body: Vec<u8>, index: String) -> impl Future<Item = CreatedResponse, Error = Error> + Send {
        let index_lock = self.catalog.read().unwrap();
        let index_handle = index_lock.get_index(&index).unwrap();
        let index = index_handle.get_index();
        let schema = index.schema();
        let (line_sender, line_recv) = index_lock.settings.get_channel::<Vec<u8>>();
        let (doc_sender, doc_recv) = unbounded::<Document>();

        for _ in 0..index_lock.settings.json_parsing_threads {
            let schema_clone = schema.clone();
            let doc_sender = doc_sender.clone();
            let line_recv_clone = line_recv.clone();
            thread::spawn(move || BulkHandler::parsing_documents(schema_clone, doc_sender, line_recv_clone));
        }
        let writer = index_handle.get_writer();
        thread::spawn(move || BulkHandler::index_documents(&writer, doc_recv));

        let line_sender_clone = line_sender.clone();
        iter_ok(body.into_iter())
            .fold(Vec::new(), move |mut buf, line| {
                buf.push(line);
                let mut split = buf.split(|b| *b == b'\n').peekable();
                while let Some(l) = split.next() {
                    if split.peek().is_none() {
                        return Ok(l.to_vec());
                    }

                    log::debug!("Bytes in buf: {}", buf.len());
                    match line_sender_clone.send(l.to_vec()) {
                        Ok(_) => (),
                        Err(e) => return Err(e),
                    }
                }
                Ok(buf.clone())
            })
            .and_then(move |left| {
                log::debug!("Bytes left in buf: {}", left.len());
                if !left.is_empty() {
                    line_sender.send(left).map(|_| CreatedResponse)
                } else {
                    Ok(CreatedResponse)
                }
            })
            .map_err(|e| e.into())
    }
}

impl_web! {
    impl BulkHandler {
        #[post("/:index/_bulk")]
        #[content_type("application/json")]
        pub fn handle(&self, body: Vec<u8>, index: String) -> impl Future<Item = CreatedResponse, Error = Error> + Send {
            self.inner_handle(body, index)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use tokio::runtime::Runtime;

    use crate::handlers::SearchHandler;
    use crate::index::tests::*;

    use super::*;

    #[test]
    fn test_bulk_index() {
        let mut runtime = Runtime::new().unwrap();
        let server = create_test_catalog("test_index");
        let handler = BulkHandler::new(Arc::clone(&server));
        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf"}"#;

        let index_docs = handler.handle(body.as_bytes().to_vec(), "test_index".into());
        let result = runtime.block_on(index_docs);
        assert_eq!(result.is_ok(), true);
        sleep(Duration::from_secs(1));

        let search = SearchHandler::new(Arc::clone(&server));
        let check_docs = search.get_all_docs("test_index".into()).wait().unwrap();
        assert_eq!(check_docs.hits, 8);
    }
}
