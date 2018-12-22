use crate::handlers::{CreatedResponse, Error};
use crate::index::IndexCatalog;

use std::iter::Iterator;
use std::str::from_utf8;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use crossbeam::channel::{unbounded, Receiver};
use tantivy::Document;
use tantivy::IndexWriter;

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
}

impl_web! {
    impl BulkHandler {
        #[post("/:index/_bulk")]
        #[content_type("application/json")]
        pub fn handle(&self, body: Vec<u8>, index: String) -> Result<CreatedResponse, ()> {

            let index_lock = self.catalog.read().map_err(|_| ())?;
            let index_handle = index_lock.get_index(&index).map_err(|_| ())?;
            let index = index_handle.get_index();
            let schema = index.schema();
            let (line_sender, line_recv) = index_lock.settings.get_channel::<Vec<u8>>();
            let (doc_sender, doc_recv) = unbounded::<Document>();

            for _ in 0..index_lock.settings.json_parsing_threads {
                let schema_clone = schema.clone();
                let doc_sender = doc_sender.clone();
                let line_recv_clone = line_recv.clone();
                thread::spawn(move || {
                    for line in line_recv_clone {
                        if !line.is_empty() {
                            if let Ok(text) = from_utf8(&line) {
                                if let Ok(doc) = schema_clone.parse_document(text) {
                                    doc_sender.send(doc).unwrap()
                                }
                            }
                        }
                    }
                });
            }

            let writer = index_handle.get_writer();
            thread::spawn(move || BulkHandler::index_documents(&writer, doc_recv));

            let line_sender_clone = line_sender.clone();
            let response = body
                .into_iter()
                .fold(Vec::new(), move |mut buf, line| {
                    buf.push(line);
                    let mut split = buf.split(|b| *b == b'\n').peekable();
                    while let Some(l) = split.next() {
                        if split.peek().is_none() {
                            return l.to_vec();
                        }
                        line_sender_clone.send(l.to_vec()).unwrap()
                    }
                    buf.clone()
                });
            if !response.is_empty() {
                line_sender.send(response.to_vec()).unwrap();
            }
            Ok(CreatedResponse)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use handlers::SearchHandler;
    use index::tests::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_bulk_index() {
        let server = create_test_catalog("test_index");
        let handler = BulkHandler::new(Arc::clone(&server));
        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf"}"#;

        let index_docs = handler.handle(body.as_bytes().to_vec(), "test_index".into());
        assert_eq!(index_docs.is_ok(), true);
        sleep(Duration::from_secs(1));

        let search = SearchHandler::new(Arc::clone(&server));
        let check_docs = search.get_all_docs("test_index".into()).unwrap();
        assert_eq!(check_docs.hits, 8);
    }
}
