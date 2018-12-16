use crate::handlers::{CreatedResponse, Error};
use crate::index::IndexCatalog;

use std::str::from_utf8;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use tantivy::Document;
use tantivy::IndexWriter;

use crossbeam::channel::{unbounded, Receiver};
use std::iter::Iterator;

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
        fn handle(&self, body: Vec<u8>, index: String) -> Result<CreatedResponse, ()> {
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
                                match schema_clone.parse_document(text) {
                                    Ok(doc) => doc_sender.send(doc).unwrap(),
                                    Err(_) => ()
                                };
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
    use index::tests::*;
    use index::IndexCatalog;

    use handlers::search::tests::TestResults;
    use hyper::StatusCode;
    use serde_json;

    // TODO: Need Error coverage testing here.

    #[test]
    fn test_bulk_index() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let server = create_test_server(&Arc::new(RwLock::new(catalog)));

        let body = r#"
        {"test_text": "asdf1234", "test_i64": 123, "test_u64": 321, "test_unindex": "asdf"}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678, "test_unindex": "asdf"}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901, "test_unindex": "asdf"}"#;

        let req = server
            .client()
            .post("http://localhost/test_index/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::CREATED);

        // Give it a second...
        std::thread::sleep(std::time::Duration::from_secs(1));

        let check_docs = server.client().get("http://localhost/test_index").perform().unwrap();
        let docs: TestResults = serde_json::from_slice(&check_docs.read_body().unwrap()).unwrap();

        assert_eq!(docs.hits, 8);
        // TODO: Do more testing here.
    }
}
