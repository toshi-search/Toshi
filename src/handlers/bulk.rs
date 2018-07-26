use super::super::*;
use super::*;

use futures::future;
use futures::{Future, Stream};

use gotham::handler::*;
use gotham::state::FromState;

use std::io::Result as IOResult;
use std::panic::RefUnwindSafe;
use std::str::from_utf8;
use std::sync::RwLock;
use std::thread;

use tantivy::Document;
use tantivy::IndexWriter;

use crossbeam_channel::Receiver;

#[derive(Clone, Debug)]
pub struct BulkHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl RefUnwindSafe for BulkHandler {}

impl BulkHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self { BulkHandler { catalog } }

    fn index_documents(index_writer: &mut IndexWriter, doc_receiver: Receiver<Document>) -> Result<u64> {
        // TODO: Add back performance metrics...
        for doc in doc_receiver {
            index_writer.add_document(doc);
        }
        match index_writer.commit() {
            Ok(c) => Ok(c),
            Err(e) => Err(e.into()),
        }
    }
}

impl Handler for BulkHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let path = IndexPath::take_from(&mut state);
        let index_lock = self.catalog.read().unwrap();
        let index = index_lock.get_index(&path.index).unwrap();
        let schema = index.schema();
        let (line_sender, line_recv) = crossbeam_channel::unbounded::<Vec<u8>>();
        let (doc_sender, doc_recv) = crossbeam_channel::unbounded::<Document>();

        // TODO: Make this configurable
        for _ in 0..8 {
            let schema_clone = schema.clone();
            let doc_sender = doc_sender.clone();
            let line_recv_clone = line_recv.clone();
            thread::spawn(move || {
                for line in line_recv_clone {
                    if !line.is_empty() {
                        match schema_clone.parse_document(from_utf8(&line).unwrap()) {
                            Ok(doc) => doc_sender.send(doc),
                            // TODO: Add better/more error handling here, right now if an error occurs
                            // swallowed up, which is kind of bad.
                            Err(err) => error!("Failed to add doc: {:?}", err),
                        }
                    }
                }
            });
        }

        let mut index_writer = index.writer(SETTINGS.writer_memory).unwrap();
        thread::spawn(move || BulkHandler::index_documents(&mut index_writer, doc_recv));

        let body = Body::take_from(&mut state);
        let line_sender_clone = line_sender.clone();

        let response = body
            .map_err(|e| e.into_handler_error())
            .fold(Vec::new(), move |mut buf, line| {
                buf.extend(line);
                let mut split = buf.split(|b| *b == b'\n').peekable();
                while let Some(l) = split.next() {
                    if split.peek().is_none() {
                        return future::ok(l.to_vec());
                    }
                    line_sender_clone.send(l.to_vec());
                }
                future::ok(buf.clone())
            })
            .then(move |r| match r {
                Ok(buf) => {
                    if !buf.is_empty() {
                        line_sender.send(buf.to_vec());
                    }
                    let resp = create_response(&state, StatusCode::Created, None);
                    future::ok((state, resp))
                }
                Err(ref e) => handle_error(state, e),
            });
        Box::new(response)
    }
}

new_handler!(BulkHandler);

#[cfg(test)]
mod tests {

    use std::path::PathBuf;
    use tantivy::schema::*;
    use tantivy::Index;

    use super::search::tests::*;
    use super::*;
    use index::tests::*;
    use index::IndexCatalog;

    use mime;
    use serde_json;

    #[test]
    #[ignore]
    fn create_index() {
        let mut schema = SchemaBuilder::new();
        schema.add_text_field("title", TEXT | STORED);
        schema.add_text_field("body", TEXT | STORED);
        schema.add_text_field("url", STORED);
        let built = schema.build();

        Index::create_in_dir(PathBuf::from("./indexes/wikipedia"), built).unwrap();
    }

    // TODO: Need Error coverage testing here.

    #[test]
    fn test_bulk_index() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let server = create_test_server(&Arc::new(RwLock::new(catalog)));

        let body = r#"{"test_text": "asdf1234", "test_i64": 123, "test_u64": 321}
        {"test_text": "asdf5678", "test_i64": 456, "test_u64": 678}
        {"test_text": "asdf9012", "test_i64": -12, "test_u64": 901}"#;

        let req = server
            .client()
            .post("http://localhost/test_index/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::Created);

        // Give it a second...
        std::thread::sleep(std::time::Duration::from_secs(1));

        let check_docs = server.client().get("http://localhost/test_index").perform().unwrap();
        let docs: TestResults = serde_json::from_slice(&check_docs.read_body().unwrap()).unwrap();

        assert_eq!(docs.hits, 8);
        // TODO: Do more testing here.
    }
}
