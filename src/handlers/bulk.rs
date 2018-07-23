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
        let (l_send, l_recv) = crossbeam_channel::unbounded::<Vec<u8>>();
        let (d_send, d_recv) = crossbeam_channel::unbounded::<Document>();

        // TODO: Make this configurable
        for _ in 0..8 {
            let schema_clone = schema.clone();
            let doc_sender = d_send.clone();
            let l_recv_clone = l_recv.clone();
            thread::spawn(move || {
                for line in l_recv_clone {
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
        thread::spawn(move || BulkHandler::index_documents(&mut index_writer, d_recv));

        let body = Body::take_from(&mut state);
        let response = body
            .map_err(|e| e.into_handler_error())
            .fold(Vec::new(), move |mut buf, line| {
                buf.extend(line);
                let mut split = buf.split(|b| *b == b'\n').peekable();
                while let Some(l) = split.next() {
                    if split.peek().is_none() {
                        return future::ok(l.to_vec());
                    }
                    l_send.send(l.to_vec());
                }
                future::ok(buf.clone())
            })
            .then(|r| match r {
                Ok(_) => {
                    let resp = create_response(&state, StatusCode::Ok, None);
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

    // TODO: Write some some actual tests here.

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
}
