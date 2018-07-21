use super::super::*;
use super::*;

use futures::future;
use futures::stream::*;
use futures::{Async, Future, Poll, Stream};

use gotham::handler::*;
use gotham::state::FromState;

use std::io::Result as IOResult;
use std::mem::replace;
use std::panic::RefUnwindSafe;
use std::str::from_utf8;
use std::string::FromUtf8Error;
use std::sync::RwLock;
use std::thread;
use std::time::Instant;

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
        let group_count = 100_000u64;
        let mut cur = Instant::now();
        for (num_docs, doc) in doc_receiver.enumerate() {
            index_writer.add_document(doc);
            if num_docs > 0 && (num_docs as u64 % group_count == 0) {
                info!("{} Docs", num_docs);
                let new = Instant::now();
                let elapsed = new.duration_since(cur);
                info!("{:?} docs / hour", group_count * 3600 * 1_000_000 / elapsed.as_secs());
                cur = new;
            }
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
        let body = Body::take_from(&mut state);

        for _ in 0..2 {
            let schema_clone = schema.clone();
            let doc_sender = d_send.clone();
            let l_recv_clone = l_recv.clone();
            thread::spawn(move || {
                for line in l_recv_clone {
                    match schema_clone.parse_document(from_utf8(&line).unwrap()) {
                        Ok(doc) => doc_sender.send(doc),
                        Err(err) => error!("Failed to add doc: {:?}", err),
                    }
                }
            });
        }
        drop(d_send);

        let mut index_writer = index.writer_with_num_threads(2, SETTINGS.writer_memory).unwrap();
        thread::spawn(move || BulkHandler::index_documents(&mut index_writer, d_recv.clone()));

        let response = Lines::new(body)
            .map_err(|e| e.into_handler_error())
            .fold(0, move |mut acc, line| {
                info!("{:?}", from_utf8(&line));
                l_send.send(line);
                acc += 1;
                future::ok(acc)
            })
            .then(|r| match r {
                Ok(i) => {
                    info!("Recs: {}", i);
                    let resp = create_response(&state, StatusCode::Ok, None);
                    future::ok((state, resp))
                }
                Err(ref e) => handle_error(state, e),
            });
        Box::new(response)
    }
}

new_handler!(BulkHandler);

struct Lines<S: Stream> {
    buffered: Option<Vec<u8>>,
    stream:   Fuse<S>,
}

impl<S: Stream> Lines<S> {
    pub fn new(stream: S) -> Lines<S> {
        Lines {
            buffered: None,
            stream:   stream.fuse(),
        }
    }

    fn process(&mut self, flush: bool) -> Option<std::result::Result<Vec<u8>, FromUtf8Error>> {
        let buffered = replace(&mut self.buffered, None);
        if let Some(ref buffer) = buffered {
            info!("Buffer is {:?} bytes", buffer.len());
            let mut split = buffer.splitn(2, |c| *c == b'\n');
            if let Some(first) = split.next() {
                if let Some(second) = split.next() {
                    replace(&mut self.buffered, Some(second.to_vec()));
                    return Some(Ok(first.to_vec()));
                } else if flush {
                    return Some(Ok(first.to_vec()));
                }
            }
        }
        replace(&mut self.buffered, buffered);
        None
    }
}

impl<S> Stream for Lines<S>
where
    S: Stream,
    S::Item: AsRef<[u8]>,
    S::Error: From<FromUtf8Error>,
{
    type Item = Vec<u8>;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Vec<u8>>, S::Error> {
        match self.stream.poll()? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(None) => match self.process(true) {
                Some(Ok(line)) => Ok(Async::Ready(Some(line))),
                Some(Err(err)) => Err(err.into()),
                None => Ok(Async::Ready(None)),
            },
            Async::Ready(Some(chunk)) => {
                if let Some(ref mut buffer) = self.buffered {
                    buffer.extend(chunk.as_ref());
                } else {
                    self.buffered = Some(chunk.as_ref().to_vec());
                }
                match self.process(false) {
                    Some(Ok(line)) => Ok(Async::Ready(Some(line))),
                    Some(Err(err)) => Err(err.into()),
                    None => Ok(Async::NotReady),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tantivy::schema::*;
    use tantivy::Index;
    use std::path::PathBuf;

    #[ignore]
    fn create_index() {
        let mut schema = SchemaBuilder::new();
        schema.add_text_field("title", TEXT | STORED);
        schema.add_text_field("body", TEXT | STORED);
        schema.add_text_field("url", TEXT | STORED);
        let built = schema.build();

        Index::create_in_dir(PathBuf::from("./indexes/wikipedia"), built).unwrap();
    }
}
