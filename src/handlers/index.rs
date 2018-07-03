
use super::*;

use std::io::Result;
use futures::{future, Stream, Future};

#[derive(Deserialize, Debug)]
pub struct IndexDoc {
    pub index: String,
    pub field: String,
    pub query: String,
}

#[derive(Clone, Debug)]
pub struct IndexHandler(Arc<Mutex<IndexCatalog>>);

impl IndexHandler {
    pub fn new(catalog: Arc<Mutex<IndexCatalog>>) -> Self {
        IndexHandler(catalog)
    }
}

impl Handler for IndexHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let f = Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                let t: IndexDoc = serde_json::from_slice(&b).unwrap();
                info!("{:?}", t);
                {
                    let index_lock = self.0.lock().unwrap();
                    let index = index_lock.get_index(t.index).unwrap();
                    let index_schema = index.schema();
                    let field = index_schema.get_field(&t.field).unwrap();
                    let mut index_writer = index.writer(SETTINGS.writer_memory).unwrap();
                    index_writer.add_document(doc!(field => t.field));
                    index_writer.commit().unwrap();
                }
                let resp = create_response(&state, StatusCode::Created, None);
                future::ok((state, resp))
            }
            Err(e) => future::err((state, e.into_handler_error())),
        });
        Box::new(f)
    }
}

impl NewHandler for IndexHandler {
    type Instance = Self;

    fn new_handler(&self) -> Result<Self::Instance> { Ok(self.clone()) }
}
