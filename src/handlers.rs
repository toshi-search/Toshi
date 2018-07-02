use futures::{future, Future, Stream};
use gotham::handler::*;
use gotham::http::response::create_response;
use gotham::state::{FromState, State};
use hyper::{Body, StatusCode};
use index::{get_index, search_index};
use mime;
use serde_json;
use settings::SETTINGS;
use std::sync::Arc;
use tantivy::schema::*;
use tantivy::Index;

use std::io::Result;
use std::panic::RefUnwindSafe;
use std::sync::Mutex;

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct Search {
    pub field: String,
    pub term:  String,

    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize { 5 }

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct IndexDoc {
    pub field: String,
}

#[derive(Clone, Debug)]
pub struct IndexHandler {
    pub indexes: Arc<Mutex<Index>>,
}

impl IndexHandler {
    pub fn new(index: Index) -> Self {
        IndexHandler {
            indexes: Arc::new(Mutex::new(index)),
        }
    }
}

impl RefUnwindSafe for IndexHandler {}

impl Handler for IndexHandler {
    fn handle(self, state: State) -> Box<HandlerFuture> {
        let resp = create_response(&state, StatusCode::Ok, Some(("Hi!".as_bytes().to_vec(), mime::TEXT_HTML)));
        Box::new(future::ok((state, resp)))
    }
}

impl NewHandler for IndexHandler {
    type Instance = Self;

    fn new_handler(&self) -> Result<Self::Instance> { Ok(self.clone()) }
}

pub fn base_handler(mut state: State) -> Box<HandlerFuture> {
    let resp = create_response(&state, StatusCode::Ok, Some(("Hi!".as_bytes().to_vec(), mime::TEXT_HTML)));
    Box::new(future::ok((state, resp)))
}

pub fn search_handler(mut state: State) -> Box<HandlerFuture> {
    let f = Body::take_from(&mut state).concat2().then(|body| match body {
        Ok(b) => {
            let search: Search = serde_json::from_slice(&b).unwrap();
            let docs = search_index(&search).unwrap();
            let data = Some((serde_json::to_vec_pretty(&docs).unwrap(), mime::APPLICATION_JSON));
            let resp = create_response(&state, StatusCode::Ok, data);
            future::ok((state, resp))
        }
        Err(e) => future::err((state, e.into_handler_error())),
    });
    Box::new(f)
}

pub fn index_handler(mut state: State) -> Box<HandlerFuture> {
    let f = Body::take_from(&mut state).concat2().then(|body| match body {
        Ok(b) => {
            let t: IndexDoc = serde_json::from_slice(&b).unwrap();
            info!("{:?}", t);
            let mut schema = SchemaBuilder::default();
            let field = schema.add_text_field("field", TEXT | STORED);
            let b = schema.build();
            let index = get_index(&SETTINGS.path, Some(&b)).unwrap();
            let mut index_writer = index.writer(SETTINGS.writer_memory).unwrap();
            index_writer.add_document(doc!(field => t.field));
            index_writer.commit().unwrap();
            let resp = create_response(&state, StatusCode::Created, None);
            future::ok((state, resp))
        }
        Err(e) => future::err((state, e.into_handler_error())),
    });
    Box::new(f)
}
