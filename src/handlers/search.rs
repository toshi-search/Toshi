use super::*;

use futures::{future, Future, Stream};
use std::io::Result;
use std::panic::RefUnwindSafe;

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct Search {
    pub field: String,
    pub term:  String,

    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize { 5 }

#[derive(Clone, Debug)]
pub struct SearchHandler {
    catalog: Arc<IndexCatalog>,
}

impl RefUnwindSafe for SearchHandler {}

impl SearchHandler {
    pub fn new(catalog: Arc<IndexCatalog>) -> Self { SearchHandler { catalog } }
}

impl Handler for SearchHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let index = IndexPath::take_from(&mut state);
        let f = Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                let search: Search = serde_json::from_slice(&b).unwrap();
                let docs = { self.catalog.search_index(&index.index, &search).unwrap() };
                info!(
                    "Query returned {} docs on Index: {} \n Query: {:#?}",
                    docs.len(),
                    index.index,
                    search
                );
                let data = Some((serde_json::to_vec_pretty(&docs).unwrap(), mime::APPLICATION_JSON));
                let resp = create_response(&state, StatusCode::Ok, data);
                future::ok((state, resp))
            }
            Err(e) => future::err((state, e.into_handler_error())),
        });
        Box::new(f)
    }
}

new_handler!(SearchHandler);
