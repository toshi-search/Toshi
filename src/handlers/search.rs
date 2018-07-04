use super::*;

use futures::{future, Future, Stream};
use std::io::Result;

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct Search {
    pub index: String,
    pub field: String,
    pub term:  String,

    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize { 5 }

#[derive(Clone, Debug)]
pub struct SearchHandler(Arc<Mutex<IndexCatalog>>);

impl SearchHandler {
    pub fn new(catalog: Arc<Mutex<IndexCatalog>>) -> Self { SearchHandler(catalog) }
}

impl Handler for SearchHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let f = Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                let search: Search = serde_json::from_slice(&b).unwrap();
                let docs = {
                    let i = self.0.lock().unwrap();
                    i.search_index(&search).unwrap()
                };
                info!("Query returned {} docs \n Query: {:#?}", docs.len(), search);
                let data = Some((serde_json::to_vec_pretty(&docs).unwrap(), mime::APPLICATION_JSON));
                let resp = create_response(&state, StatusCode::Ok, data);
                future::ok((state, resp))
            }
            Err(e) => future::err((state, e.into_handler_error())),
        });
        Box::new(f)
    }
}

impl NewHandler for SearchHandler {
    type Instance = Self;

    fn new_handler(&self) -> Result<Self::Instance> { Ok(self.clone()) }
}
