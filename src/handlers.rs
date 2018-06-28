use futures::{future, Future, Stream};
use gotham::{
    handler::{HandlerFuture, IntoHandlerError},
    http::response::create_response,
    state::{FromState, State},
};
use hyper::{Body, StatusCode};
use index::get_index;
use mime;
use serde_json;
use tantivy::{collector::TopCollector, query::FuzzyTermQuery, schema::*, Result};

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct Search {
    pub idx_path: String,
    pub field:    String,
    pub term:     String,

    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize { 5 }

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct IndexDoc {
    pub idx_path: String,
    pub field:    String,
    pub numbers:  i64,
}

fn search_index(s: &Search) -> Result<Vec<Document>> {
    info!("Search: {:?}", s);
    let index = get_index(&s.idx_path, None)?;
    index.load_searchers()?;
    let searcher = index.searcher();
    let schema = index.schema();
    let field = schema.get_field(&s.field).unwrap();
    let term = Term::from_field_text(field, &s.term);
    let query = FuzzyTermQuery::new(term, 2, true);
    let mut collector = TopCollector::with_limit(s.limit);
    searcher.search(&query, &mut collector)?;

    Ok(collector.docs().into_iter().map(|d| searcher.doc(&d).unwrap()).collect())
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
            let index = get_index(&t.idx_path, Some(&b)).unwrap();
            let mut index_writer = index.writer(200_000_000).unwrap();
            index_writer.add_document(doc!(field => t.field));
            index_writer.commit().unwrap();
            let resp = create_response(&state, StatusCode::Created, None);
            future::ok((state, resp))
        }
        Err(e) => future::err((state, e.into_handler_error())),
    });
    Box::new(f)
}
