use futures::{future, Stream, Future};

use gotham::handler::{HandlerFuture, IntoHandlerError};
use gotham::http::response::create_response;
use gotham::state::{FromState, State};

use serde_json;
use mime;
use hyper::{Body, Response, StatusCode};
use tantivy::schema::*;

use index::get_index;

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct Search {
    pub term: String,
}

#[derive(Serialize, Deserialize, StateData, StaticResponseExtender, Debug)]
pub struct IndexDoc {
    pub idx_path: String,
    pub field: String,
    pub numbers: i64,
}

pub fn search_handler(mut state: State) -> (State, Response) {
    let t = Search::take_from(&mut state);
    let b = serde_json::to_string_pretty(&t).unwrap();
    let resp = create_response(
        &state,
        StatusCode::Ok,
        Some((b.into_bytes(), mime::APPLICATION_JSON)),
    );
    (state, resp)
}

pub fn index_handler(mut state: State) -> Box<HandlerFuture> {
    let f = Body::take_from(&mut state).concat2().then(|body| {
        match body {
            Ok(b) => {
                let t: IndexDoc = serde_json::from_slice(&b).unwrap();

                info!("{:?}", t);

                let mut schema = SchemaBuilder::default();
                let field = schema.add_text_field("field", TEXT | STORED);

                let b = schema.build();

                let index = get_index(&t.idx_path, &b).unwrap();
                let mut index_writer = index.writer(200_000_000).unwrap();

                index_writer.add_document(doc!(field => &t.field));
                index_writer.commit().unwrap();

                let resp = create_response(&state, StatusCode::Created, None);
                future::ok((state, resp))
            }
            Err(e) => return future::err((state, e.into_handler_error())),
        }
    });
    Box::new(f)
}
