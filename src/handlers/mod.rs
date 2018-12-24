macro_rules! new_handler {
    ($N:ident) => {
        impl NewHandler for $N {
            type Instance = Self;

            fn new_handler(&self) -> gotham::error::Result<Self::Instance> {
                Ok(self.clone())
            }
        }
    };
}

pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use crate::settings::Settings;

use gotham::handler::HandlerError;
use gotham::helpers::http::response::create_response;
use gotham::state::State;
use gotham_derive::{StateData, StaticResponseExtender};

use failure::Fail;
use futures::{future, future::FutureResult};
use hyper::{Body, Response, StatusCode};
use serde::{Deserialize, Serialize};

type FutureError = FutureResult<(State, Response<Body>), (State, HandlerError)>;

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct IndexPath {
    index: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct QueryOptions {
    #[serde(default = "Settings::default_pretty")]
    pretty: bool,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    reason: String,
}

impl ErrorResponse {
    pub fn new(reason: &str) -> Self {
        ErrorResponse {
            reason: reason.to_string(),
        }
    }
}

fn to_json<T: Serialize>(result: T, pretty: bool) -> Vec<u8> {
    if pretty {
        serde_json::to_vec_pretty(&result).unwrap()
    } else {
        serde_json::to_vec(&result).unwrap()
    }
}

fn handle_error<T: Fail + Sized + Send>(state: State, err: T) -> FutureError {
    let err = serde_json::to_vec(&ErrorResponse::new(&format!("{}", err))).unwrap();
    let resp = create_response(&state, StatusCode::BAD_REQUEST, mime::APPLICATION_JSON, err);
    future::ok((state, resp))
}
