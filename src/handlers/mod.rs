pub mod index;
pub mod root;
pub mod search;

use index::*;
use settings::SETTINGS;

use gotham::handler::*;
use gotham::http::response::create_response;
use gotham::state::*;
use hyper::{Body, StatusCode};

use std::sync::{Arc, Mutex};

use mime;
use serde_json;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    reason: String,
}

impl ErrorResponse {
    pub fn new(reason: &str) -> Self {
        ErrorResponse { reason: reason.to_string() }
    }
}