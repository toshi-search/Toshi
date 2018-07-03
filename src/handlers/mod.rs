pub mod index;
pub mod root;
pub mod search;

use index::*;
use settings::SETTINGS;

use gotham::handler::*;
use gotham::state::*;
use gotham::http::response::create_response;
use hyper::{StatusCode, Body};

use std::sync::{Arc, Mutex};

use mime;
use serde_json;