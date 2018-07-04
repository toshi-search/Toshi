#[macro_use]
extern crate gotham_derive;
extern crate gotham;
extern crate hyper;
extern crate mime;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate log;

#[macro_use]
extern crate quick_error;
extern crate futures;
extern crate tantivy;

#[macro_use]
extern crate lazy_static;
extern crate config;
extern crate pretty_env_logger;

mod handlers;
mod index;
pub mod router;
pub mod settings;

use tantivy::ErrorKind;

quick_error! {
    #[derive(Debug)]
    pub enum ToshiError {
        UnknownIndexField(err: ErrorKind)
    }
}

pub type ToshiResult<T> = Result<T, ToshiError>;
