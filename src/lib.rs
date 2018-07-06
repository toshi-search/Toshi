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

use tantivy::ErrorKind;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        IOError(err: String) {}
        UnknownIndexField(err: String) {}
        UnknownIndex(err: String) {}
        TantivyError(err: String) {}
    }
}

impl From<tantivy::Error> for Error {
    fn from(err: tantivy::Error) -> Error {
        match err.0 {
            ErrorKind::CorruptedFile(p) | ErrorKind::PathDoesNotExist(p) | ErrorKind::FileAlreadyExists(p) => {
                Error::IOError(format!("{:?}", p)).into()
            }
            ErrorKind::IOError(e) => Error::IOError(e.to_string()).into(),
            ErrorKind::SchemaError(e) => Error::UnknownIndex(e.to_string()).into(),
            e => Error::TantivyError(e.to_string()).into(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error { Error::IOError(err.to_string()).into() }
}

pub type Result<T> = std::result::Result<T, Error>;

mod handlers;
mod index;
pub mod router;
pub mod settings;
