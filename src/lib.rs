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

#[cfg(feature = "pretty_log")]
extern crate pretty_env_logger;

#[cfg(feature = "env_log")]
extern crate env_logger;

#[macro_use]
extern crate tantivy;
extern crate futures;

mod handlers;
mod index;
pub mod router;
