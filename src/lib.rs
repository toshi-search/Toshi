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
extern crate tantivy;
extern crate futures;

mod handlers;
mod index;
pub mod router;
