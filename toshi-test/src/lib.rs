/// Toshi-test is a modified version of Gotham's Test server, credit goes to them
/// https://github.com/gotham-rs/gotham/blob/master/gotham/src/test.rs
/// This was created to create a more integration test oriented way to test Toshi services
/// instead of magically bypassing some setup to call services.
pub use crate::server::*;
pub use crate::test::*;
use std::net::SocketAddr;

pub mod server;
pub mod test;

pub type Result<T> = std::result::Result<T, failure::Error>;
pub static CONTENT_TYPE: &str = "application/json";

pub fn get_localhost() -> SocketAddr {
    "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
}
