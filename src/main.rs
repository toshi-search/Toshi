#[cfg(feature = "pretty_log")]
extern crate pretty_env_logger;

#[cfg(feature = "env_log")]
extern crate env_logger;

extern crate gotham;
extern crate toshi;

use toshi::router::router;

#[cfg(feature = "pretty_log")]
fn init_logging() { pretty_env_logger::init(); }

#[cfg(feature = "env_log")]
fn init_logging() { env_logger::init(); }

#[cfg(all(not(feature = "pretty_log"), not(feature = "env_log")))]
fn init_logging() {}

pub fn main() {
    std::env::set_var("RUST_LOG", "info");
    init_logging();

    let addr = "127.0.0.1:7878";
    gotham::start(addr, router())
}
