extern crate pretty_env_logger;
extern crate gotham;
extern crate toshi;

use toshi::router::router;

pub fn main() {
    std::env::set_var("RUST_LOG", "info");
    pretty_env_logger::init();

    let addr = "127.0.0.1:7878";
    gotham::start(addr, router())
}
