extern crate gotham;
extern crate pretty_env_logger;
extern crate toshi;

use toshi::router::router;
use toshi::settings::{HEADER, SETTINGS};

pub fn main() {
    std::env::set_var("RUST_LOG", "info");
    pretty_env_logger::init();

    println!("{}", HEADER);
    let addr = format!("{}:{}", &SETTINGS.host, SETTINGS.port);
    gotham::start(addr, router())
}
