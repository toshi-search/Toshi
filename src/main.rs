extern crate pretty_env_logger;

extern crate gotham;
extern crate toshi;

use toshi::router::router;
use toshi::settings::SETTINGS;

pub fn main() {
    std::env::set_var("RUST_LOG", "info");
    pretty_env_logger::init();

    let addr = format!("{}:{}", &SETTINGS.host, SETTINGS.port);
    gotham::start(addr, router())
}
