use clap::{crate_authors, crate_description, crate_version, App, AppSettings, Arg, ArgMatches};
use log::info;

use toshi::cluster::Place;
use toshi::settings::HEADER;

fn main() {
    let settings = settings();
    let host = settings.value_of("host").unwrap();
    let port = settings.value_of("port").unwrap();
    std::env::set_var("RUST_LOG", settings.value_of("level").unwrap());
    pretty_env_logger::init();

    println!("{}", HEADER);
    info!("Starting Toshi Placement Service...");
    let addr = format!("{}:{}", host, port).parse().unwrap();
    let service = Place::get_service(addr);

    tokio::run(service);
}

fn settings<'a>() -> ArgMatches<'a> {
    App::new("Toshi Placement Driver")
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .default_value("127.0.0.1"),
        ).arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value("8081"),
        ).arg(
            Arg::with_name("level")
                .short("l")
                .long("level")
                .takes_value(true)
                .default_value("info"),
        ).arg(
            Arg::with_name("consul-host")
                .short("C")
                .long("consul-host")
                .takes_value(true)
                .default_value("localhost"),
        ).arg(
            Arg::with_name("consul-port")
                .short("P")
                .long("consul-port")
                .takes_value(true)
                .default_value("8500"),
        ).get_matches()
}
