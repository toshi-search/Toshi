use clap::{crate_authors, crate_description, crate_version, App, Arg, ArgMatches};
use hyper::http::uri::Scheme;
use log::info;

use toshi::cluster::Consul;
use toshi::cluster::Place;
use toshi::settings::HEADER;

fn main() {
    let settings = settings();
    let host = settings.value_of("host").unwrap();
    let port = settings.value_of("port").unwrap();
    let consul_host = settings.value_of("consul-host").unwrap();
    let consul_port = settings.value_of("consul-port").unwrap();

    std::env::set_var("RUST_LOG", settings.value_of("level").unwrap());
    pretty_env_logger::init();

    println!("{}", HEADER);
    info!("Starting Toshi Placement Service...");
    let addr = format!("{}:{}", host, port).parse().unwrap();
    let consul_addr = format!("{}:{}", consul_host, consul_port).parse().unwrap();
    let consul = Consul::builder()
        .with_address(consul_addr)
        .with_scheme(Scheme::HTTP)
        .build()
        .unwrap();
    let service = Place::get_service(addr, consul);

    tokio::run(service);
}

fn settings() -> ArgMatches<'static> {
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
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value("8081"),
        )
        .arg(
            Arg::with_name("level")
                .short("l")
                .long("level")
                .takes_value(true)
                .default_value("info"),
        )
        .arg(
            Arg::with_name("consul-host")
                .short("C")
                .long("consul-host")
                .takes_value(true)
                .default_value("localhost"),
        )
        .arg(
            Arg::with_name("consul-port")
                .short("P")
                .long("consul-port")
                .takes_value(true)
                .default_value("8500"),
        )
        .get_matches()
}
