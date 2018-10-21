extern crate gotham;
extern crate pretty_env_logger;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate toshi;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use toshi::commit::IndexWatcher;
use toshi::index::IndexCatalog;
use toshi::router::router_with_catalog;
use toshi::settings::{Settings, HEADER};

use clap::{App, Arg, ArgMatches};

pub fn main() {
    let code = runner();
    std::process::exit(code);
}

pub fn runner() -> i32 {
    let options: ArgMatches = App::new("Toshi Search")
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::with_name("config").short("c").takes_value(true))
        .arg(
            Arg::with_name("level")
                .short("l")
                .long("level")
                .takes_value(true)
                .default_value("info"),
        )
        .arg(
            Arg::with_name("path")
                .short("dp")
                .long("data-path")
                .takes_value(true)
                .default_value("data/"),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .default_value("localhost"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value("8080"),
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
        .arg(
            Arg::with_name("cluster-name")
                .short("N")
                .long("cluster-name")
                .takes_value(true)
                .default_value("default"),
        )
        .get_matches();

    let settings = if options.is_present("config") {
        let cfg = options.value_of("config").unwrap();
        info!("Reading config from: {}", cfg);
        Settings::new(cfg).expect("Invalid Config file")
    } else {
        Settings::from_args(&options)
    };

    std::env::set_var("RUST_LOG", &settings.log_level);
    pretty_env_logger::init();

    let index_catalog = match IndexCatalog::new(PathBuf::from(&settings.path), settings.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error Encountered - {}", e.to_string());
            std::process::exit(1);
        }
    };
    let catalog_arc = Arc::new(RwLock::new(index_catalog));

    if settings.auto_commit_duration > 0 {
        let commit_watcher = IndexWatcher::new(Arc::clone(&catalog_arc), settings.auto_commit_duration);
        commit_watcher.start();
    }

    let addr = format!("{}:{}", &settings.host, settings.port);
    println!("{}", HEADER);
    gotham::start(addr, router_with_catalog(&catalog_arc));

    0
}
