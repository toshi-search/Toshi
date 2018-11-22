extern crate gotham;
extern crate pretty_env_logger;
extern crate uuid;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate futures;
extern crate hyper;
extern crate num_cpus;
extern crate systemstat;
extern crate tokio;
extern crate toshi;

use futures::Future;
use std::{
    fs::create_dir,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use uuid::Uuid;

use toshi::cluster;
use toshi::cluster::ConsulInterface;
use toshi::commit::IndexWatcher;
use toshi::index::IndexCatalog;
use toshi::router::router_with_catalog;
use toshi::settings::{Settings, HEADER};

use clap::{App, Arg, ArgMatches};

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::*;

pub fn main() {
    let code = runner();
    std::process::exit(code);
}

pub fn runner() -> i32 {
    let options: ArgMatches = App::new("Toshi Search")
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .takes_value(true)
                .default_value("config/config.toml"),
        ).arg(
            Arg::with_name("level")
                .short("l")
                .long("level")
                .takes_value(true)
                .default_value("info"),
        ).arg(
            Arg::with_name("path")
                .short("d")
                .long("data-path")
                .takes_value(true)
                .default_value("data/"),
        ).arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .default_value("localhost"),
        ).arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value("8080"),
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
        ).arg(
            Arg::with_name("cluster-name")
                .short("N")
                .long("cluster-name")
                .takes_value(true)
                .default_value("kitsune"),
        ).arg(
            Arg::with_name("enable-clustering")
                .short("E")
                .long("enable-clustering")
                .takes_value(false),
        ).get_matches();

    let settings = if options.is_present("config") {
        let cfg = options.value_of("config").unwrap();
        info!("Reading config from: {}", cfg);
        Settings::new(cfg).expect("Invalid Config file")
    } else {
        Settings::from_args(&options)
    };

    std::env::set_var("RUST_LOG", &settings.log_level);
    pretty_env_logger::init();

    if !Path::new(&settings.path).exists() {
        info!("Base data path {} does not exist, creating it...", settings.path);
        create_dir(settings.path.clone()).expect("Unable to create data directory");
    }

    if settings.enable_clustering || options.is_present("enable-clustering") {
        // If this is the first node in a new cluster, we need to register the cluster name in Consul
        let cluster_name = options.value_of("cluster-name").expect("Unable to get cluster name");
        let mut consul_client: ConsulInterface = ConsulInterface::default().with_cluster_name(cluster_name.to_string());

        let settings_path_read = settings.path.clone();
        let settings_path_write = settings.path.clone();

        // Build future that will connect to consul and register the node_id
        let connect_consul = consul_client
            .register_cluster()
            .and_then(move |_| cluster::read_node_id(settings_path_read.as_str()))
            .then(|result| match result {
                Ok(id) => {
                    let parsed_id = Uuid::parse_str(&id).expect("Parsed node ID is not a UUID");
                    cluster::write_node_id(settings_path_write, parsed_id.to_hyphenated().to_string())
                }

                Err(_) => {
                    let new_id = Uuid::new_v4();
                    cluster::write_node_id(settings_path_write, new_id.to_hyphenated().to_string())
                }
            }).and_then(move |id| {
                consul_client.node_id = Some(id);
                consul_client.register_node()
            }).map_err(|err| error!("Error: {}", err));

        // Run the tokio runtime, this will start an event loop that will process
        // the connect_consul future. It will block until the future is completed
        // by either completing successfuly or erroring out.
        tokio::run(connect_consul);
    } else {
        info!("Clustering disabled...")
    }

    let index_catalog = match IndexCatalog::new(PathBuf::from(&settings.path), settings.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error Encountered - {}", e.to_string());
            std::process::exit(1);
        }
    };
    let catalog_arc = Arc::new(RwLock::new(index_catalog));
    let mut runtime = Builder::new().name_prefix("toshi-runtime").build().unwrap();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        info!("Shutting down...");
        r.store(false, Ordering::SeqCst)
    }).expect("Error Setting up shutdown handler");

    if settings.auto_commit_duration > 0 {
        let commit_watcher = IndexWatcher::new(Arc::clone(&catalog_arc), settings.auto_commit_duration);
        commit_watcher.start(&mut runtime);
    }

    let addr = format!("{}:{}", &settings.host, settings.port);
    println!("{}", HEADER);
    gotham::start_on_executor(addr, router_with_catalog(&catalog_arc), runtime.executor());

    while running.load(Ordering::SeqCst) {}
    runtime.shutdown_now().wait().unwrap();
    catalog_arc.write().unwrap().clear();
    0
}
