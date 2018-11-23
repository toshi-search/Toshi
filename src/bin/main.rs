extern crate clap;
extern crate futures;
extern crate gotham;
extern crate hyper;
extern crate log;
extern crate num_cpus;
extern crate pretty_env_logger;
extern crate systemstat;
extern crate tokio;
extern crate toshi;
extern crate uuid;

use clap::{crate_authors, crate_description, crate_version};
use clap::{App, Arg, ArgMatches};
use futures::{sync::oneshot, Future, Stream};
use log::{error, info};
use std::{
    fs::create_dir,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tokio::runtime::Runtime;
use uuid::Uuid;

use toshi::{
    cluster::{self, ConsulInterface},
    commit::IndexWatcher,
    index::IndexCatalog,
    router::router_with_catalog,
    settings::{Settings, HEADER},
};

pub fn main() -> Result<(), ()> {
    let mut rt = Runtime::new().expect("failed to start new Runtime");

    let (tx, shutdown_signal) = oneshot::channel();
    let server = {
        let server = runner();
        server.select(shutdown(tx)).map(|_| ()).map_err(|_| ())
    };

    rt.spawn(server);

    shutdown_signal.map_err(|_| ()).and_then(move |_| rt.shutdown_now()).wait()
}

pub fn runner() -> impl Future<Item = (), Error = ()> {
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

    // If this is the first node in a new cluster, we need to register the cluster name in Consul
    let cluster_name = options.value_of("cluster-name").expect("Unable to get cluster name");

    // Run the tokio runtime, this will start an event loop that will process
    // the connect_consul future. It will block until the future is completed
    // by either completing successfuly or erroring out.
    connect_to_consul(settings.path.clone(), cluster_name.into())
        .and_then(move |_| gotham::init_server(addr, router_with_catalog(&catalog_arc)))
}

fn connect_to_consul(path: String, cluster_name: String) -> impl Future<Item = (), Error = ()> {
    let mut consul_client = ConsulInterface::default().with_cluster_name(cluster_name.to_string());

    let settings_path_read = path.clone();
    let settings_path_write = path.clone();

    // Build future that will connect to consul and register the node_id
    consul_client
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
        }).map_err(|e| error!("Error: {}", e))
}

fn shutdown(signal: oneshot::Sender<()>) -> impl Future<Item = (), Error = ()> {
    // Create an infinite stream of "Ctrl+C" notifications. Each item received
    // on this stream may represent multiple ctrl-c signals.
    tokio_signal::ctrl_c()
        .flatten_stream()
        .take(1)
        .into_future()
        .and_then(move |_| {
            info!("Gracefully shutting down...");
            Ok(signal.send(()))
        }).map(|_| ())
        .map_err(|_| unreachable!("ctrl-c should never error out"))
}
