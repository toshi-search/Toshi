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

use clap::{crate_authors, crate_description, crate_version, App, Arg, ArgMatches};
use futures::{future, sync::oneshot, Future, Stream};
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
use std::net::SocketAddr;

pub fn main() -> Result<(), ()> {
    let settings = settings();

    std::env::set_var("RUST_LOG", &settings.log_level);
    pretty_env_logger::init();

    let mut rt = Runtime::new().expect("failed to start new Runtime");

    let (tx, shutdown_signal) = oneshot::channel();

    let index_catalog = {
        let path = PathBuf::from(settings.path.clone());
        let index_catalog = match IndexCatalog::new(path, settings.clone()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Error Encountered - {}", e.to_string());
                std::process::exit(1);
            }
        };

        Arc::new(RwLock::new(index_catalog))
    };

    // Create the toshi application future, this future runs the entire
    // Toshi application. It will produce a oneshot channel message when
    // it has received the shutdown signal from the OS.
    let toshi = {
        // Create the future that runs forever and spawns the webserver
        // and clustering abilities of Toshi.
        let server = run(index_catalog.clone(), settings);

        // Create the future that waits for a shutdown signal and
        // triggers the oneshot shutdown_signal to tell the tokio
        // runtime it's time to gracefully.
        let shutdown = shutdown(tx);

        // Select between either future, this will resolve to the
        // first future to resolve. Since, the server future runs forever
        // the only time this will resolve is when the shutdown signal has been
        // received and it will "throwaway" the server future. This gives Toshi
        // an opportunity to clean up its resources.
        server.select(shutdown)
    };

    rt.spawn(toshi.map(|_| ()).map_err(|_| ()));

    // Gracefully shutdown the tokio runtime when a shutdown message has been
    // received on the shutdown_signal channel.
    shutdown_signal
        .map_err(|_| unreachable!("Shutdown signal channel should not error, This is a bug."))
        .and_then(move |_| {
            index_catalog
                .write()
                .expect("Unable to acquire write lock on index catalog")
                .clear();

            Ok(())
        })
        .and_then(move |_| rt.shutdown_now())
        .wait()
}

// Extract the settings from the cli or config file
fn settings() -> Settings {
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
        )
        .arg(
            Arg::with_name("level")
                .short("l")
                .long("level")
                .takes_value(true)
                .default_value("info"),
        )
        .arg(
            Arg::with_name("path")
                .short("d")
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
                .default_value("kitsune"),
        )
        .arg(
            Arg::with_name("enable-clustering")
                .short("E")
                .long("enable-clustering")
                .takes_value(false),
        )
        .get_matches();

    let settings = if options.is_present("config") {
        let cfg = options.value_of("config").unwrap();
        info!("Reading config from: {}", cfg);
        Settings::new(cfg).expect("Invalid Config file")
    } else {
        Settings::from_args(&options)
    };

    settings
}

// Create the future that runs forever and spawns the webserver
// and clustering abilities of Toshi.
fn run(catalog: Arc<RwLock<IndexCatalog>>, settings: Settings) -> impl Future<Item = (), Error = ()> {
    if !Path::new(&settings.path).exists() {
        info!("Base data path {} does not exist, creating it...", settings.path);
        create_dir(settings.path.clone()).expect("Unable to create data directory");
    }

    // Create the auto commit watcher future that spawns commit actions
    // every interval.
    let commit_watcher = if settings.auto_commit_duration > 0 {
        let commit_watcher = IndexWatcher::new(catalog.clone(), settings.auto_commit_duration);
        future::Either::A(future::lazy(move || {
            commit_watcher.start();

            future::ok::<(), ()>(())
        }))
    } else {
        future::Either::B(future::ok::<(), ()>(()))
    };

    let addr: SocketAddr = format!("{}:{}", &settings.host, settings.port).parse().unwrap();

    println!("{}", HEADER);

    if settings.enable_clustering {
        // Run the tokio runtime, this will start an event loop that will process
        // the connect_consul future. It will block until the future is completed
        // by either completing successfully or erroring out.
        let run = connect_to_consul(settings.path.clone(), settings.cluster_name.into())
            .and_then(move |_| commit_watcher)
            .and_then(move |_| router_with_catalog(&addr, &catalog));

        future::Either::A(run)
    } else {
        let run = commit_watcher.and_then(move |_| router_with_catalog(&addr, &catalog));
        future::Either::B(run)
    }
}

// Spawn a future that will connect to the consul cluster from the provided path
// and cluster name.
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
        })
        .and_then(move |id| {
            consul_client.node_id = Some(id);
            consul_client.register_node()
        })
        .map_err(|e| error!("Error: {}", e))
}

// A future that takes a shutdown signal sender and will produce a message
// on the channel once it has received the shutdown signal.
fn shutdown(signal: oneshot::Sender<()>) -> impl Future<Item = (), Error = ()> {
    // Create a future that will take the first shutdown signal received and
    // will produce a shutdown message on the signal channel passed in.
    tokio_signal::ctrl_c()
        .flatten_stream()
        .take(1)
        .into_future()
        .and_then(move |_| {
            info!("Gracefully shutting down...");
            Ok(signal.send(()))
        })
        .map(|_| ())
        .map_err(|_| unreachable!("ctrl-c should never error out"))
}
