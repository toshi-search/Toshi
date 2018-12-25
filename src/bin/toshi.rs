use toshi::{
    cluster::{self, Consul},
    commit::IndexWatcher,
    index::IndexCatalog,
    router::router_with_catalog,
    settings::{Settings, HEADER},
};

use clap::{crate_authors, crate_description, crate_version, App, Arg, ArgMatches};
use futures::{future, sync::oneshot, Future, Stream};
use log::{error, info};
use tokio::runtime::Runtime;
use uuid::Uuid;

use std::net::SocketAddr;
use std::{
    fs::create_dir,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

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

    let toshi = {
        let server = run(index_catalog.clone(), settings);
        let shutdown = shutdown(tx);
        server.select(shutdown)
    };

    rt.spawn(toshi.map(|_| ()).map_err(|_| ()));

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

fn settings() -> Settings {
    let options: ArgMatches = App::new("Toshi Search")
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        .arg(Arg::with_name("config").short("c").long("config").takes_value(true))
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
                .short("e")
                .long("enable-clustering")
                .takes_value(true),
        )
        .get_matches();

    if options.is_present("config") {
        let cfg = options.value_of("config").unwrap();
        Settings::new(cfg).expect("Invalid Config file")
    } else {
        Settings::from_args(&options)
    }
}

fn run(catalog: Arc<RwLock<IndexCatalog>>, settings: Settings) -> impl Future<Item = (), Error = ()> {
    if !Path::new(&settings.path).exists() {
        info!("Base data path {} does not exist, creating it...", settings.path);
        create_dir(settings.path.clone()).expect("Unable to create data directory");
    }

    let commit_watcher = if settings.auto_commit_duration > 0 {
        let commit_watcher = IndexWatcher::new(catalog.clone(), settings.auto_commit_duration);
        future::Either::A(future::lazy(move || {
            commit_watcher.start();

            future::ok::<(), ()>(())
        }))
    } else {
        future::Either::B(future::ok::<(), ()>(()))
    };

    let addr = format!("{}:{}", &settings.host, settings.port);

    let bind: SocketAddr = addr.parse().unwrap();
    println!("{}", HEADER);

    if settings.enable_clustering {
        let settings = settings.clone();
        let run = future::lazy(move || connect_to_consul(&settings))
            .and_then(move |_| commit_watcher)
            .and_then(move |_| router_with_catalog(&bind, &catalog));

        future::Either::A(run)
    } else {
        let run = commit_watcher.and_then(move |_| router_with_catalog(&bind, &catalog));
        future::Either::B(run)
    }
}

fn connect_to_consul(settings: &Settings) -> impl Future<Item = (), Error = ()> {
    let consul_address = format!("{}:{}", &settings.consul_host, settings.consul_port);
    let cluster_name = settings.cluster_name.clone();
    let settings_path_read = settings.path.clone();
    let settings_path_write = settings.path.clone();

    future::lazy(move || {
        let mut consul_client = Consul::builder()
            .with_cluster_name(cluster_name)
            .with_address(consul_address)
            .build()
            .unwrap();

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
                consul_client.set_node_id(id);
                consul_client.register_node()
            })
            .map_err(|e| error!("Error: {}", e))
    })
}

fn shutdown(signal: oneshot::Sender<()>) -> impl Future<Item = (), Error = ()> {
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
