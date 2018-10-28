extern crate gotham;
extern crate pretty_env_logger;
extern crate uuid;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate tokio;
extern crate toshi;
extern crate hyper;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use hyper::rt;

use toshi::commit::IndexWatcher;
use toshi::index::IndexCatalog;
use toshi::router::router_with_catalog;
use toshi::settings::{Settings, HEADER};
use toshi::cluster::ConsulInterface;
use toshi::cluster;

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

    // If this is the first node in a new cluster, we need to register the cluster name in Consul
    let cluster_name = options.value_of("cluster-name").expect("Unable to get cluster name");
    let mut consul_client: ConsulInterface = ConsulInterface::default()
        .with_cluster_name(cluster_name.to_string());
    let reg_future = consul_client.register_cluster();
    // This blocks so that we don't proceed if we can't talk to Consul
    rt::run(reg_future);

    // Now we need to register this node with the cluster. If it is a new node and has no file containing the node ID,
    // we generate a new one and save it. Otherwise, read it in and register with consul.
    let node_id: String;
    if let Ok(nid) = cluster::read_node_id(&settings.path) {
        info!("Node ID is: {}", nid);
        node_id = nid;
    } else {
        // If no file exists containing the node ID, generate a new one and write it
        let random_id = uuid::Uuid::new_v4().to_hyphenated().to_string();
        info!("No Node ID found. Creating new one: {}", random_id);
        node_id = random_id.clone();
        if let Err(err) = cluster::write_node_id(random_id, &settings.path) {
            error!("{:?}", err);
            std::process::exit(1);
        }
    }
    consul_client.node_id = Some(node_id.clone());
    let reg_future = consul_client.register_node();
    // Registers the node with Consul. Blocks since we don't want to proceed if we can't register.
    rt::run(reg_future);

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
    println!("Node ID: {}", node_id);
    println!("{}", HEADER);
    gotham::start(addr, router_with_catalog(&catalog_arc));
    0
}
