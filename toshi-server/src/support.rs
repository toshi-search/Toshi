use clap::{crate_authors, crate_description, crate_version, App, Arg, ArgMatches};

use crate::settings::Settings;

#[cfg_attr(tarpaulin, skip)]
pub fn settings() -> Settings {
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
                .default_value("0.0.0.0"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value("8080"),
        )
        .arg(
            Arg::with_name("consul-addr")
                .short("C")
                .long("consul-addr")
                .takes_value(true)
                .default_value("127.0.0.1:8500"),
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
                .takes_value(false),
        )
        .arg(Arg::with_name("experimental").short("x").long("experimental").takes_value(false))
        .arg(Arg::with_name("master").short("m").long("master").takes_value(false))
        .arg(
            Arg::with_name("nodes")
                .short("n")
                .long("nodes")
                .takes_value(true)
                .multiple(true)
                .default_value(""),
        )
        .get_matches();

    match options.value_of("config") {
        Some(v) => Settings::new(v).expect("Invalid configuration file"),
        None => Settings::from_args(&options),
    }
}
