use std::str::FromStr;

use config::{Config, ConfigError, File, FileFormat, Source};
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use serde::Deserialize;
use tantivy::merge_policy::*;
use structopt::StructOpt;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const HEADER: &str = r#"
  ______         __   _   ____                 __
 /_  __/__  ___ / /  (_) / __/__ ___ _________/ /
  / / / _ \(_-</ _ \/ / _\ \/ -_) _ `/ __/ __/ _ \
 /_/  \___/___/_//_/_/ /___/\__/\_,_/_/  \__/_//_/
 Such Relevance, Much Index, Many Search, Wow
 "#;

pub const RPC_HEADER: &str = r#"
 ______         __   _   ___  ___  _____
/_  __/__  ___ / /  (_) / _ \/ _ \/ ___/
 / / / _ \(_-</ _ \/ / / , _/ ___/ /__
/_/  \___/___/_//_/_/ /_/|_/_/   \___/
Such coordination, Much consensus, Many RPC, Wow
"#;

#[derive(PartialEq)]
pub enum MergePolicyType {
    Log,
    NoMerge,
}

const fn default_level_log_size() -> f64 { 0.75 }
const fn default_min_layer_size() -> u32 { 10_000 }
const fn default_min_merge_size() -> usize { 8 }

pub fn settings() -> Settings {
    let options = Settings::from_args();
    if !&options.config.is_empty() {
        Settings::new(&options.config).expect("Invalid Configuration File")
    } else {
        options
    }
}

#[derive(Deserialize, Clone, Debug, StructOpt)]
pub struct ConfigMergePolicy {
    #[structopt(long, default_value = "log")]
    kind: String,
    #[structopt(long, default_value = "8")]
    #[serde(default = "default_min_merge_size")]
    min_merge_size: usize,
    #[structopt(long, default_value = "10000")]
    #[serde(default = "default_min_layer_size")]
    min_layer_size: u32,
    #[structopt(long, default_value = "0.75")]
    #[serde(default = "default_level_log_size")]
    level_log_size: f64,
}

impl Default for ConfigMergePolicy {
    fn default() -> Self {
        Self {
            kind: "log".into(),
            min_merge_size: 0,
            min_layer_size: 0,
            level_log_size: 0.0
        }
    }
}

impl ConfigMergePolicy {

    pub fn get_kind(&self) -> MergePolicyType {
        match self.kind.to_ascii_lowercase().as_ref() {
            "log" => MergePolicyType::Log,
            "nomerge" => MergePolicyType::NoMerge,
            _ => panic!("Unknown Merge Typed Defined"),
        }
    }
}

#[derive(Deserialize, Clone, Debug, StructOpt, Default)]
pub struct Experimental {
    #[structopt(long, default_value = "127.0.0.1:8500")]
    #[serde(default = "Settings::default_consul_addr")]
    pub consul_addr: String,
    #[structopt(long, default_value = "kitsune")]
    #[serde(default = "Settings::default_cluster_name")]
    pub cluster_name: String,
    #[structopt(long)]
    #[serde(default = "Settings::default_leader")]
    pub leader: bool,
    #[structopt(long)]
    #[serde(default = "Settings::default_nodes")]
    pub nodes: Vec<String>,
    #[structopt(long, default_value = "1")]
    #[serde(default = "Settings::default_id")]
    pub id: u64,
    #[structopt(long, default_value = "8081")]
    #[serde(default = "Settings::default_rpc_port")]
    pub rpc_port: u16,
}

#[derive(Deserialize, Clone, Debug, StructOpt)]
#[structopt(name = "toshi", version = env!("CARGO_PKG_VERSION"))]
pub struct Settings {
    #[serde(skip)]
    #[structopt(short, long, default_value = "config/config.toml")]
    pub config: String,
    #[serde(default = "Settings::default_host")]
    #[structopt(short, long, default_value = "127.0.0.1")]
    pub host: String,
    #[serde(default = "Settings::default_port")]
    #[structopt(short, long, default_value = "8080")]
    pub port: u16,
    #[serde(default = "Settings::default_path")]
    #[structopt(short = "P", long, default_value = "data/")]
    pub path: String,
    #[serde(default = "Settings::default_level")]
    #[structopt(short, long, default_value = "info")]
    pub log_level: String,
    #[serde(default = "Settings::default_writer_memory")]
    #[structopt(short, long, default_value = "200000000")]
    pub writer_memory: usize,
    #[serde(default = "Settings::default_json_parsing_threads")]
    #[structopt(short, long, default_value = "4")]
    pub json_parsing_threads: usize,
    #[serde(default = "Settings::default_auto_commit_duration")]
    #[structopt(short, long, default_value = "5")]
    pub auto_commit_duration: f32,
    #[serde(default = "Settings::default_bulk_buffer_size")]
    #[structopt(short, long, default_value = "10000")]
    pub bulk_buffer_size: usize,
    #[structopt(flatten)]
    #[serde(default = "Settings::default_merge_policy")]
    pub merge_policy: ConfigMergePolicy,
    #[serde(default = "Settings::default_experimental")]
    #[structopt(short, long)]
    pub experimental: bool,
    #[structopt(flatten)]
    #[serde(default = "Experimental::default")]
    pub experimental_features: Experimental,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            config: "config/config.toml".into(),
            host: Settings::default_host(),
            port: Settings::default_port(),
            path: Settings::default_path(),
            log_level: Settings::default_level(),
            writer_memory: Settings::default_writer_memory(),
            json_parsing_threads: Settings::default_json_parsing_threads(),
            auto_commit_duration: Settings::default_auto_commit_duration(),
            bulk_buffer_size: Settings::default_bulk_buffer_size(),
            merge_policy: ConfigMergePolicy::default(),
            experimental: false,
            experimental_features: Experimental::default()
        }
    }
}

impl FromStr for Settings {
    type Err = ConfigError;

    fn from_str(cfg: &str) -> Result<Self, ConfigError> {
        Self::from_config(File::from_str(cfg, FileFormat::Toml))
    }
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        Self::from_config(File::with_name(path))
    }

    pub fn from_config<T: Source + Send + Sync + 'static>(c: T) -> Result<Self, ConfigError> {
        let mut cfg = Config::new();
        match cfg.merge(c) {
            Ok(_) => {}
            Err(e) => panic!("Problem with config file: {}", e),
        };
        cfg.try_into()
    }

    pub fn default_pretty() -> bool {
        false
    }

    pub fn default_result_limit() -> usize {
        100
    }

    pub fn default_host() -> String {
        "0.0.0.0".to_string()
    }

    pub fn default_path() -> String {
        "data/".to_string()
    }

    pub fn default_port() -> u16 {
        8080
    }

    pub fn default_level() -> String {
        "info".to_string()
    }

    pub fn default_writer_memory() -> usize {
        200_000_000
    }

    pub fn default_json_parsing_threads() -> usize {
        4
    }

    pub fn default_bulk_buffer_size() -> usize {
        10000
    }

    pub fn default_auto_commit_duration() -> f32 {
        10.0
    }

    pub fn default_merge_policy() -> ConfigMergePolicy {
        ConfigMergePolicy {
            kind: "log".to_string(),
            min_merge_size: 8,
            min_layer_size: 10_000,
            level_log_size: 0.75,
        }
    }

    pub fn default_consul_addr() -> String {
        "127.0.0.1:8500".to_string()
    }

    pub fn default_cluster_name() -> String {
        "kitsune".to_string()
    }

    pub fn default_leader() -> bool {
        false
    }

    pub fn default_nodes() -> Vec<String> {
        Vec::new()
    }

    pub fn default_experimental() -> bool {
        false
    }

    pub fn default_id() -> u64 {
        1
    }

    pub fn default_rpc_port() -> u16 {
        8081
    }

    pub fn get_channel<T>(&self) -> (Sender<T>, Receiver<T>) {
        if self.bulk_buffer_size == 0 {
            unbounded::<T>()
        } else {
            bounded::<T>(self.bulk_buffer_size)
        }
    }

    pub fn get_nodes(&self) -> Vec<String> {
        self.experimental_features.nodes.clone()
    }

    pub fn get_merge_policy(&self) -> Box<dyn MergePolicy> {
        match self.merge_policy.get_kind() {
            MergePolicyType::Log => {
                let mut mp = LogMergePolicy::default();
                mp.set_level_log_size(self.merge_policy.level_log_size);
                mp.set_min_layer_size(self.merge_policy.min_layer_size);
                mp.set_min_merge_size(self.merge_policy.min_merge_size);
                Box::new(mp)
            }
            MergePolicyType::NoMerge => Box::new(NoMergePolicy::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use toshi_test::cmp_float;

    #[test]
    fn valid_default_config() {
        let default = Settings::from_str("").unwrap();
        assert_eq!(default.host, "0.0.0.0");
        assert_eq!(default.port, 8080);
        assert_eq!(default.path, "data/");
        assert_eq!(default.writer_memory, 200_000_000);
        assert_eq!(default.log_level, "info");
        assert_eq!(default.json_parsing_threads, 4);
        assert_eq!(default.bulk_buffer_size, 10000);
        assert_eq!(default.merge_policy.kind, "log");
        assert_eq!(default.merge_policy.level_log_size, 0.75);
        assert_eq!(default.merge_policy.min_layer_size, 10_000);
        assert_eq!(default.merge_policy.min_merge_size, 8);
        assert_eq!(default.experimental, false);
        assert_eq!(default.experimental_features.leader, false);
    }

    #[test]
    fn valid_merge_policy() {
        let cfg = r#"
            [merge_policy]
            kind = "log"
            level_log_size = 10.5
            min_layer_size = 20
            min_merge_size = 30"#;

        let config = Settings::from_str(cfg).unwrap();
        assert_eq!(cmp_float(config.merge_policy.level_log_size as f32, 10.5), true);
        assert_eq!(config.merge_policy.min_layer_size, 20);
        assert_eq!(config.merge_policy.min_merge_size, 30);
    }

    #[test]
    fn valid_no_merge_policy() {
        let cfg = r#"
            [merge_policy]
            kind = "nomerge""#;

        let config = Settings::from_str(cfg).unwrap();

        assert!(config.merge_policy.get_kind() == MergePolicyType::NoMerge);
        assert_eq!(config.merge_policy.kind, "nomerge");
        assert_eq!(config.merge_policy.level_log_size, 0.75);
        assert_eq!(config.merge_policy.min_layer_size, 10_000);
        assert_eq!(config.merge_policy.min_merge_size, 8);
    }

    #[test]
    #[should_panic]
    fn bad_config_file() {
        Settings::new("asdf/casdf").unwrap();
    }

    #[test]
    #[should_panic]
    fn bad_merge_type() {
        let cfg = r#"
            [merge_policy]
            kind = "asdf1234""#;

        let config = Settings::from_str(cfg).unwrap();
        config.get_merge_policy();
    }
}
