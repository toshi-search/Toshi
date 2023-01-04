use std::str::FromStr;

use config::{Config, ConfigError, File, FileFormat, Source};
use serde::Deserialize;
use structopt::StructOpt;
use tantivy::merge_policy::*;

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

pub const DEFAULT_PRETTY: bool = false;
pub const DEFAULT_RESULT_LIMIT: usize = 100;
pub const DEFAULT_HOST: &str = "0.0.0.0";
pub const DEFAULT_PATH: &str = "data/";
pub const DEFAULT_PORT: u16 = 8080;
pub const DEFAULT_LEVEL: &str = "info";
pub const DEFAULT_WRITER_MEMORY: usize = 200_000_000;
pub const DEFAULT_JSON_PARSING_THREADS: usize = 4;
pub const DEFAULT_BULK_BUFFER_SIZE: usize = 10000;
pub const DEFAULT_MAX_LINE_LENGTH: usize = 10000;
pub const DEFAULT_AUTO_COMMIT_DURATION: f32 = 10.0;
pub const DEFAULT_LEADER: bool = false;
pub const DEFAULT_NODES: Vec<String> = Vec::new();
pub const DEFAULT_ID: u64 = 1;
pub const DEFAULT_RPC_PORT: u16 = 8081;
pub const DEFAULT_LEVEL_LOG_SIZE: f64 = 0.75;
pub const DEFAULT_MIN_LAYER_SIZE: u32 = 10_000;
pub const DEFAULT_MIN_MERGE_SIZE: usize = 8;

pub fn default_merge_policy() -> ConfigMergePolicy {
    ConfigMergePolicy {
        kind: "log".to_string(),
        min_merge_size: DEFAULT_MIN_MERGE_SIZE,
        min_layer_size: DEFAULT_MIN_LAYER_SIZE,
        level_log_size: DEFAULT_LEVEL_LOG_SIZE,
    }
}

pub fn settings() -> Settings {
    let options = Settings::from_args();
    if !&options.config.is_empty() {
        Settings::new(&options.config).expect("Invalid Configuration File")
    } else {
        options
    }
}

#[derive(Deserialize, Clone, Debug, StructOpt)]
#[serde(default = "ConfigMergePolicy::default")]
pub struct ConfigMergePolicy {
    #[structopt(long, default_value = "log")]
    kind: String,
    #[structopt(long, default_value)]
    min_merge_size: usize,
    #[structopt(long, default_value)]
    min_layer_size: u32,
    #[structopt(long, default_value)]
    level_log_size: f64,
}

impl Default for ConfigMergePolicy {
    fn default() -> Self {
        Self {
            kind: "log".into(),
            min_merge_size: DEFAULT_MIN_MERGE_SIZE,
            min_layer_size: DEFAULT_MIN_LAYER_SIZE,
            level_log_size: DEFAULT_LEVEL_LOG_SIZE,
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
    #[structopt(long)]
    pub leader: bool,
    #[structopt(long)]
    pub nodes: Vec<String>,
    #[structopt(long, default_value = "1")]
    pub id: u64,
    #[structopt(long, default_value = "8081")]
    pub rpc_port: u16,
}

#[derive(Deserialize, Clone, Debug, StructOpt)]
#[structopt(name = "toshi", version = env!("CARGO_PKG_VERSION"))]
#[serde(default = "Settings::default")]
pub struct Settings {
    #[serde(skip)]
    #[structopt(short, long, default_value = "config/config.toml")]
    pub config: String,
    #[structopt(short, long, default_value = "127.0.0.1")]
    pub host: String,
    #[structopt(short, long, default_value = "8080")]
    pub port: u16,
    #[structopt(short = "P", long, default_value = "data/")]
    pub path: String,
    #[structopt(short, long, default_value = "info")]
    pub log_level: String,
    #[structopt(short, long, default_value = "200000000")]
    pub writer_memory: usize,
    #[structopt(short, long, default_value = "4")]
    pub json_parsing_threads: usize,
    #[structopt(short, long, default_value = "5")]
    pub auto_commit_duration: f32,
    #[structopt(short, long, default_value = "10000")]
    pub bulk_buffer_size: usize,
    #[structopt(short, long, default_value = "10000")]
    pub max_line_length: usize,
    #[structopt(flatten)]
    pub merge_policy: ConfigMergePolicy,
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
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT,
            path: DEFAULT_PATH.into(),
            log_level: DEFAULT_LEVEL.into(),
            writer_memory: DEFAULT_WRITER_MEMORY,
            json_parsing_threads: DEFAULT_JSON_PARSING_THREADS,
            auto_commit_duration: DEFAULT_AUTO_COMMIT_DURATION,
            bulk_buffer_size: DEFAULT_BULK_BUFFER_SIZE,
            max_line_length: DEFAULT_MAX_LINE_LENGTH,
            merge_policy: ConfigMergePolicy::default(),
            experimental: false,
            experimental_features: Experimental::default(),
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
        Config::builder().add_source(c).build()?.try_deserialize::<Self>()
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
                mp.set_max_docs_before_merge(self.merge_policy.min_merge_size);
                Box::new(mp)
            }
            MergePolicyType::NoMerge => Box::new(NoMergePolicy::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::commit::tests::cmp_float;

    use super::*;

    #[test]
    fn valid_default_config() {
        let default = Settings::default();
        assert_eq!(default.host, "0.0.0.0");
        assert_eq!(default.port, 8080);
        assert_eq!(default.path, "data/");
        assert_eq!(default.writer_memory, 200_000_000);
        assert_eq!(default.log_level, "info");
        assert_eq!(default.json_parsing_threads, 4);
        assert_eq!(default.bulk_buffer_size, 10000);
        assert_eq!(default.max_line_length, 10000);
        assert_eq!(default.merge_policy.kind, "log");
        assert!(cmp_float(default.merge_policy.level_log_size as f32, 0.75));
        assert_eq!(default.merge_policy.min_layer_size, 10_000);
        assert_eq!(default.merge_policy.min_merge_size, 8);
        assert!(!default.experimental);
        assert!(!default.experimental_features.leader);
    }

    #[test]
    fn valid_merge_policy() {
        let cfg = r#"
            host = "asdf:8080"
            [merge_policy]
            kind = "log"
            level_log_size = 10.5
            min_layer_size = 20
            min_merge_size = 30"#;

        let config = Settings::from_str(cfg).unwrap();
        assert!(cmp_float(config.merge_policy.level_log_size as f32, 10.5));
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
        assert!(cmp_float(config.merge_policy.level_log_size as f32, 0.75));
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
