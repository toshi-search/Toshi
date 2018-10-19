use clap::ArgMatches;
use config::{Config, ConfigError, File, FileFormat, Source};
use crossbeam_channel::*;
use tantivy::merge_policy::*;

use std::str::FromStr;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const HEADER: &str = r#"
  ______         __   _   ____                 __
 /_  __/__  ___ / /  (_) / __/__ ___ _________/ /
  / / / _ \(_-</ _ \/ / _\ \/ -_) _ `/ __/ __/ _ \
 /_/  \___/___/_//_/_/ /___/\__/\_,_/_/  \__/_//_/
 Such Relevance, Much Index, Many Search, Wow
 "#;

#[derive(PartialEq)]
pub enum MergePolicyType {
    Log,
    NoMerge,
}

#[derive(Deserialize, Clone)]
pub struct ConfigMergePolicy {
    kind:           String,
    min_merge_size: Option<usize>,
    min_layer_size: Option<u32>,
    level_log_size: Option<f64>,
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

#[derive(Deserialize, Clone)]
pub struct Settings {
    #[serde(default = "Settings::default_host")]
    pub host: String,
    #[serde(default = "Settings::default_port")]
    pub port: u16,
    #[serde(default = "Settings::default_path")]
    pub path: String,
    #[serde(default = "Settings::default_level")]
    pub log_level: String,
    #[serde(default = "Settings::default_writer_memory")]
    pub writer_memory: usize,
    #[serde(default = "Settings::default_json_parsing_threads")]
    pub json_parsing_threads: usize,
    #[serde(default = "Settings::default_auto_commit_duration")]
    pub auto_commit_duration: u64,
    #[serde(default = "Settings::default_bulk_buffer_size")]
    pub bulk_buffer_size: usize,

    #[serde(default = "Settings::default_merge_policy")]
    pub merge_policy: ConfigMergePolicy,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            host:                 Settings::default_host(),
            port:                 Settings::default_port(),
            path:                 Settings::default_path(),
            log_level:            Settings::default_level(),
            writer_memory:        Settings::default_writer_memory(),
            json_parsing_threads: Settings::default_json_parsing_threads(),
            auto_commit_duration: Settings::default_auto_commit_duration(),
            bulk_buffer_size:     Settings::default_bulk_buffer_size(),
            merge_policy:         Settings::default_merge_policy(),
        }
    }
}

impl FromStr for Settings {
    type Err = ConfigError;

    fn from_str(cfg: &str) -> Result<Self, ConfigError> { Self::from_config(File::from_str(cfg, FileFormat::Toml)) }
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> { Self::from_config(File::with_name(path)) }

    pub fn from_args(args: ArgMatches) -> Self {
        Self {
            host: args.value_of("host").unwrap().to_string(),
            port: args.value_of("port").unwrap().parse::<u16>().unwrap(),
            path: args.value_of("path").unwrap().to_string(),
            log_level: args.value_of("level").unwrap().to_string(),
            ..Default::default()
        }
    }

    pub fn from_config<T: Source + Send + Sync + 'static>(c: T) -> Result<Self, ConfigError> {
        let mut cfg = Config::new();
        match cfg.merge(c) {
            Ok(_) => {}
            Err(e) => panic!("Problem with config file: {}", e),
        };
        cfg.try_into()
    }

    pub fn default_pretty() -> bool { false }

    pub fn default_result_limit() -> usize { 100 }

    pub fn default_host() -> String { "localhost".into() }

    pub fn default_path() -> String { "data/".into() }

    pub fn default_port() -> u16 { 8080 }

    pub fn default_level() -> String { "info".into() }

    pub fn default_writer_memory() -> usize { 200_000_000 }

    pub fn default_json_parsing_threads() -> usize { 4 }

    pub fn default_bulk_buffer_size() -> usize { 10000 }

    pub fn default_auto_commit_duration() -> u64 { 10 }

    pub fn default_merge_policy() -> ConfigMergePolicy {
        ConfigMergePolicy {
            kind:           "log".to_string(),
            min_merge_size: None,
            min_layer_size: None,
            level_log_size: None,
        }
    }

    pub fn get_channel<T>(&self) -> (Sender<T>, Receiver<T>) {
        if self.bulk_buffer_size == 0 {
            unbounded::<T>()
        } else {
            bounded::<T>(self.bulk_buffer_size)
        }
    }

    pub fn get_merge_policy(&self) -> Box<MergePolicy> {
        match self.merge_policy.get_kind() {
            MergePolicyType::Log => {
                let mut mp = LogMergePolicy::default();
                if let Some(v) = self.merge_policy.level_log_size {
                    mp.set_level_log_size(v);
                }
                if let Some(v) = self.merge_policy.min_layer_size {
                    mp.set_min_layer_size(v);
                }
                if let Some(v) = self.merge_policy.min_merge_size {
                    mp.set_min_merge_size(v);
                }
                Box::new(mp)
            }
            MergePolicyType::NoMerge => Box::new(NoMergePolicy::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_default_config() {
        let default = Settings::from_str("").unwrap();
        assert_eq!(default.host, "localhost");
        assert_eq!(default.port, 8080);
        assert_eq!(default.path, "data/");
        assert_eq!(default.writer_memory, 200_000_000);
        assert_eq!(default.log_level, "info");
        assert_eq!(default.json_parsing_threads, 4);
        assert_eq!(default.bulk_buffer_size, 10000);
        assert_eq!(default.merge_policy.kind, "log");
        assert_eq!(default.merge_policy.level_log_size, None);
        assert_eq!(default.merge_policy.min_layer_size, None);
        assert_eq!(default.merge_policy.min_merge_size, None);
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

        assert_eq!(config.merge_policy.level_log_size.unwrap(), 10.5);
        assert_eq!(config.merge_policy.min_layer_size.unwrap(), 20);
        assert_eq!(config.merge_policy.min_merge_size.unwrap(), 30);
    }

    #[test]
    fn valid_no_merge_policy() {
        let cfg = r#"
            [merge_policy]
            kind = "nomerge""#;

        let config = Settings::from_str(cfg).unwrap();

        assert!(config.merge_policy.get_kind() == MergePolicyType::NoMerge);
        assert_eq!(config.merge_policy.kind, "nomerge");
        assert_eq!(config.merge_policy.level_log_size, None);
        assert_eq!(config.merge_policy.min_layer_size, None);
        assert_eq!(config.merge_policy.min_merge_size, None);
    }

    #[test]
    #[should_panic]
    fn bad_config_file() { Settings::new("asdf/casdf").unwrap(); }

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
