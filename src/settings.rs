use config::Source;
use config::{Config, ConfigError, File, FileFormat};
use tantivy::merge_policy::*;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const HEADER: &str = r#"
  ______         __   _   ____                 __
 /_  __/__  ___ / /  (_) / __/__ ___ _________/ /
  / / / _ \(_-</ _ \/ / _\ \/ -_) _ `/ __/ __/ _ \
 /_/  \___/___/_//_/_/ /___/\__/\_,_/_/  \__/_//_/
 Such Relevance, Much Index, Many Search, Wow
 "#;

#[derive(Debug, Deserialize)]
pub enum MergePolicyType {
    Log,
    NoMerge,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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
    #[serde(default = "Settings::default_merge_policy")]
    pub merge_policy: ConfigMergePolicy,
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> { Settings::from_config(File::with_name(path)) }

    pub fn default() -> Result<Self, ConfigError> {
        Settings::from_config(File::from_str("", FileFormat::Toml))
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
    pub fn default_merge_policy() -> ConfigMergePolicy {
        ConfigMergePolicy {
            kind: "log".to_string(),
            min_merge_size: None,
            min_layer_size: None,
            level_log_size: None
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

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::new("config/config.toml").expect("Bad Config");
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn valid_default_config() {
        let default = Settings::default().unwrap();
        assert_eq!(default.host, "localhost");
        assert_eq!(default.port, 8080);
        assert_eq!(default.path, "data/");
        assert_eq!(default.writer_memory, 200_000_000);
        assert_eq!(default.log_level, "info");
        assert_eq!(default.json_parsing_threads, 4);
        assert_eq!(default.merge_policy.kind, "log");
        assert_eq!(default.merge_policy.level_log_size, None);
        assert_eq!(default.merge_policy.min_layer_size, None);
        assert_eq!(default.merge_policy.min_merge_size, None);
    }

}
