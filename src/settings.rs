use config::{Config, ConfigError, File};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const HEADER: &str = r#"
  ______         __   _   ____                 __
 /_  __/__  ___ / /  (_) / __/__ ___ _________/ /
  / / / _ \(_-</ _ \/ / _\ \/ -_) _ `/ __/ __/ _ \
 /_/  \___/___/_//_/_/ /___/\__/\_,_/_/  \__/_//_/
 Such Relevance, Much Index, Many Search, Wow"#;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub host:          String,
    pub port:          u16,
    pub path:          String,
    pub writer_memory: usize,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut cfg = Config::new();
        match cfg.merge(File::with_name("config/config.toml")) {
            Ok(_) => {}
            Err(e) => panic!("Problem with config file: {}", e),
        };
        cfg.try_into()
    }
}

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::new().expect("Bad Config");
}
