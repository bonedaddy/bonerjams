use serde::{Deserialize, Serialize};
pub mod cluster;
pub mod database;
use simplelog::*;

#[derive(Clone, Deserialize, Serialize, Default)]
pub struct Configuration {
    pub db: database::DbOpts,
    pub api: API,
}

impl Configuration {
    pub fn load(path: &str, from_json: bool) -> anyhow::Result<Configuration> {
        let data = std::fs::read(path).expect("failed to read file");
        let config: Configuration = if from_json {
            serde_json::from_slice(data.as_slice())?
        } else {
            serde_yaml::from_slice(data.as_slice())?
        };
        Ok(config)
    }
    /// saves the configuration file to disk.
    /// when `store_market_data`is false, the `Markets.data` field is reset to default
    pub fn save(&self, path: &str, as_json: bool) -> anyhow::Result<()> {
        let data = if as_json {
            serde_json::to_string_pretty(self)?
        } else {
            serde_yaml::to_string(self)?
        };
        std::fs::write(path, data).expect("failed to write to file");
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct API {
    pub listen_address: String,
    pub concurrency_limit: u64,
    pub timeout: u64,
    pub cors: Option<CORSConfig>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct CORSConfig {
    pub allowed_origins: Vec<String>,
    pub allow_credentials: bool,
}

impl Default for API {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:3001".to_string(),
            concurrency_limit: 64,
            timeout: 64,
            cors: Some(CORSConfig {
                allowed_origins: vec!["*".to_string()],
                allow_credentials: false,
            }),
            tls_cert: None,
            tls_key: None,
        }
    }
}

pub fn init_log(debug_log: bool) -> anyhow::Result<()> {
    if debug_log {
        TermLogger::init(
            LevelFilter::Debug,
            ConfigBuilder::new()
                .set_location_level(LevelFilter::Debug)
                .build(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )?;
    } else {
        TermLogger::init(
            LevelFilter::Info,
            ConfigBuilder::new()
                .set_location_level(LevelFilter::Error)
                .build(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )?;
    }
    Ok(())
}
