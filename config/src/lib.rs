use serde::{Deserialize, Serialize};
pub mod database;
use simplelog::*;

#[derive(Clone, Deserialize, Serialize, Default)]
pub struct Configuration {
    pub db: database::DbOpts,
    pub rpc: RPC,
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
pub struct RPC {
    pub proto: String,
    pub host: String,
    pub port: String,
    pub auth_token: String,
    pub connection: ConnType,
}

impl RPC {
    /// returns the url used by the server when establishing the listener
    pub fn server_url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
    pub fn client_url(&self) -> String {
        format!("{}://{}:{}", self.proto, self.host, self.port)
    }
}

pub type RpcHost = String;
pub type RpcPort = String;
pub type UDSPath = String;

#[derive(Serialize, Deserialize, Clone)]
pub enum ConnType {
    HTTP(RpcHost, RpcPort),
    /// unix domain socket
    UDS(UDSPath),
}

impl Default for RPC {
    fn default() -> Self {
        Self {
            proto: "http".to_string(),
            host: "127.0.0.1".to_string(),
            port: "6969".to_string(),
            auth_token: "".to_string(),
            connection: ConnType::HTTP(
                "127.0.0.1".to_string() as RpcHost,
                "6969".to_string() as RpcPort,
            ),
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
