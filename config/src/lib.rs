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
    pub auth_token: String,
    pub connection: ConnType,
    pub tls_cert: String,
    pub tls_key: String,
}

impl RPC {
    /// returns the url used by the server when establishing the listener
    pub fn server_url(&self) -> String {
        self.connection.to_string()
    }
    pub fn client_url(&self) -> String {
        self.connection.to_client_url()
    }
}

pub type RpcHost = String;
pub type RpcPort = String;
pub type UDSPath = String;

#[derive(Serialize, Deserialize, Clone)]
pub enum ConnType {
    HTTPS(RpcHost, RpcPort),
    HTTP(RpcHost, RpcPort),
    /// unix domain socket
    UDS(UDSPath),
}

/// the return value of `to_string` can be used for the server listening address
impl ToString for ConnType {
    fn to_string(&self) -> String {
        match self {
            ConnType::HTTP(host, port) | ConnType::HTTPS(host, port) => {
                format!("{}:{}", host, port)
            }
            ConnType::UDS(path) => {
                path.to_string()
            }
        }
    }
}

impl ConnType {
    /// used to convert a server url into a client url by prefixing the protocol
    pub fn to_client_url(&self) -> String {
        match self {
            ConnType::HTTP { .. } => {
                format!("http://{}", self.to_string())
            }
            ConnType::HTTPS { .. } => {
                format!("https://{}", self.to_string())
            }
            ConnType::UDS { .. } => {
                format!("unix://{}", self.to_string())
            }
        }
    }
}

impl Default for RPC {
    fn default() -> Self {
        Self {
            auth_token: "".to_string(),
            connection: ConnType::HTTP(
                "127.0.0.1".to_string() as RpcHost,
                "6969".to_string() as RpcPort,
            ),
            tls_cert: "".to_string(),
            tls_key: "".to_string(),
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
