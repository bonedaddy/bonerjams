use serde::{Deserialize, Serialize};
pub mod database;
use simplelog::*;

#[derive(Clone, Deserialize, Serialize)]
pub struct Configuration {
    pub db: database::DbOpts,
    pub rpc_endpoint: String,
    pub rpc_auth_token: String,
    pub debug_log: bool,
}

impl Configuration {
    pub fn init_log(&self) -> anyhow::Result<()> {
        if self.debug_log {
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
}
