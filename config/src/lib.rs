use serde::{Serialize, Deserialize};
pub mod database;

#[derive(Clone, Deserialize, Serialize)]
pub struct Configuration {
    pub db: database::DbOpts,
    pub rpc_endpoint: String,
}