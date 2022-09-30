use db::types::DbKey;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct RequestGet {
    pub keys: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ResponseGet {
    pub values: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct RequestPut {
    pub entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub tree: String,
    pub data: KeyValue,
}

#[derive(Serialize, Deserialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
impl DbKey for KeyValue {
    fn key(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.key.clone())
    }
}
#[derive(Serialize, Deserialize)]
pub struct Empty {}