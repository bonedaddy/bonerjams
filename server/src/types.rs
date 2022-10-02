use std::collections::HashMap;

use db::types::DbKey;

#[derive(Clone)]
pub struct State {
    pub db: std::sync::Arc<db::Database>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Empty;

/// type alias for a vector of key-values
pub type Values = Vec<KeyValue>;

/// request object used for batch insertion of keys into the key value store
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PutKVsRequest {
    /// key (tree) => value (vec![(key, value)])
    ///
    /// allow batch insertion of key-value records, grouping the
    /// records to insert based on the tree they will be inserted them
    ///
    /// the hashmap key is a base64 encoded string
    pub entries: HashMap<String, Vec<KeyValue>>,
}

/// a wrapper object used when bulk inserting records into the keyvalue store
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// an object that wraps the value to insert into the keyvalue store
/// preventing the key from being serialized as part of the value
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct KeystoreValue {
    #[serde(skip_deserializing, skip_serializing)]
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// implements the DbKey trait for the KeystoreValue object
/// which is used to help information sled where to store the object
impl DbKey for KeystoreValue {
    fn key(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.key.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HealthCheck {
    pub ok: bool,
}

#[tonic_rpc::tonic_rpc(json)]
trait KeyValueStore {
    fn get_kv(key: Vec<u8>) -> Vec<u8>;
    /// list all values within a given tree
    fn list(tree: Vec<u8>) -> Values;
    /// put a key-value into the default tree
    /// use put_kvs for batch insertion, or for control
    /// of the trees which are used
    fn put_kv(key: Vec<u8>, value: Vec<u8>) -> Empty;
    fn put_kvs(request: PutKVsRequest) -> Empty;
    fn health_check() -> HealthCheck;
}
