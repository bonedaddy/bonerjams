use std::collections::HashMap;

use crate::types::DbKey;
use tokio::sync::mpsc;
use tonic::transport::NamedService;

pub struct BatchPutEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone)]
pub struct State {
    pub db: std::sync::Arc<crate::Database>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Empty;

/// type alias for a vector of key-values
pub type Values = Vec<KeyValue>;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[repr(u8)]
pub enum Exists {
    Found = 0_u8,
    NotFound = 1_u8,
}

impl Exists {
    pub fn bool(&self) -> bool {
        self.into()
    }
}

impl From<Exists> for bool {
    fn from(input: Exists) -> Self {
        (&input).into()
    }
}

impl From<&Exists> for bool {
    fn from(input: &Exists) -> Self {
        match input {
            Exists::Found => true,
            Exists::NotFound => false,
        }
    }
}

/// request object used for batch deletion of keys into the key value store
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DeleteKVsRequest {
    /// key (tree) => value (vec![(key, value)])
    ///
    /// allow batch insertion of key-value records, grouping the
    /// records to insert based on the tree they will be inserted them
    ///
    /// the hashmap key is a base64 encoded string
    pub entries: HashMap<String, Vec<Vec<u8>>>,
}

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

/// request object used for batch insertion of keys into the key value store
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ExistsKVsRequest {
    /// key (tree) => value (vec![(key, value)])
    ///
    /// allow batch insertion of key-value records, grouping the
    /// records to insert based on the tree they will be inserted them
    ///
    /// the hashmap key is a base64 encoded string
    pub entries: HashMap<String, Vec<Vec<u8>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ExistKVsResponse {
    pub entries: HashMap<String, HashMap<String, Exists>>,
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

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct HealthCheck {
    pub ok: bool,
}

impl NamedService for HealthCheck {
    const NAME: &'static str = "BONERJAMS";
}

impl AsRef<str> for HealthCheck {
    fn as_ref(&self) -> &str {
        Self::NAME
    }
}

#[tonic_rpc::tonic_rpc(cbor)]
trait KeyValueStore {
    /// Returns the value matching the given key under the default tree
    fn get_kv(key: Vec<u8>) -> Vec<u8>;
    /// List all values within a given tree
    fn list(tree: Vec<u8>) -> Values;
    /// Inserts a key-value into the default tree
    fn put_kv(key: Vec<u8>, value: Vec<u8>) -> Empty;
    /// Batch insert multiple key-values under any tree.
    /// If a non default tree is specified that doesnt exist it is created.
    fn put_kvs(request: PutKVsRequest) -> Empty;
    /// Removes a single key-value record from the default tree
    fn delete_kv(key: Vec<u8>) -> Empty;
    /// Batch remove multiple records under any tree
    fn delete_kvs(request: DeleteKVsRequest) -> Empty;
    fn exist(key: Vec<u8>) -> Exists;
    fn batch_exist(request: ExistsKVsRequest) -> ExistKVsResponse;
    /// Returns a basic healthcheck that doesn't conform to the
    /// gRPC health checking service standard
    fn health_check() -> HealthCheck;
}

use std::sync::{Arc, Mutex};

/// an update sent over pubsub streams, with base64 encoded
/// keys and values
pub type Update = Result<(String, String), tonic::Status>;

/// a basis state object to be used with the reference pubsub implementation
#[derive(Clone)]
pub struct PubSubState {
    pub subscribers: Arc<super::pubsub::Broadcaster<Update>>,
}

/// Provides a trait that can be used to implement extremely basic publish/subscribe patterns.
/// Out of the box support is provided for channels identifier with strings, that send values of type
/// string, using the `PubSubState`.
#[tonic_rpc::tonic_rpc(cbor)]
trait PubSub {
    #[server_streaming]
    #[client_streaming]
    /// opens up a streaming connection that will receiving incoming messages on `channel`
    fn sub(channel: String) -> (String, String);
    /// publishes the given value on the given channel
    fn publish(channel: String, value: String) -> ();
}
