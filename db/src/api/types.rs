use std::collections::HashMap;

use crate::types::DbKey;

pub struct BatchPutEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone)]
pub struct State {
    pub db: std::sync::Arc<crate::Database>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Empty;

/// type alias for a vector of key-values
pub type Values = Vec<KeyValue>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
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
    /// key (namespace) => value (vec![(key, value)])
    ///
    /// allow batch insertion of key-value records, grouping the
    /// records to insert based on the tree they will be inserted them
    pub entries: HashMap<String, Vec<String>>,
}

/// request object used for batch insertion of keys into the key value store
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PutKVsRequest {
    /// key (namespace) => value (vec![(key, value)])
    ///
    /// allow batch insertion of key-value records, grouping the
    /// records to insert based on the tree they will be inserted them
    pub entries: HashMap<String, Vec<KeyValue>>,
}

/// request object used for batch insertion of keys into the key value store
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ExistsKVsRequest {
    /// key (namespace) => value (vec![(key, value)])
    ///
    /// allow batch insertion of key-value records, grouping the
    /// records to insert based on the tree they will be inserted them
    ///
    /// the hashmap key is a base64 encoded string
    pub entries: HashMap<String, Vec<String>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GetKVsRequest {
    pub entries: HashMap<String, Vec<String>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GetKVsResponse {
    pub entries: HashMap<String, Vec<WrappedDocument>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ExistKVsResponse {
    pub entries: HashMap<String, HashMap<String, Exists>>,
}

/// a wrapper object used when bulk inserting records into the keyvalue store
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Status {
    pub msg: String,
}

/// because datacake accepts u64 keys, and we can't store
/// the actual stringified key within the document, we
/// need to wrap the data that the document contains with
/// the actual key
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct WrappedDocument {
    pub key: String,
    pub data: Vec<u8>,
}
