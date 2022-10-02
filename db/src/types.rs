/// the default tree identifier
pub const DEFAULT_TREE_ID: &str = "__sled__default";

pub trait DbKey {
    /// returns the key of value being inserted into the db
    fn key(&self) -> anyhow::Result<Vec<u8>>;
}

/// various trees and their keys for use with sled
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DbTrees<'a> {
    Custom(&'a str),
    Binary(&'a [u8]),
    Default,
}

impl<'a> ToString for DbTrees<'a> {
    fn to_string(&self) -> String {
        match self {
            Self::Binary(tree_key) => base64::encode(tree_key),
            DbTrees::Custom(tree_key) => tree_key.to_string(),
            DbTrees::Default => DEFAULT_TREE_ID.to_string(),
        }
    }
}

impl<'a> DbTrees<'a> {
    pub fn raw(&self) -> Vec<u8> {
        match self {
            DbTrees::Custom(tree_key) => tree_key.as_bytes().to_vec(),
            DbTrees::Default => DEFAULT_TREE_ID.as_bytes().to_vec(),
            DbTrees::Binary(tree_key) => tree_key.to_vec(),
        }
    }
}
