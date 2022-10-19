/// the default tree identifier
pub const DEFAULT_TREE_ID: &str = "__sled__default";

pub trait DbKey {
    /// returns the key of value being inserted into the db
    fn key(&self) -> anyhow::Result<Vec<u8>>;
}

/// provides convenience types for interacting with trees
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbTrees<'a> {
    /// custom tree names specified as strings
    Custom(&'a str),
    /// custom tree names specified as raw bytes
    Binary(&'a [u8]),
    /// the default tree
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
