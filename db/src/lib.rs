//! an embedded database using the sled framework

pub mod api;
use serde::Serialize;
pub mod types;
use anyhow::{anyhow, Result};
use bonerjams_config::database::DbOpts;
use sled::{IVec, Tree};
use std::sync::Arc;

use self::types::{DbKey, DbTrees};

pub mod prelude {
    pub use super::api::{kv_server::*, types::*};
    pub use super::types::*;
}

/// Database is the main embedded database object using the
/// sled db
#[derive(Clone)]
pub struct Database {
    db: sled::Db,
}

/// DbTree is a wrapper around the sled::Tree type providing
/// convenience functions
#[derive(Clone)]
pub struct DbTree {
    pub tree: Tree,
}

/// DbBatch is a wrapper around the sled::Batch type providing
/// convenience functions
#[derive(Default, Clone)]
pub struct DbBatch {
    batch: sled::Batch,
    count: u64,
}

impl Database {
    /// returns a new sled database
    pub fn new(cfg: &DbOpts) -> Result<Arc<Self>> {
        let sled_config: sled::Config = cfg.into();
        let db = sled_config.open()?;
        drop(sled_config);
        Ok(Arc::new(Database { db }))
    }
    /// opens the given database tree
    pub fn open_tree(self: &Arc<Self>, tree: DbTrees) -> Result<Arc<DbTree>> {
        DbTree::open(&self.db, tree)
    }
    /// opens the given db tree, return a vector of (key, value)
    pub fn list_values(self: &Arc<Self>, tree: DbTrees) -> Result<Vec<(IVec, IVec)>> {
        let tree = self.open_tree(tree)?;
        Ok(tree
            .iter()
            .filter_map(|entry| {
                if let Ok((key, value)) = entry {
                    Some((key, value))
                } else {
                    None
                }
            })
            .collect())
    }
    pub async fn flush_async(&self) -> sled::Result<usize> {
        self.db.flush_async().await
    }
    /// flushes teh database
    pub fn flush(self: &Arc<Self>) -> Result<usize> {
        Ok(self.db.flush()?)
    }
    /// returns a clone of the inner database
    pub fn inner(self: &Arc<Self>) -> sled::Db {
        self.db.clone()
    }
    /// destroys all trees except the default tree
    pub fn destroy(self: &Arc<Self>) {
        self.db
            .tree_names()
            .iter()
            .filter(|tree_name| tree_name.as_ref().ne(types::DEFAULT_TREE_ID.as_bytes()))
            .for_each(|tree_name| {
                if let Err(err) = self.db.drop_tree(tree_name) {
                    log::error!("failed to drop tree {:?}: {:#?}", tree_name.as_ref(), err);
                }
            });
    }
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> sled::Result<Option<sled::IVec>> {
        self.db.get(key)
    }
    pub fn deserialize<K: AsRef<[u8]>, T>(&self, key: K) -> Result<T>
    where
        T: serde::de::DeserializeOwned + Clone,
    {
        let value = self.get(key)?;
        if let Some(value) = value {
            let result = serde_json::from_slice(&value)?;
            Ok(result)
        } else {
            Err(anyhow!("value for key is None"))
        }
    }
    pub fn apply_batch(&self, batch: &mut DbBatch) -> sled::Result<()> {
        self.db.apply_batch(batch.take_inner())
    }
    /// insert raw, untyped bytes
    pub fn insert_raw(&self, key: &[u8], value: &[u8]) -> Result<Option<sled::IVec>> {
        Ok(self.db.insert(key, value)?)
    }
    /// inserts a value into the default tree
    pub fn insert<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + DbKey,
    {
        self.db.insert(value.key()?, serde_json::to_vec(value)?)?;
        Ok(())
    }
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }
}

impl DbTree {
    pub fn open(db: &sled::Db, tree: DbTrees) -> Result<Arc<Self>> {
        let tree = db.open_tree(&tree.to_string())?;
        Ok(Arc::new(Self { tree }))
    }
    pub fn len(&self) -> usize {
        self.tree.len()
    }
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
    pub fn iter(&self) -> sled::Iter {
        self.tree.iter()
    }
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> sled::Result<bool> {
        self.tree.contains_key(key)
    }
    pub fn flush(&self) -> sled::Result<usize> {
        self.tree.flush()
    }
    pub async fn flush_async(&self) -> sled::Result<usize> {
        self.tree.flush_async().await
    }
    pub fn apply_batch(&self, batch: &mut DbBatch) -> sled::Result<()> {
        self.tree.apply_batch(batch.take_inner())
    }
    pub fn insert<T>(&self, value: &T) -> Result<Option<sled::IVec>>
    where
        T: Serialize + DbKey,
    {
        Ok(self.tree.insert(value.key()?, serde_json::to_vec(value)?)?)
    }
    /// insert raw, untyped bytes
    pub fn insert_raw(&self, key: &[u8], value: &[u8]) -> Result<Option<sled::IVec>> {
        Ok(self.tree.insert(key, value)?)
    }
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> sled::Result<Option<sled::IVec>> {
        self.tree.get(key)
    }
    /* currently broken
    pub fn entries2<K: PartialEq + DbKey, T>(&self, skip_keys: &[K]) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        Ok(self
            .tree
            .into_iter()
            .keys()
            .filter_map(|key| match key {
                Ok(key) => Some(key),
                Err(_) => None,
            })
            .filter(|key| {
                skip_keys
                    .iter()
                    .filter_map(|k| {
                        if let Ok(key) = k.key() {
                            Some(key)
                        } else {
                            None
                        }
                    })
                    .any(|x| x == key.as_ref().to_vec())
            })
            .filter_map(|key| {
                if let Ok(value) = self.deserialize(key) {
                    Some(value)
                } else {
                    None
                }
            })
            .collect())
    }*/
    pub fn deserialize<K: AsRef<[u8]>, T>(&self, key: K) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = self.get(key)?;
        if let Some(value) = value {
            Ok(serde_json::from_slice(&value)?)
        } else {
            Err(anyhow!("value for key is None"))
        }
    }
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.tree.remove(key)?;
        Ok(())
    }
}

impl DbBatch {
    pub fn new() -> DbBatch {
        DbBatch {
            batch: Default::default(),
            count: 0,
        }
    }
    pub fn remove<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + DbKey,
    {
        self.batch.remove(value.key()?);
        self.count += 1;
        Ok(())
    }
    /// removes raw, untyped bytes
    pub fn remove_raw(&mut self, key: &[u8]) -> Result<()> {
        self.batch.remove(key);
        self.count += 1;
        Ok(())
    }
    /// inserts raw untyped bytes
    pub fn insert_raw(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.batch.insert(key, value);
        self.count += 1;
        Ok(())
    }
    pub fn insert<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + DbKey,
    {
        self.batch.insert(value.key()?, serde_json::to_vec(value)?);
        self.count += 1;
        Ok(())
    }
    /// returns the inner batch, and should only be used when the batch object
    /// is finished with and the batch needs to be applied, as it replaces the inner
    /// batch with its default version
    pub fn take_inner(&mut self) -> sled::Batch {
        std::mem::take(&mut self.batch)
    }
    pub fn inner(&self) -> &sled::Batch {
        &self.batch
    }
    pub fn count(&self) -> u64 {
        self.count
    }
}
