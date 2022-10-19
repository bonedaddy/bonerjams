use super::types::*;
use crate::{
    types::{DbKey, DbTrees},
    DbBatch, DbTree,
};

use std::{collections::HashMap, sync::Arc};

use tonic::Status;

#[tonic::async_trait]
impl key_value_store_server::KeyValueStore for Arc<State> {
    async fn get_kv(
        &self,
        request: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Vec<u8>>, tonic::Status> {
        let db = self.db.clone();
        let arg = request.into_inner();
        match db.get(arg) {
            Ok(Some(key)) => Ok(tonic::Response::new(key.to_vec())),
            Ok(None) => Err(Status::new(tonic::Code::NotFound, "")),
            Err(err) => Err(Status::new(tonic::Code::Internal, err.to_string())),
        }
    }
    async fn list(
        &self,
        request: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Values>, tonic::Status> {
        let db = self.db.clone();
        let arg = request.into_inner();
        let tree = if arg.is_empty() {
            crate::types::DbTrees::Default
        } else {
            crate::types::DbTrees::Binary(&arg[..])
        };
        let db_tree = match db.open_tree(tree) {
            Ok(tree) => tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        let values: Values = db_tree
            .iter()
            .collect::<Vec<_>>()
            .iter()
            .flatten()
            .map(|(key, value)| KeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .collect();
        Ok(tonic::Response::new(values))
    }
    async fn put_kv(
        &self,
        key: tonic::Request<(Vec<u8>, Vec<u8>)>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let (key, value) = key.into_inner();
        let db_tree = match db.open_tree(crate::types::DbTrees::Default) {
            Ok(tree) => tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        match db_tree.insert_raw(&key[..], &value[..]) {
            Ok(_) => {
                self.async_flush(db_tree).await?;
                Ok(tonic::Response::new(Empty {}))
            }
            Err(err) => Err(Status::new(tonic::Code::Internal, err.to_string())),
        }
    }
    async fn put_kvs(
        &self,
        request: tonic::Request<PutKVsRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let mut args = request.into_inner();
        for (tree, values) in args.entries.iter_mut() {
            let tree_name = if tree.is_empty() {
                vec![]
            } else {
                match base64::decode(tree) {
                    Ok(tree_name) => tree_name,
                    Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
                }
            };
            let tree = if tree_name.is_empty() {
                crate::types::DbTrees::Default
            } else {
                crate::types::DbTrees::Binary(&tree_name[..])
            };
            let db_tree = match db.open_tree(tree) {
                Ok(tree) => tree,
                Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
            };
            let mut batch = DbBatch::new();
            for value in values.iter_mut() {
                if let Err(err) = batch.insert_raw(&value.key[..], &value.value[..]) {
                    return Err(Status::new(tonic::Code::Internal, err.to_string()));
                }
            }
            self.apply_and_flush(db_tree, &mut batch).await?;
        }
        Ok(tonic::Response::new(Empty {}))
    }
    async fn delete_kv(
        &self,
        request: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let key = request.into_inner();
        let db_tree = match db.open_tree(crate::types::DbTrees::Default) {
            Ok(tree) => tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        match db_tree.delete(&key[..]) {
            Ok(_) => {
                self.async_flush(db_tree).await?;
                Ok(tonic::Response::new(Empty {}))
            }
            Err(err) => Err(Status::new(tonic::Code::Internal, err.to_string())),
        }
    }
    async fn delete_kvs(
        &self,
        request: tonic::Request<DeleteKVsRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let mut args = request.into_inner();
        for (tree, keys) in args.entries.iter_mut() {
            let tree_name = if tree.is_empty() {
                vec![]
            } else {
                match base64::decode(tree) {
                    Ok(tree_name) => tree_name,
                    Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
                }
            };
            let tree = if tree_name.is_empty() {
                crate::types::DbTrees::Default
            } else {
                crate::types::DbTrees::Binary(&tree_name[..])
            };
            let db_tree = match db.open_tree(tree) {
                Ok(tree) => tree,
                Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
            };
            let mut batch = DbBatch::new();
            for key in keys.iter_mut() {
                if let Err(err) = batch.remove_raw(key) {
                    return Err(Status::new(tonic::Code::Internal, err.to_string()));
                }
            }
            self.apply_and_flush(db_tree, &mut batch).await?;
        }
        Ok(tonic::Response::new(Empty {}))
    }
    async fn health_check(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<HealthCheck>, tonic::Status> {
        Ok(tonic::Response::new(HealthCheck { ok: true }))
    }
    async fn exist(
        &self,
        key: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Exists>, tonic::Status> {
        let db_tree = match self.db.open_tree(DbTrees::Default) {
            Ok(db_tree) => db_tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        let exists = match db_tree.contains_key(key.into_inner()) {
            Ok(response) => {
                if response {
                    Exists::Found
                } else {
                    Exists::NotFound
                }
            }
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        Ok(tonic::Response::new(exists))
    }
    async fn batch_exist(
        &self,
        key: tonic::Request<ExistsKVsRequest>,
    ) -> Result<tonic::Response<ExistKVsResponse>, tonic::Status> {
        let req = key.into_inner();
        let mut response = ExistKVsResponse {
            entries: HashMap::with_capacity(req.entries.len()),
        };
        req.entries.into_iter().for_each(|(tree, keys)| {
            let tree_name = if tree.is_empty() {
                vec![]
            } else {
                match base64::decode(&tree) {
                    Ok(tree_name) => tree_name,
                    Err(_err) => return,
                }
            };
            let db_tree = if tree_name.is_empty() {
                crate::types::DbTrees::Default
            } else {
                crate::types::DbTrees::Binary(&tree_name[..])
            };
            let db_tree = match self.db.open_tree(db_tree) {
                Ok(db) => db,
                Err(_err) => return,
            };
            keys.into_iter().for_each(|key| {
                let contains = if let Ok(contains) = db_tree.contains_key(&key) {
                    contains
                } else {
                    false
                };
                match response.entries.get_mut(&tree) {
                    Some(entries) => {
                        entries.insert(
                            base64::encode(key),
                            if contains {
                                Exists::Found
                            } else {
                                Exists::NotFound
                            },
                        );
                    }
                    None => {
                        let mut res = HashMap::with_capacity(5);
                        res.insert(
                            base64::encode(key),
                            if contains {
                                Exists::Found
                            } else {
                                Exists::NotFound
                            },
                        );
                        response.entries.insert(tree.clone(), res);
                    }
                }
            })
        });
        Ok(tonic::Response::new(response))
    }
}

impl State {
    async fn apply_and_flush(
        &self,
        db_tree: Arc<DbTree>,
        batch: &mut DbBatch,
    ) -> Result<(), tonic::Status> {
        if let Err(err) = db_tree.apply_batch(batch) {
            return Err(Status::new(tonic::Code::Internal, err.to_string()));
        }
        self.async_flush(db_tree).await?;
        Ok(())
    }
    async fn async_flush(&self, db_tree: Arc<DbTree>) -> Result<(), tonic::Status> {
        if let Err(err) = db_tree.flush_async().await {
            return Err(Status::new(tonic::Code::Internal, err.to_string()));
        }
        if let Err(err) = self.db.flush_async().await {
            return Err(Status::new(tonic::Code::Internal, err.to_string()));
        }
        Ok(())
    }
}