use std::hash::Hasher;
use std::sync::Arc;

use crate::api::error::Error;
use crate::prelude::{GetKVsResponse, Status, WrappedDocument};
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use datacake::cluster::{Consistency, Storage};

use super::error::ApiResult;
use super::ApiState;
pub async fn get_value<S: Storage + Send + Sync + 'static>(
    State(handle): State<Arc<ApiState<S>>>,
    Json(mut input): Json<crate::api::types::GetKVsRequest>,
) -> ApiResult<(StatusCode, Json<GetKVsResponse>)> {
    let mut response = GetKVsResponse {
        entries: Default::default(),
    };
    for (namespace, keys) in input.entries.iter_mut() {
        let hashed_keys = keys.iter_mut().map(|key| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            hasher.write(key.as_bytes());
            hasher.finish()
        });
        match handle.cluster_api.get_many(namespace, hashed_keys).await {
            Ok(res) => {
                response.entries.insert(
                    namespace.clone(),
                    res.filter_map(|doc| {
                        let wrapped_document: WrappedDocument =
                            match serde_json::from_slice(&doc.data[..]) {
                                Ok(doc) => doc,
                                Err(_) => return None,
                            };
                        Some(wrapped_document)
                    })
                    .collect::<Vec<_>>(),
                );
            }
            Err(err) => {
                return Err(Error::CustomServerError(err.to_string()).into());
            }
        }
    }
    Ok((StatusCode::OK, Json(response)))
}

pub async fn put_value<S: Storage + Send + Sync + 'static>(
    State(handle): State<Arc<ApiState<S>>>,
    Json(mut input): Json<crate::api::types::PutKVsRequest>,
) -> ApiResult<(StatusCode, Json<Status>)> {
    for (namespace, key_values) in input.entries.iter_mut() {
        let key_values = key_values.iter_mut().filter_map(|kv| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            hasher.write((&kv.key).as_bytes());
            let wrapped_document = match serde_json::to_vec(&WrappedDocument {
                key: std::mem::take(&mut kv.key),
                data: std::mem::take(&mut kv.value),
            }) {
                Ok(doc) => doc,
                Err(err) => {
                    log::error!("failed to serialize document {:#?}", err);
                    return None;
                }
            };
            Some((hasher.finish(), wrapped_document))
        });
        if let Err(err) = handle
            .cluster_api
            .put_many(namespace, key_values, Consistency::EachQuorum)
            .await
        {
            return Err(Error::CustomServerError(err.to_string()).into());
        }
    }
    Ok((
        StatusCode::OK,
        Json(Status {
            msg: "ok".to_string(),
        }),
    ))
}

pub async fn remove_value<S: Storage + Send + Sync + 'static>(
    State(handle): State<Arc<ApiState<S>>>,
    Json(mut input): Json<crate::api::types::DeleteKVsRequest>,
) -> ApiResult<(StatusCode, Json<Status>)> {
    for (namespace, keys) in input.entries.iter_mut() {
        let hashed_keys = keys.iter_mut().map(|key| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            hasher.write(key.as_bytes());
            hasher.finish()
        });
        if let Err(err) = handle
            .cluster_api
            .del_many(namespace, hashed_keys, Consistency::EachQuorum)
            .await
        {
            return Err(Error::CustomServerError(err.to_string()).into());
        }
    }
    Ok((
        StatusCode::OK,
        Json(Status {
            msg: "ok".to_string(),
        }),
    ))
}
