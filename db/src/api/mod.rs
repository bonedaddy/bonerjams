//! Provides an api for a distributed, replicated key-value store built upon datacake

#[cfg(feature = "client")]
pub mod client;

pub mod error;
pub mod kv_server;
pub mod types;
use crate::types::DbKey;
use axum::error_handling::HandleErrorLayer;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::BoxError;
use axum::Router;
use bonerjams_config::API;
use datacake::cluster::DatacakeCluster;
use datacake::cluster::DatacakeHandle;
use datacake::cluster::Storage;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct ApiState<S: Storage + Send + Sync + 'static> {
    cluster_api: Arc<DatacakeHandle<S>>,
}

/// while the router accepts keys as string's, the underlying datastore (datacake) requires
/// keys be `u64`, therefore we use the Sip24 hasher used by `HashMap` to hash the keys before
/// insertion into the database
pub fn new_router<S: Storage + Send + Sync + 'static>(
    cluster: &DatacakeCluster<S>,
    api_conf: API,
) -> Router {
    let cluster_api = cluster.handle();
    let handle = Arc::new(ApiState {
        cluster_api: Arc::new(cluster_api),
    });
    let router = if let Some(cors_conf) = api_conf.cors {
        Router::new().layer(
            CorsLayer::new()
                .allow_origin(
                    cors_conf
                        .allowed_origins
                        .iter()
                        .filter_map(|origin| {
                            if let Ok(val) = HeaderValue::from_str(origin) {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .allow_credentials(cors_conf.allow_credentials),
        )
    } else {
        Router::new()
    };
    router
        .route("/put", post(self::kv_server::put_value))
        .route("/get", post(self::kv_server::get_value))
        .route("/delete", post(self::kv_server::remove_value))
        .layer(
            ServiceBuilder::new()
                // Handle errors from middleware
                .layer(HandleErrorLayer::new(handle_error))
                .load_shed()
                .concurrency_limit(api_conf.concurrency_limit as usize)
                .timeout(std::time::Duration::from_secs(api_conf.timeout))
                .layer(TraceLayer::new_for_http()),
        )
        .with_state(handle)
}

async fn handle_error(error: BoxError) -> impl IntoResponse {
    return (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Something went wrong: {}", error),
    )
        .into_response();
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::prelude::{
        DeleteKVsRequest, GetKVsRequest, GetKVsResponse, KeyValue, PutKVsRequest,
    };
    use axum::response::Response;
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use bonerjams_config::Configuration;
    use datacake;
    use datacake::cluster::{ClusterOptions, ConnectionConfig, DCAwareSelector};
    use datacake_sled::{self, SledStorage};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::net::{SocketAddr, TcpListener};
    use tower::Service; // for `call`
    use tower::ServiceExt; // for `oneshot` and `ready`
    #[tokio::test(flavor = "multi_thread")]
    async fn test_key_value_server() {
        std::env::set_var("RUST_LOG", "debug");
        let _ = tracing_subscriber::fmt::try_init();
        let node_d = age::x25519::Identity::generate();
        let store_1 = SledStorage::open_temporary().unwrap();

        let addr_1 = "127.0.0.1:9000".parse::<SocketAddr>().unwrap();
        let connection_cfg_1 = ConnectionConfig::new(addr_1, addr_1, Vec::<String>::new());

        let cluster_1 = DatacakeCluster::connect(
            node_d,
            connection_cfg_1,
            store_1,
            DCAwareSelector::default(),
            ClusterOptions::default(),
        )
        .await
        .unwrap();
        let app = new_router(&cluster_1, API::default());
        let put_kv_req = {
            let mut entries = HashMap::with_capacity(100);
            let mut key_values = Vec::with_capacity(100);
            for idx in 0..100 {
                key_values.push(KeyValue {
                    key: format!("key-{}", idx),
                    value: format!("muchdata yo {}", idx).as_bytes().to_vec(),
                });
            }
            entries.insert("bigkeyspace".to_string(), key_values.clone());
            entries.insert("yourkeyspace".to_string(), key_values.clone());
            PutKVsRequest { entries }
        };
        let del_kv_req = {
            let mut entries = HashMap::with_capacity(100);
            let mut keys = Vec::with_capacity(100);
            for idx in 0..100 {
                keys.push(format!("key-{}", idx));
            }
            entries.insert("bigkeyspace".to_string(), keys.clone());
            entries.insert("yourkeyspace".to_string(), keys.clone());
            DeleteKVsRequest { entries }
        };
        let get_kv_req = {
            let mut entries = HashMap::with_capacity(100);
            let mut keys = Vec::with_capacity(100);
            for idx in 0..100 {
                keys.push(format!("key-{}", idx));
            }
            entries.insert("bigkeyspace".to_string(), keys.clone());
            entries.insert("yourkeyspace".to_string(), keys.clone());
            GetKVsRequest { entries }
        };

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/put")
                    .method(http::Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&put_kv_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/get")
                    .method(http::Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&get_kv_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let res: GetKVsResponse = serde_json::from_slice(&body).unwrap();
        res.entries.iter().for_each(|(namespace, wrapped_doc)| {
            wrapped_doc.iter().for_each(|doc| {
                let parts = doc.key.split("-").collect::<Vec<_>>();
                assert_eq!(parts.len(), 2);
                let idx = parts[1].parse::<i32>().unwrap();
                let msg = String::from_utf8(doc.data.clone()).unwrap();
                assert_eq!(msg, format!("muchdata yo {}", idx));
            });
        });
        println!("{:#?}", res);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/delete")
                    .method(http::Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&del_kv_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        cluster_1.shutdown().await;
    }
}
