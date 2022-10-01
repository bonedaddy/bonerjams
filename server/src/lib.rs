pub mod client;
pub mod types;
use axum::extract::{ContentLengthLimit, Extension, State};
use axum::routing::post;
use axum::Router;
use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use axum_jrpc::{JrpcResult, JsonRpcExtractor, JsonRpcResponse};
use db::types::DbTrees;
use serde::Deserialize;
use std::sync::Arc;
use tower::ServiceBuilder;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<db::Database>,
}

pub async fn start_rpc_server(conf: config::Configuration) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let router = router(AppState {
        db: db::Database::new(&conf.db)?,
    });

    log::debug!("listening");
    axum::Server::bind(&conf.rpc_endpoint.parse().unwrap())
        .serve(router.into_make_service())
        .await
        .unwrap();
    Ok(())
}

pub fn router(state: AppState) -> Router {
    Router::new().route("/", post(handler)).layer(
        ServiceBuilder::new()
            .layer(Extension(Arc::new(state)))
            .into_inner(),
    )
}

async fn handler(
    Extension(state): Extension<Arc<AppState>>,
    ContentLengthLimit(value): ContentLengthLimit<JsonRpcExtractor, 1024>,
) -> JrpcResult {
    use crate::types::{Empty, KeyValue, RequestGet, RequestPut, ResponseGet};
    let answer_id = value.get_answer_id();
    println!("{:?}", value);
    match value.method.as_str() {
        "get" => {
            let request: RequestGet = value.parse_params()?;
            Ok(JsonRpcResponse::success(
                answer_id,
                ResponseGet {
                    values: request
                        .keys
                        .iter()
                        .filter_map(|key| {
                            if let Ok(Some(value)) = state.db.get(key) {
                                Some(value.to_vec())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                },
            ))
        }
        "put" => {
            let request: RequestPut = value.parse_params()?;
            for entry in request.entries.iter() {
                let tree = if entry.tree.is_empty() {
                    DbTrees::Default
                } else {
                    DbTrees::Custom(entry.tree.as_str())
                };
                match state.db.open_tree(tree) {
                    Ok(db_tree) => {
                        if let Err(err) = db_tree.insert(&entry.data) {
                            log::error!("failed to insert entry {:#?}", err);
                        }
                    }
                    Err(err) => {
                        return Err(JsonRpcResponse::error(
                            answer_id,
                            CustomError::Unknown(err.to_string()).into(),
                        ))
                    }
                }
            }
            Ok(JsonRpcResponse::success(answer_id, Empty {}))
        }
        "newTree" => {
            // tree name
            let result: String = value.parse_params()?;
            if let Err(err) = state.db.open_tree(DbTrees::Custom(result.as_str())) {
                return Err(JsonRpcResponse::error(
                    answer_id,
                    CustomError::Unknown(err.to_string()).into(),
                ));
            }
            Ok(JsonRpcResponse::success(answer_id, Empty {}))
        }
        method => Ok(value.method_not_found(method)),
    }
}

#[derive(Debug, thiserror::Error)]
enum CustomError {
    #[error("{0}")]
    Unknown(String),
}

impl From<CustomError> for JsonRpcError {
    fn from(error: CustomError) -> Self {
        JsonRpcError::new(
            JsonRpcErrorReason::ServerError(-32099),
            error.to_string(),
            serde_json::Value::Null,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        client::{Client, RpcRequest},
        types::{Entry, RequestPut},
    };
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use reqwest::header::CONTENT_TYPE;
    use serde_json::{json, Value};
    use std::net::{SocketAddr, TcpListener};
    use tower::util::ServiceExt;
    // You can also spawn a server and talk to it like any other HTTP server:
    #[tokio::test]
    async fn test_basic_server() {
        let conf = config::Configuration {
            db: config::database::DbOpts {
                path: "/tmp/bonerjams.db".to_string(),
                ..Default::default()
            },
            rpc_endpoint: "0.0.0.0:42696".to_string(),
        };
        {
            let conf = conf.clone();
            tokio::spawn(async move {
                let _ = start_rpc_server(conf).await;
            });
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let client = Client::new("http://127.0.0.1:42696");
        client
            .async_send(
                RpcRequest::Put,
                serde_json::json!([Entry {
                    tree: "".to_string(),
                    data: types::KeyValue {
                        key: "foo_bar".as_bytes().to_vec(),
                        value: "baz".as_bytes().to_vec()
                    }
                }]),
            )
            .await;
    }
}
