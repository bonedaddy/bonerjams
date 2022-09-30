pub mod types;
use axum::extract::{ContentLengthLimit, Extension, State};
use axum::routing::post;
use axum::Router;
use axum_jrpc::{JrpcResult, JsonRpcExtractor, JsonRpcResponse};
use db::types::DbTrees;
use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use serde::Deserialize;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use std::sync::Arc;

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

    let router = Router::new()
    .route("/", post(handler))
    .layer(Extension(Arc::new(AppState {
        db: db::Database::new(&conf.db)?,
    })));

    log::debug!("listening");
    axum::Server::bind(&conf.rpc_endpoint.parse().unwrap())
        .serve(router.into_make_service())
        .await
        .unwrap();
    Ok(())
}

async fn handler(
    Extension(state): Extension<Arc<AppState>>,
    ContentLengthLimit(value): ContentLengthLimit<JsonRpcExtractor, 1024>,
) -> JrpcResult {
    use crate::types::{RequestGet, ResponseGet, RequestPut, KeyValue, Empty};
    let answer_id = value.get_answer_id();
    println!("{:?}", value);
    match value.method.as_str() {
        "get" => {
            let request: RequestGet = value.parse_params()?;
            Ok(JsonRpcResponse::success(answer_id, ResponseGet { values: request.keys.iter().filter_map(|key| {
                if let Ok(Some(value)) = state.db.get(key) {
                    Some(value.to_vec())
                } else {
                    None
                }
            }).collect::<Vec<_>>()} ))
        }
        "put" => {
            let request: RequestPut = value.parse_params()?;
            for entry in request.entries.iter() {
                match state.db.open_tree(DbTrees::Custom(entry.tree.as_str())) {
                    Ok(db_tree) => {
                        if let Err(err) = db_tree.insert(&entry.data) {
                            log::error!("failed to insert entry {:#?}", err);
                        }
                    }
                    Err(err) => {

                    }
                }
            }
            Ok(JsonRpcResponse::success(answer_id, Empty{}))
        }
        "newTree" => {
            let result: [i32; 2] = value.parse_params()?;
            let result = match failing_div(result[0], result[1]).await {
                Ok(result) => result,
                Err(e) => return Err(JsonRpcResponse::error(answer_id, e.into())),
            };

            Ok(JsonRpcResponse::success(answer_id, result))
        }
        method => Ok(value.method_not_found(method)),
    }
}

async fn failing_sub(a: i32, b: i32) -> anyhow::Result<i32> {
    anyhow::ensure!(a > b, "a must be greater than b");
    Ok(a - b)
}

async fn failing_div(a: i32, b: i32) -> Result<i32, CustomError> {
    if b == 0 {
        Err(CustomError::DivideByZero)
    } else {
        Ok(a / b)
    }
}

#[derive(Deserialize, Debug)]
struct Test {
    a: i32,
    b: i32,
}

#[derive(Debug, thiserror::Error)]
enum CustomError {
    #[error("Divisor must not be equal to 0")]
    DivideByZero,
    #[error("{0}")]
    Unknown(String)
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
