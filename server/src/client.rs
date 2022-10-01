use std::sync::atomic::Ordering;

use serde_json::{Value, json};
use reqwest::header::CONTENT_TYPE;

pub struct Client {
    pub url: String,
    pub request_id: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

pub enum RpcRequest {
    Get,
    Put,
    NewTree,
}


impl Client {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            request_id: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0))
        }
    }
    pub fn send(&self, request: RpcRequest, params: Value) {
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        let json_request = request.build_request_json(id, params);
        let json_response = reqwest::blocking::Client::new()
        .post(&self.url)
        .header(CONTENT_TYPE, "application/json")
        .body(json_request.to_string())
        .send().unwrap();
        println!("response {:#?}", json_response);
    }
    pub async fn async_send(&self, request: RpcRequest, params: Value) {
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        let json_request = request.build_request_json(id, params);
        let json_response = reqwest::Client::new()
        .post(&self.url)
        .header(CONTENT_TYPE, "application/json")
        .body(json_request.to_string())
        .send().await.unwrap();
        println!("response {:#?}", json_response);
    }
    pub fn new_request(&self, request: RpcRequest, params: Value) -> Value {
        request.build_request_json(
            self.request_id.fetch_add(1, Ordering::SeqCst),
            params
        )
    }
}

impl RpcRequest {
    pub(crate) fn build_request_json(self, id: u64, params: Value) -> Value {
        let jsonrpc = "2.0";
        json!({
           "jsonrpc": jsonrpc,
           "id": id,
           "method": format!("{}", self.to_string()),
           "params": params,
        })
    }
}


impl ToString for RpcRequest {
    fn to_string(&self) -> String {
        match self {
            Self::Get => "get".to_string(),
            Self::Put => "put".to_string(),
            Self::NewTree => "newTree".to_string(),
        }
    }
}