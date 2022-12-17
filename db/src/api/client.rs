use std::collections::HashMap;

use reqwest;

use crate::prelude::{
    ClusterStatistics, GetKVsRequest, GetKVsResponse, KeyValue, PutKVsRequest, Status,
};
use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    http::{self, Request, StatusCode},
};
pub struct KVClient {
    client: reqwest::Client,
    url: String,
}

impl KVClient {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?,
            url: url.to_string(),
        })
    }
    pub async fn put_key_values(
        &self,
        namespaces: &[String],
        kvs: &mut [Vec<KeyValue>],
    ) -> Result<()> {
        let mut entries = HashMap::with_capacity(namespaces.len());
        for (ns, kvs) in namespaces.into_iter().zip(kvs.iter_mut()) {
            entries.insert(ns.clone(), std::mem::take(kvs));
        }
        let response: Status = self
            .client
            .post(format!("{}/put", self.url))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&PutKVsRequest { entries })?))
            .send()
            .await?
            .json()
            .await?;
        if response.msg.eq_ignore_ascii_case("ok") {
            return Ok(());
        } else {
            return Err(anyhow!("failed to send request {}", response.msg));
        }
    }
    pub async fn get_key_values(
        &self,
        namespaces: &[String],
        keys: &mut [Vec<String>],
    ) -> Result<GetKVsResponse> {
        let mut entries = HashMap::with_capacity(namespaces.len());
        for (ns, kvs) in namespaces.into_iter().zip(keys.iter_mut()) {
            entries.insert(ns.clone(), std::mem::take(kvs));
        }
        let response = self
            .client
            .post(format!("{}/get", self.url))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&GetKVsRequest { entries })?))
            .send()
            .await?;
        if response.status().ne(&StatusCode::OK) {
            return Err(anyhow!("invalid status code"));
        }
        let response: GetKVsResponse = response.json().await?;
        Ok(response)
    }
    pub async fn delete_key_values(
        &self,
        namespaces: &[String],
        keys: &mut [Vec<String>],
    ) -> Result<()> {
        let mut entries = HashMap::with_capacity(namespaces.len());
        for (ns, kvs) in namespaces.into_iter().zip(keys.iter_mut()) {
            entries.insert(ns.clone(), std::mem::take(kvs));
        }
        let response = self
            .client
            .post(format!("{}/delete", self.url))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&GetKVsRequest { entries })?))
            .send()
            .await?;
        if response.status().ne(&StatusCode::OK) {
            return Err(anyhow!("invalid status code"));
        }
        let response: Status = response.json().await?;
        if response.msg.eq_ignore_ascii_case("ok") {
            return Ok(());
        } else {
            return Err(anyhow!("failed to send request {}", response.msg));
        }
    }
    pub async fn cluster_stats(&self) -> Result<ClusterStatistics> {
        let response = self
            .client
            .post(format!("{}/cluster/stats", self.url))
            .header(http::header::CONTENT_TYPE, "application/json")
            .send()
            .await?;
        if response.status().ne(&StatusCode::OK) {
            return Err(anyhow!("invalid status code"));
        }
        let response: ClusterStatistics = response.json().await?;
        Ok(response)
    }
}
