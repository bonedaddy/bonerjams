use super::tonic_openssl::CustomChannel;


use super::{types::*};
use anyhow::{anyhow, Result};
use config::RPC;
use hyper::{
    header::{HeaderValue},
};
use hyper::{Uri};




use std::{str::FromStr, sync::Arc};

use tonic::body::BoxBody;


use tower::Service;
use tower::ServiceBuilder;
use tower_http::{
    set_header::SetRequestHeaderLayer,
};
pub struct BatchPutEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone)]
struct InnerClient {
    rpc_conf: RPC,
    auth_token: String,
    kc: CustomChannel,
}

pub struct Client {
    conf: config::Configuration,
    inner: InnerClient,
}

impl Client {
    pub async fn new(
        conf: &config::Configuration,
        auth_token: &str,
        tls: bool,
    ) -> Result<Arc<Client>> {
        let channel = CustomChannel::new(tls, Uri::from_str(&conf.rpc.client_url())?, auth_token)
            .await
            .unwrap();
        let client = Arc::new(Client {
            inner: InnerClient {
                auth_token: auth_token.to_string(),
                kc: channel,
                rpc_conf: conf.rpc.clone(),
            },
            conf: conf.clone(),
        });
        Ok(client)
    }
    pub async fn batch_put(
        self: &Arc<Self>,
        args: Vec<(Vec<u8>, Vec<BatchPutEntry>)>,
    ) -> Result<()> {
        let mut inner = self.inner.clone();
        inner.batch_put(args).await
    }
    pub async fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        let mut inner = self.inner.clone();
        inner.put(key, value).await
    }
    pub async fn get(self: &Arc<Self>, key: &[u8]) -> Result<Vec<u8>> {
        let mut inner = self.inner.clone();
        inner.get(key).await
    }
    pub async fn list(self: &Arc<Self>, tree: &[u8]) -> Result<Values> {
        let mut inner = self.inner.clone();
        inner.list(tree).await
    }
    /// blocks until the client, and rpc server is ready
    pub async fn ready(self: &Arc<Self>) -> Result<()> {
        let mut inner = self.inner.clone();
        inner.ready().await
    }
    pub async fn exists(self: &Arc<Self>, key: &[u8]) -> Result<bool> {
        let mut inner = self.inner.clone();
        inner.exists(key).await
    }
    pub async fn batch_exists(
        self: &Arc<Self>,
        args: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
    ) -> Result<ExistKVsResponse> {
        let mut inner = self.inner.clone();
        inner.batch_exists(args).await
    }
}

impl InnerClient {
    async fn authenticated_client(
        &mut self,
    ) -> Result<
        key_value_store_client::KeyValueStoreClient<
            impl Service<
                    hyper::Request<BoxBody>,
                    Response = hyper::Response<
                        impl hyper::body::HttpBody<
                            Data = hyper::body::Bytes,
                            Error = impl Into<tower_http::BoxError>,
                        >,
                    >,
                    Error = impl Into<tower_http::BoxError>,
                > + Clone
                + Send
                + Sync
                + 'static,
        >,
        tonic::transport::Error,
    > {
        Ok(key_value_store_client::KeyValueStoreClient::new(
            ServiceBuilder::new()
                // Set a `User-Agent` header
                .layer(SetRequestHeaderLayer::overriding(
                    http::header::HeaderName::from_bytes("authorization".as_bytes()).unwrap(),
                    HeaderValue::from_str(&self.auth_token).unwrap(),
                ))
                .service(self.kc.clone()),
        ))
    }
    fn client(&mut self) -> key_value_store_client::KeyValueStoreClient<CustomChannel> {
        key_value_store_client::KeyValueStoreClient::new(self.kc.clone())
    }
    /// args maps trees -> keyvalues
    async fn batch_put(&mut self, args: Vec<(Vec<u8>, Vec<BatchPutEntry>)>) -> Result<()> {
        let req = PutKVsRequest {
            entries: args
                .into_iter()
                .map(|(tree, key_values)| {
                    let tree = base64::encode(tree);
                    (
                        tree,
                        key_values
                            .into_iter()
                            .map(|key_value| KeyValue {
                                key: key_value.key,
                                value: key_value.value,
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect(),
        };
        if self.auth_token.is_empty() {
            self.client().put_kvs(req).await?;
        } else {
            self.authenticated_client().await?.put_kvs(req).await?;
        }

        Ok(())
    }
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.auth_token.is_empty() {
            self.client().put_kv((key.to_vec(), value.to_vec())).await?;
        } else {
            self.authenticated_client()
                .await?
                .put_kv((key.to_vec(), value.to_vec()))
                .await?;
        }
        Ok(())
    }
    async fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        if self.auth_token.is_empty() {
            Ok(self.client().get_kv(key.to_vec()).await?.into_inner())
        } else {
            Ok(self
                .authenticated_client()
                .await?
                .get_kv(key.to_vec())
                .await?
                .into_inner())
        }
    }
    async fn list(&mut self, tree: &[u8]) -> Result<Values> {
        if self.auth_token.is_empty() {
            Ok(self.client().list(tree.to_vec()).await?.into_inner())
        } else {
            Ok(self
                .authenticated_client()
                .await?
                .list(tree.to_vec())
                .await?
                .into_inner())
        }
    }
    /// blocks until the client, and rpc server is ready
    async fn ready(&mut self) -> Result<()> {
        loop {
            if self.auth_token.is_empty() {
                match self.client().health_check(()).await {
                    Ok(ok) => {
                        if !ok.into_inner().ok {
                            log::warn!("health check not ok, waiting...");
                        } else {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        log::warn!("client not ready, waiting... {:#?}", err);
                    }
                }
            } else {
                match self.authenticated_client().await?.health_check(()).await {
                    Ok(ok) => {
                        if !ok.into_inner().ok {
                            log::warn!("health check not ok, waiting...");
                        } else {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        log::warn!("client not ready, waiting... {:#?}", err);
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await
        }
    }
    pub async fn exists(&mut self, key: &[u8]) -> Result<bool> {
        if self.auth_token.is_empty() {
            match self.client().exist(key.to_vec()).await {
                Ok(exists) => {
                    let inner = exists.into_inner();
                    Ok(inner.into())
                }
                Err(err) => Err(anyhow::anyhow!("{:#?}", err)),
            }
        } else {
            match self.authenticated_client().await?.exist(key.to_vec()).await {
                Ok(exists) => {
                    let inner = exists.into_inner();
                    Ok(inner.into())
                }
                Err(err) => Err(anyhow::anyhow!("{:#?}", err)),
            }
        }
    }
    pub async fn batch_exists(
        &mut self,
        args: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
    ) -> Result<ExistKVsResponse> {
        let req = ExistsKVsRequest {
            entries: args
                .into_iter()
                .map(|(tree, keys)| {
                    let tree = base64::encode(tree);
                    (tree, keys)
                })
                .collect(),
        };
        if self.auth_token.is_empty() {
            match self.client().batch_exist(req).await {
                Ok(res) => {
                    let inner = res.into_inner();
                    Ok(inner)
                }
                Err(err) => Err(anyhow!("{:#?}", err)),
            }
        } else {
            match self.authenticated_client().await?.batch_exist(req).await {
                Ok(res) => {
                    let inner = res.into_inner();
                    Ok(inner)
                }
                Err(err) => Err(anyhow!("{:#?}", err)),
            }
        }
    }
}
