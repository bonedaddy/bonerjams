use std::sync::Arc;
use tonic::transport::Channel;

use crate::types::*;
use anyhow::Result;

pub struct BatchPutEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone)]
struct InnerClient {
    kc: key_value_store_client::KeyValueStoreClient<Channel>,
}

pub struct Client {
    inner: InnerClient,
}

impl Client {
    pub async fn new(url: &str) -> Result<Arc<Client>> {
        let client = Arc::new(Client {
            inner: InnerClient {
                kc: key_value_store_client::KeyValueStoreClient::connect(url.to_string()).await?,
            },
        });
        client.ready().await?;
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
}

impl InnerClient {
    /// args maps trees -> keyvalues
    async fn batch_put(&mut self, args: Vec<(Vec<u8>, Vec<BatchPutEntry>)>) -> Result<()> {
        self.kc
            .put_kvs(PutKVsRequest {
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
            })
            .await?;
        Ok(())
    }
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let _ = self.kc.put_kv((key.to_vec(), value.to_vec())).await?;
        Ok(())
    }
    async fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        Ok(self.kc.get_kv(key.to_vec()).await?.into_inner())
    }
    async fn list(&mut self, tree: &[u8]) -> Result<Values> {
        Ok(self.kc.list(tree.to_vec()).await?.into_inner())
    }
    /// blocks until the client, and rpc server is ready
    async fn ready(&mut self) -> Result<()> {
        loop {
            match self.kc.health_check(()).await {
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
            tokio::time::sleep(std::time::Duration::from_millis(250)).await
        }
    }
}
