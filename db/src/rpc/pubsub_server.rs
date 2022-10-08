use super::types::*;
use crate::{
    types::{DbKey, DbTrees},
    DbBatch, DbTree,
};
use config::ConnType;
use std::{collections::HashMap, sync::Arc};
use tokio::net::UnixListener;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{metadata::MetadataValue, transport::Server, Request, Status};
use tonic_health::ServingStatus;
#[tonic::async_trait]
impl pub_sub_server::PubSub for PubSubState {
    type SubStream = ReceiverStream<super::types::Update>;

    async fn sub(
        &self,
        channels: tonic::Request<tonic::Streaming<String>>,
    ) -> Result<tonic::Response<Self::SubStream>, Status> {
        let mut channels = channels.into_inner();
        let (tx, rx) = mpsc::channel(20);
        let subscribers = Arc::clone(&self.subscribers);
        let data = Arc::clone(&self.data);
        tokio::spawn(async move {
            while let Some(channel) = channels.message().await.unwrap() {
                let existing_data = data.lock().unwrap().get(&channel).cloned();
                match existing_data {
                    None => {}
                    Some(value) => {
                        tx.send(Ok((channel.clone(), value))).await.unwrap();
                    }
                }
                let mut subscribers = subscribers.lock().unwrap();
                subscribers
                    .entry(channel)
                    .or_insert(vec![])
                    .push(tx.clone());
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn publish(
        &self,
        kvp: tonic::Request<(String, String)>,
    ) -> Result<tonic::Response<()>, Status> {
        let (key, value) = kvp.into_inner();
        self.data.lock().unwrap().insert(key.clone(), value.clone());
        let to_send = {
            let subscribers = self.subscribers.lock().unwrap();
            subscribers.get(&key).unwrap_or(&vec![]).clone()
        };
        for subscriber in to_send {
            subscriber
                .send(Ok((key.clone(), value.clone())))
                .await
                .unwrap();
        }
        Ok(tonic::Response::new(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Mutex};
    #[tokio::test]
    async fn test_bidirectional() {
        let state = PubSubState {
            data: Arc::new(Mutex::new(HashMap::new())),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        };
        let addr = run_server(pub_sub_server::PubSubServer::new(state)).await;
        let mut client = pub_sub_client::PubSubClient::connect(addr)
            .await
            .expect("Error connecting");
        let (tx, rx) = mpsc::channel(10);
        let mut updates = client
            .sub(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        tx.send("foo".to_string()).await.unwrap();
        client
            .publish(("foo".to_string(), "fooval".to_string()))
            .await
            .unwrap();
        client
            .publish(("bar".to_string(), "barval".to_string()))
            .await
            .unwrap();
        assert_eq!(
            ("foo".to_string(), "fooval".to_string()),
            updates.message().await.unwrap().unwrap()
        );
        tx.send("bar".to_string()).await.unwrap();
        assert_eq!(
            ("bar".to_string(), "barval".to_string()),
            updates.message().await.unwrap().unwrap()
        );
        client
            .publish(("foo".to_string(), "fooval2".to_string()))
            .await
            .unwrap();
        assert_eq!(
            ("foo".to_string(), "fooval2".to_string()),
            updates.message().await.unwrap().unwrap()
        );
    }

    use std::convert::Infallible;

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{
        body::BoxBody,
        codegen::{
            http::{Request, Response},
            Service,
        },
        transport::{Body, NamedService, Server},
    };
    /// Returns the address to connect to.
    pub async fn run_server<S>(svc: S) -> String
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        format!("http://127.0.0.1:{}", port)
    }
}
