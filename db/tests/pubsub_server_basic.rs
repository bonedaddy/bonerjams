use bonerjams_db::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::ReceiverStream;

#[tokio::test]
async fn test_bidirectional() {
    let state = PubSubState::default();
    let addr = run_server(pub_sub_server::PubSubServer::new(state)).await;
    let mut client = pub_sub_client::PubSubClient::connect(addr)
        .await
        .expect("Error connecting");
    let (tx, rx) = tokio::sync::mpsc::channel(10);
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