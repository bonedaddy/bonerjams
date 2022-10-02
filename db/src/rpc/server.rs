use super::types::*;
use crate::{types::DbKey, DbBatch};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, Server},
    Request, Status,
};
use tonic_health::{server::HealthReporter, ServingStatus};

#[tonic::async_trait]
impl key_value_store_server::KeyValueStore for Arc<State> {
    async fn get_kv(
        &self,
        request: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Vec<u8>>, tonic::Status> {
        let db = self.db.clone();
        let arg = request.into_inner();
        match db.get(arg) {
            Ok(Some(key)) => Ok(tonic::Response::new(key.to_vec())),
            Ok(None) => Err(Status::new(tonic::Code::NotFound, "")),
            Err(err) => Err(Status::new(tonic::Code::Internal, err.to_string())),
        }
    }
    async fn list(
        &self,
        request: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Values>, tonic::Status> {
        let db = self.db.clone();
        let arg = request.into_inner();
        let tree = if arg.is_empty() {
            crate::types::DbTrees::Default
        } else {
            crate::types::DbTrees::Binary(&arg[..])
        };
        let db_tree = match db.open_tree(tree) {
            Ok(tree) => tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        let values: Values = db_tree
            .iter()
            .collect::<Vec<_>>()
            .iter()
            .flatten()
            .map(|(key, value)| KeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .collect();
        Ok(tonic::Response::new(values))
    }
    async fn put_kv(
        &self,
        key: tonic::Request<(Vec<u8>, Vec<u8>)>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let (key, value) = key.into_inner();
        let db_tree = match db.open_tree(crate::types::DbTrees::Default) {
            Ok(tree) => tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        match db_tree.insert_raw(&key[..], &value[..]) {
            Ok(_) => {
                if let Err(err) = db_tree.flush_async().await {
                    return Err(Status::new(tonic::Code::Internal, err.to_string()));
                }
                if let Err(err) = db.flush_async().await {
                    return Err(Status::new(tonic::Code::Internal, err.to_string()));
                }
                Ok(tonic::Response::new(Empty {}))
            }
            Err(err) => Err(Status::new(tonic::Code::Internal, err.to_string())),
        }
    }
    async fn put_kvs(
        &self,
        request: tonic::Request<PutKVsRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let mut args = request.into_inner();
        for (tree, values) in args.entries.iter_mut() {
            let tree_name = if tree.is_empty() {
                vec![]
            } else {
                match base64::decode(tree) {
                    Ok(tree_name) => tree_name,
                    Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
                }
            };
            let tree = if tree_name.is_empty() {
                crate::types::DbTrees::Default
            } else {
                crate::types::DbTrees::Binary(&tree_name[..])
            };
            let db_tree = match db.open_tree(tree) {
                Ok(tree) => tree,
                Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
            };
            let mut batch = DbBatch::new();
            for value in values.iter_mut() {
                if let Err(err) = batch.insert_raw(&value.key[..], &value.value[..]) {
                    return Err(Status::new(tonic::Code::Internal, err.to_string()));
                }
            }
            if let Err(err) = db_tree.apply_batch(&mut batch) {
                return Err(Status::new(tonic::Code::Internal, err.to_string()));
            }
            if let Err(err) = db_tree.flush_async().await {
                return Err(Status::new(tonic::Code::Internal, err.to_string()));
            }
            if let Err(err) = db.flush_async().await {
                return Err(Status::new(tonic::Code::Internal, err.to_string()));
            }
        }
        Ok(tonic::Response::new(Empty {}))
    }
    async fn health_check(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<HealthCheck>, tonic::Status> {
        Ok(tonic::Response::new(HealthCheck { ok: true }))
    }
}
pub async fn start_server(conf: config::Configuration) -> anyhow::Result<()> {
    let listener = TcpListenerStream::new(TcpListener::bind(&conf.rpc.server_url()).await?);
    let state = Arc::new(State {
        db: crate::Database::new(&conf.db)?,
    });
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(HealthCheck { ok: true }, ServingStatus::Serving)
        .await;

    let server_builder = Server::builder().add_service(health_service);

    let server_builder = if !conf.rpc.auth_token.is_empty() {
        server_builder.add_service(
            key_value_store_server::KeyValueStoreServer::with_interceptor(state, check_auth),
        )
    } else {
        server_builder.add_service(key_value_store_server::KeyValueStoreServer::new(state))
    };
    Ok(server_builder.serve_with_incoming(listener).await?)
}

pub fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    let token: MetadataValue<_> = "Bearer some-secret-token".parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}
#[cfg(test)]
mod test {
    use crate::rpc::client;
    use crate::rpc::client::BatchPutEntry;
    use crate::rpc::types::KeyValue;
    use config::{database::DbOpts, Configuration};
    use std::collections::HashMap;
    #[tokio::test(flavor = "multi_thread")]
    #[allow(unused_must_use)]
    async fn test_run_server() {
        let conf = Configuration {
            debug_log: false,
            db: DbOpts {
                path: "/tmp/kek2232222.db".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        conf.init_log();

        run_server(conf).await;
    }

    // starts the server and runs some basic tests
    async fn run_server(conf: config::Configuration) {
        tokio::spawn(async move { super::start_server(conf).await });
        let client = client::Client::new("http://127.0.0.1:8668", "Bearer some-secret-token")
            .await
            .unwrap();
        client
            .put(
                "four twenty blaze".as_bytes(),
                "sixty nine gigity".as_bytes(),
            )
            .await
            .unwrap();
        client.put("1".as_bytes(), "2".as_bytes()).await.unwrap();
        client.put("3".as_bytes(), "4".as_bytes()).await.unwrap();
        let response = client.get("four twenty blaze".as_bytes()).await.unwrap();
        println!("key 'four twenty blaze', value {:?}", unsafe {
            String::from_utf8_unchecked(response)
        });

        let mut entries = HashMap::new();
        entries.insert(
            "".to_string(),
            vec![KeyValue {
                key: "sixety_nine".as_bytes().to_vec(),
                value: "l33tm0d3".as_bytes().to_vec(),
            }],
        );
        entries.insert(
            base64::encode(vec![4, 2, 0]),
            vec![KeyValue {
                key: "sixety_nine".as_bytes().to_vec(),
                value: "l33tm0d3".as_bytes().to_vec(),
            }],
        );
        client
            .batch_put(vec![
                (
                    vec![],
                    vec![BatchPutEntry {
                        key: "sixety_nine".as_bytes().to_vec(),
                        value: "l33tm0d3".as_bytes().to_vec(),
                    }],
                ),
                (
                    vec![4, 2, 0],
                    vec![BatchPutEntry {
                        key: "sixety_nine".as_bytes().to_vec(),
                        value: "l33tm0d3".as_bytes().to_vec(),
                    }],
                ),
            ])
            .await
            .unwrap();
        let response = client.get("four twenty blaze".as_bytes()).await.unwrap();
        println!("response {}", unsafe {
            String::from_utf8_unchecked(response)
        });
        client
            .list(&[])
            .await
            .unwrap()
            .iter()
            .for_each(|key_value| {
                println!(
                    "key {}, value {}",
                    unsafe { String::from_utf8_unchecked(key_value.key.clone()) },
                    unsafe { String::from_utf8_unchecked(key_value.value.clone()) }
                )
            });
    }
}
