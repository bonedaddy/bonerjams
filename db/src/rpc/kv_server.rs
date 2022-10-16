use super::{types::*, tls::SelfSignedCert, string_reader::StringReader};
use crate::{
    types::{DbKey, DbTrees},
    DbBatch, DbTree,
};
use hyper::server::conn::Http;
use config::ConnType;
use tokio_rustls::{rustls::{ServerConfig, PrivateKey}, TlsAcceptor};
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use tokio::net::UnixListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{metadata::MetadataValue, transport::{Server, ServerTlsConfig, Identity, Certificate}, Request, Status};
use tonic_health::ServingStatus;
use tonic::{Response,  Streaming};
use tower_http::ServiceBuilderExt;
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
                self.async_flush(db_tree).await?;
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
            self.apply_and_flush(db_tree, &mut batch).await?;
        }
        Ok(tonic::Response::new(Empty {}))
    }
    async fn delete_kv(
        &self,
        request: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let key = request.into_inner();
        let db_tree = match db.open_tree(crate::types::DbTrees::Default) {
            Ok(tree) => tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        match db_tree.delete(&key[..]) {
            Ok(_) => {
                self.async_flush(db_tree).await?;
                Ok(tonic::Response::new(Empty {}))
            }
            Err(err) => Err(Status::new(tonic::Code::Internal, err.to_string())),
        }
    }
    async fn delete_kvs(
        &self,
        request: tonic::Request<DeleteKVsRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let db = self.db.clone();
        let mut args = request.into_inner();
        for (tree, keys) in args.entries.iter_mut() {
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
            for key in keys.iter_mut() {
                if let Err(err) = batch.remove_raw(key) {
                    return Err(Status::new(tonic::Code::Internal, err.to_string()));
                }
            }
            self.apply_and_flush(db_tree, &mut batch).await?;
        }
        Ok(tonic::Response::new(Empty {}))
    }
    async fn health_check(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<HealthCheck>, tonic::Status> {
        Ok(tonic::Response::new(HealthCheck { ok: true }))
    }
    async fn exist(
        &self,
        key: tonic::Request<Vec<u8>>,
    ) -> Result<tonic::Response<Exists>, tonic::Status> {
        let db_tree = match self.db.open_tree(DbTrees::Default) {
            Ok(db_tree) => db_tree,
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        let exists = match db_tree.contains_key(key.into_inner()) {
            Ok(response) => {
                if response {
                    Exists::Found
                } else {
                    Exists::NotFound
                }
            }
            Err(err) => return Err(Status::new(tonic::Code::Internal, err.to_string())),
        };
        Ok(tonic::Response::new(exists))
    }
    async fn batch_exist(
        &self,
        key: tonic::Request<ExistsKVsRequest>,
    ) -> Result<tonic::Response<ExistKVsResponse>, tonic::Status> {
        let req = key.into_inner();
        let mut response = ExistKVsResponse {
            entries: HashMap::with_capacity(req.entries.len()),
        };
        req.entries.into_iter().for_each(|(tree, keys)| {
            let tree_name = if tree.is_empty() {
                vec![]
            } else {
                match base64::decode(&tree) {
                    Ok(tree_name) => tree_name,
                    Err(_err) => return,
                }
            };
            let db_tree = if tree_name.is_empty() {
                crate::types::DbTrees::Default
            } else {
                crate::types::DbTrees::Binary(&tree_name[..])
            };
            let db_tree = match self.db.open_tree(db_tree) {
                Ok(db) => db,
                Err(_err) => return,
            };
            keys.into_iter().for_each(|key| {
                let contains = if let Ok(contains) = db_tree.contains_key(&key) {
                    contains
                } else {
                    false
                };
                match response.entries.get_mut(&tree) {
                    Some(entries) => {
                        entries.insert(
                            base64::encode(key),
                            if contains {
                                Exists::Found
                            } else {
                                Exists::NotFound
                            },
                        );
                    }
                    None => {
                        let mut res = HashMap::with_capacity(5);
                        res.insert(
                            base64::encode(key),
                            if contains {
                                Exists::Found
                            } else {
                                Exists::NotFound
                            },
                        );
                        response.entries.insert(tree.clone(), res);
                    }
                }
            })
        });
        Ok(tonic::Response::new(response))
    }
}

impl State {
    async fn apply_and_flush(
        &self,
        db_tree: Arc<DbTree>,
        batch: &mut DbBatch,
    ) -> Result<(), tonic::Status> {
        if let Err(err) = db_tree.apply_batch(batch) {
            return Err(Status::new(tonic::Code::Internal, err.to_string()));
        }
        self.async_flush(db_tree).await?;
        Ok(())
    }
    async fn async_flush(&self, db_tree: Arc<DbTree>) -> Result<(), tonic::Status> {
        if let Err(err) = db_tree.flush_async().await {
            return Err(Status::new(tonic::Code::Internal, err.to_string()));
        }
        if let Err(err) = self.db.flush_async().await {
            return Err(Status::new(tonic::Code::Internal, err.to_string()));
        }
        Ok(())
    }
}

/// starts the gRPC server providing RPC access to the underlying sled database
pub async fn start_server(conf: config::Configuration) -> anyhow::Result<()> {
    let state = Arc::new(State {
        db: crate::Database::new(&conf.db)?,
    });
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(HealthCheck { ok: true }, ServingStatus::Serving)
        .await;


    match &conf.rpc.connection {
        ConnType::HTTPS(_, _) => {
            let tls_cert_info: SelfSignedCert = (&conf.rpc).into();
            log::info!("loading certs");
            let identity = Identity::from_pem(tls_cert_info.cert()?, tls_cert_info.key()?);


            let server_builder = Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .accept_http1(true)
            .add_service(health_service);

            let server_builder = if !conf.rpc.auth_token.is_empty() {
                server_builder.add_service(
                    key_value_store_server::KeyValueStoreServer::with_interceptor(state, check_auth),
                )
            } else {
                server_builder.add_service(key_value_store_server::KeyValueStoreServer::new(state))
            };

            let listener = TcpListenerStream::new(TcpListener::bind(&conf.rpc.server_url()).await?);
            Ok(server_builder.serve_with_incoming(listener).await?)
        }
        ConnType::HTTP(_, _) => {
            let server_builder = Server::builder()
            .add_service(health_service);

            let server_builder = if !conf.rpc.auth_token.is_empty() {
                server_builder.add_service(
                    key_value_store_server::KeyValueStoreServer::with_interceptor(state, check_auth),
                )
            } else {
                server_builder.add_service(key_value_store_server::KeyValueStoreServer::new(state))
            };
            let listener = TcpListenerStream::new(TcpListener::bind(&conf.rpc.server_url()).await?);
            Ok(server_builder.serve_with_incoming(listener).await?)
        }
        ConnType::UDS(path) => {
            let server_builder = Server::builder()
            .add_service(health_service);

            let server_builder = if conf.rpc.auth_token.is_empty() {
                server_builder.add_service(
                    key_value_store_server::KeyValueStoreServer::with_interceptor(state, check_auth),
                )
            } else {
                server_builder.add_service(key_value_store_server::KeyValueStoreServer::new(state))
            };
            tokio::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).await?;
            let uds = UnixListener::bind(path)?;
            let uds_stream = UnixListenerStream::new(uds);
            Ok(server_builder.serve_with_incoming(uds_stream).await?)
        }
    }
}

pub fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    let token: MetadataValue<_> = "Bearer some-secret-token".parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}


#[derive(Debug)]
pub struct ConnInfo {
    pub addr: std::net::SocketAddr,
    pub certificates: Vec<tokio_rustls::rustls::Certificate>,
}

#[cfg(test)]
mod test {
    use crate::rpc::{client, tls::SelfSignedCert};
    use crate::rpc::client::BatchPutEntry;
    use crate::rpc::types::KeyValue;
    use config::{database::DbOpts, Configuration, ConnType, RpcHost, RpcPort, RPC};
    use std::collections::HashMap;
    #[tokio::test(flavor = "multi_thread")]
    #[allow(unused_must_use)]
    async fn test_run_server_uds() {
        let conf = Configuration {
            db: DbOpts {
                path: "/tmp/test_server_uds.db".to_string(),
                ..Default::default()
            },
            rpc: RPC {
                auth_token: "Bearer some-secret-token".to_string(),
                connection: ConnType::UDS("/tmp/test_server.ipc".to_string()),
                ..Default::default()
            },
        };
        config::init_log(true);

        run_server(conf).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(unused_must_use)]
    async fn test_run_server_tcp() {
        let conf = Configuration {
            db: DbOpts {
                path: "/tmp/kek2232222.db".to_string(),
                ..Default::default()
            },
            rpc: RPC {
                auth_token: "Bearer some-secret-token".to_string(),
                connection: ConnType::HTTP(
                    "127.0.0.1".to_string() as RpcHost,
                    "8668".to_string() as RpcPort,
                ),
                ..Default::default()
            },
        };
        config::init_log(true);

        run_server(conf).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    #[allow(unused_must_use)]
    async fn test_run_server_tcp_tls() {

        let self_signed = super::super::tls::create_self_signed(
            &["https://localhost:8668".to_string(), "localhost".to_string(), "localhost:8668".to_string()],
            1000,
            false,
        ).unwrap();
        let mut conf = Configuration {
            db: DbOpts {
                path: "/tmp/kek2232222.db".to_string(),
                ..Default::default()
            },
            rpc: RPC {
                auth_token: "Bearer some-secret-token".to_string(),
                connection: ConnType::HTTPS(
                    "localhost".to_string() as RpcHost,
                    "8668".to_string() as RpcPort,
                ),
                tls_cert: self_signed.base64_cert,
                tls_key: self_signed.base64_key,
            },
        };
        config::init_log(true);

        run_server_tls(conf).await;
    }
    // starts the server and runs some basic tests
    async fn run_server_tls(mut conf: config::Configuration) {
        {
            let conf = conf.clone();
            tokio::spawn(async move { super::start_server(conf).await });
        }
        tokio::time::sleep(std::time::Duration::from_secs(
            5
        )).await;

        let client = client::Client::new_tls(&conf,  "Bearer some-secret-token")
            .await
            .unwrap();
        client.ready().await.unwrap();
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
        client
            .batch_exists(vec![
                (vec![], vec!["sixety_nine".as_bytes().to_vec()]),
                (
                    vec![4, 2, 0],
                    vec![
                        "sixety_nine".as_bytes().to_vec(),
                        "foobarbaz".as_bytes().to_vec(),
                    ],
                ),
            ])
            .await
            .unwrap()
            .entries
            .iter()
            .for_each(|exists_tree| println!("{:#?}", exists_tree));
    }
    // starts the server and runs some basic tests
    async fn run_server(conf: config::Configuration) {
        {
            let conf = conf.clone();
            tokio::spawn(async move { super::start_server(conf).await });
        }

        let client = client::Client::new(&conf, "Bearer some-secret-token")
            .await
            .unwrap();
        client.ready().await.unwrap();
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
        client
            .batch_exists(vec![
                (vec![], vec!["sixety_nine".as_bytes().to_vec()]),
                (
                    vec![4, 2, 0],
                    vec![
                        "sixety_nine".as_bytes().to_vec(),
                        "foobarbaz".as_bytes().to_vec(),
                    ],
                ),
            ])
            .await
            .unwrap()
            .entries
            .iter()
            .for_each(|exists_tree| println!("{:#?}", exists_tree));
    }
}
