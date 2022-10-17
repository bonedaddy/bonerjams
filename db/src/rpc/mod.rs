//! exposes access to the sled database via gRPC
pub mod client;
pub mod kv_server;
pub mod pubsub_server;
pub mod self_signed_cert;
pub mod string_reader;
pub mod tonic_openssl;
pub mod types;

use crate::rpc::types::HealthCheck;
use crate::types::DbKey;
use config::ConnType;
use self_signed_cert::SelfSignedCert;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::UnixListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{
    metadata::MetadataValue,
    transport::{Identity, Server, ServerTlsConfig},
    Request, Status,
};
use tonic_health::ServingStatus;
use types::State;
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
            let identity = Identity::from_pem(tls_cert_info.cert()?, tls_cert_info.key()?);

            let server_builder = Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))?
                .add_service(health_service);

            let server_builder = if !conf.rpc.auth_token.is_empty() {
                let auth_token = Arc::new(conf.rpc.auth_token.clone());
                server_builder.add_service(
                    types::key_value_store_server::KeyValueStoreServer::with_interceptor(
                        state,
                        move |req: Request<()>| -> Result<Request<()>, Status> {
                            let token: MetadataValue<_> = auth_token.parse().unwrap();
                            match req.metadata().get("authorization") {
                                Some(t) if token == t => Ok(req),
                                _ => Err(Status::unauthenticated("No valid auth token")),
                            }
                        },
                    ),
                )
            } else {
                server_builder.add_service(types::key_value_store_server::KeyValueStoreServer::new(
                    state,
                ))
            };

            let listener = TcpListenerStream::new(TcpListener::bind(&conf.rpc.server_url()).await?);
            Ok(server_builder.serve_with_incoming(listener).await?)
        }
        ConnType::HTTP(_, _) => {
            let server_builder = Server::builder().add_service(health_service);

            let server_builder = if !conf.rpc.auth_token.is_empty() {
                let auth_token = Arc::new(conf.rpc.auth_token.clone());
                server_builder.add_service(
                    types::key_value_store_server::KeyValueStoreServer::with_interceptor(
                        state,
                        move |req: Request<()>| -> Result<Request<()>, Status> {
                            let token: MetadataValue<_> = auth_token.parse().unwrap();

                            match req.metadata().get("authorization") {
                                Some(t) if token == t => Ok(req),
                                _ => Err(Status::unauthenticated("No valid auth token")),
                            }
                        },
                    ),
                )
            } else {
                server_builder.add_service(types::key_value_store_server::KeyValueStoreServer::new(
                    state,
                ))
            };
            let listener = TcpListenerStream::new(TcpListener::bind(&conf.rpc.server_url()).await?);
            Ok(server_builder.serve_with_incoming(listener).await?)
        }
        ConnType::UDS(path) => {
            let server_builder = Server::builder().add_service(health_service);

            let server_builder = if conf.rpc.auth_token.is_empty() {
                let auth_token = Arc::new(conf.rpc.auth_token.clone());
                server_builder.add_service(
                    types::key_value_store_server::KeyValueStoreServer::with_interceptor(
                        state,
                        move |req: Request<()>| -> Result<Request<()>, Status> {
                            let token: MetadataValue<_> = auth_token.parse().unwrap();
                            match req.metadata().get("authorization") {
                                Some(t) if token == t => Ok(req),
                                _ => Err(Status::unauthenticated("No valid auth token")),
                            }
                        },
                    ),
                )
            } else {
                server_builder.add_service(types::key_value_store_server::KeyValueStoreServer::new(
                    state,
                ))
            };
            tokio::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).await?;
            let uds = UnixListener::bind(path)?;
            let uds_stream = UnixListenerStream::new(uds);
            Ok(server_builder.serve_with_incoming(uds_stream).await?)
        }
    }
}
