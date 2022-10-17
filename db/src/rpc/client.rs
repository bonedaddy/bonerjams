use super::{types::*, tls::SelfSignedCert, string_reader::StringReader};
use openssl::ssl::SslConnector;
use openssl::ssl::SslMethod;
use anyhow::{anyhow, Result};
use config::RPC;
use std::{sync::Arc, str::FromStr};
use tonic::{codegen::InterceptedService, transport::{Channel, ClientTlsConfig, Identity, Certificate}, Request, Status};
use tower::ServiceBuilder;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use hyper::{client::connect::HttpConnector, Uri};
use hyper_openssl::HttpsConnector;
use openssl::{
    x509::X509,
};
use self::auth_service::AuthSvc;
use self::tls_service::MyChannel;
use self::tls_service::MyClient;
use tonic_openssl::ALPN_H2_WIRE;
pub struct BatchPutEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone)]
struct InnerClient {
    rpc_conf: RPC,
    auth_token: String,
    kc: MyChannel,
}

pub struct Client {
    conf: config::Configuration,
    inner: InnerClient,
}

impl Client {
    pub async fn new(conf: &config::Configuration, auth_token: &str) -> Result<Arc<Client>> {
        let channel = tls_service::MyChannel::new(false, Uri::from_str(&conf.rpc.client_url())?).await.unwrap();
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
    pub async fn new_tls(conf: &config::Configuration, auth_token: &str) -> Result<Arc<Client>> {
        let channel = tls_service::MyChannel::new(true, Uri::from_str(&conf.rpc.client_url())?).await.unwrap();
        let client = Arc::new(Client {
            inner: InnerClient {
                auth_token: auth_token.to_string(),
                kc: channel,
                rpc_conf: conf.rpc.clone()
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
    ) -> key_value_store_client::KeyValueStoreClient<
        InterceptedService<AuthSvc, fn(req: Request<()>) -> Result<Request<()>, Status>>,
    > {
        key_value_store_client::KeyValueStoreClient::new(
            ServiceBuilder::new()
                .layer(tonic::service::interceptor(
                    intercept as fn(req: Request<()>) -> Result<Request<()>, Status>,
                ))
                .layer_fn(auth_service::AuthSvc::new)
                .service(self.kc.clone())
        )
    }
    fn client(&mut self) -> key_value_store_client::KeyValueStoreClient<MyChannel> {
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
            self.authenticated_client().await.put_kvs(req).await?;
        }

        Ok(())
    }
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.auth_token.is_empty() {
            self.client().put_kv((key.to_vec(),value.to_vec())).await?;
        } else {
            self.authenticated_client().await.put_kv((key.to_vec(),value.to_vec())).await?;
        }
        Ok(())
    }
    async fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        if self.auth_token.is_empty() {
            Ok(self.client().get_kv(key.to_vec()).await?.into_inner())
        } else {
            Ok(self.authenticated_client().await.get_kv(key.to_vec()).await?.into_inner())
        }
    }
    async fn list(&mut self, tree: &[u8]) -> Result<Values> {
        if self.auth_token.is_empty() {
            Ok(self.client().list(tree.to_vec()).await?.into_inner())
        } else {
            Ok(self.authenticated_client().await.list(tree.to_vec()).await?.into_inner())
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
                match self.authenticated_client().await.health_check(()).await {
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
            match self.authenticated_client().await.exist(key.to_vec()).await {
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
            match self.authenticated_client().await.batch_exist(req).await {
                Ok(res) => {
                    let inner = res.into_inner();
                    Ok(inner)
                }
                Err(err) => Err(anyhow!("{:#?}", err)),
            }
        }
    }
}
// An interceptor function.
fn intercept(mut req: Request<()>) -> Result<Request<()>, Status> {
    req.metadata_mut()
        .insert("authorization", "Bearer some-secret-token".parse().unwrap());
    println!("received {:?}", req);
    Ok(req)
}

mod auth_service {
    use http::{Request, Response};
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tonic::body::BoxBody;
    use tonic::transport::Body;
    use tonic::transport::Channel;
    use tower::Service;

    use super::tls_service::MyChannel;

    pub struct AuthSvc {
        inner: MyChannel ,
    }

    impl AuthSvc {
        pub fn new(inner: MyChannel) -> Self {
            AuthSvc { inner }
        }
    }

    impl Service<Request<BoxBody>> for AuthSvc {
        type Response = Response<Body>;
        type Error = Box<dyn std::error::Error + Send + Sync>;
        #[allow(clippy::type_complexity)]
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx).map_err(Into::into)
        }

        fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
            // This is necessary because tonic internally uses `tower::buffer::Buffer`.
            // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
            // for details on why this is necessary
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);

            Box::pin(async move {
                // Do extra async work here...
                let response = inner.call(req).await?;

                Ok(response)
            })
        }
    }
}


mod tls_service {
    use http::{Request, Response};
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tonic::body::BoxBody;
    use tonic::transport::Body;
    use tonic::transport::Channel;
    use tower::Service;
    use hyper::{
        client::{HttpConnector, ResponseFuture},
        Client, Uri,
    };
    use hyper_openssl::HttpsConnector;
    use openssl::{
        ssl::{SslConnector, SslMethod},
        x509::X509,
    };
    use std::{error::Error};
    use tonic_openssl::ALPN_H2_WIRE;
    


    #[derive(Clone)]
    pub struct MyChannel {
        pub uri: Uri,
        pub client: MyClient,
    }
    
    #[derive(Clone)]
    pub enum MyClient {
        ClearText(Client<HttpConnector, BoxBody>),
        Tls(Client<HttpsConnector<HttpConnector>, BoxBody>),
    }
    
    impl MyChannel {
        pub async fn new(tls: bool, uri: Uri) -> Result<Self, Box<dyn Error>> {
            let mut http = HttpConnector::new();
            http.enforce_http(false);
            let client = match tls {
                false => MyClient::ClearText(Client::builder().http2_only(true).build(http)),
                true => {
                    let mut connector = SslConnector::builder(SslMethod::tls())?;
                    connector.set_alpn_protos(ALPN_H2_WIRE)?;
                    let mut https = HttpsConnector::with_connector(http, connector)?;
                    https.set_callback(|c, _| {
                        c.set_verify_hostname(false);
                        c.set_verify(openssl::ssl::SslVerifyMode::NONE);
                        Ok(())
                    });
                    MyClient::Tls(Client::builder().http2_only(true).build(https))
                }
            };
    
            Ok(Self { client, uri })
        }
    }
    

    // Check out this blog post for an introduction to Tower:
    // https://tokio.rs/blog/2021-05-14-inventing-the-service-trait
    impl Service<Request<BoxBody>> for MyChannel {
        type Response = Response<Body>;
        type Error = hyper::Error;
        type Future = ResponseFuture;
    
        fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
            Ok(()).into()
        }
    
        fn call(&mut self, mut req: Request<BoxBody>) -> Self::Future {
            let uri = Uri::builder()
                .scheme(self.uri.scheme().unwrap().clone())
                .authority(self.uri.authority().unwrap().clone())
                .path_and_query(req.uri().path_and_query().unwrap().clone())
                .build()
                .unwrap();
            *req.uri_mut() = uri;
            match &self.client {
                MyClient::ClearText(client) => client.request(req),
                MyClient::Tls(client) => client.request(req),
            }
        }
    }
}
