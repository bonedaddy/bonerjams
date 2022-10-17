//! A openssl adaptor for `tonic`.
//!
//! Examples can be found in the `example` crate
//! within the repository.
/*


Copyright (c) 2020 Lucio Franco

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


*/

//! A openssl adaptor for `tonic`.
//!
//! Examples can be found in the `example` crate
//! within the repository.

#![warn(missing_debug_implementations, missing_docs, unreachable_pub)]

use async_stream::try_stream;
use futures::{Stream, TryStream, TryStreamExt};
use openssl::ssl::{Ssl, SslAcceptor};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::{server::Connected, Certificate};

/// Wrapper error type.
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A const that contains the on the wire `h2` alpn
/// value that can be passed directly to OpenSSL.
pub const ALPN_H2_WIRE: &[u8] = b"\x02h2";

/// Wrap some incoming stream of io types with OpenSSL's
/// `SslStream` type. This will take some acceptor and a
/// stream of io types and accept connections.
pub fn incoming<S>(
    incoming: S,
    acceptor: SslAcceptor,
) -> impl Stream<Item = Result<SslStream<S::Ok>, Error>>
where
    S: TryStream + Unpin,
    S::Ok: AsyncRead + AsyncWrite + Send + Sync + Debug + Unpin + 'static,
    S::Error: Into<Error>,
{
    let mut incoming = incoming;

    try_stream! {
        while let Some(stream) = incoming.try_next().await? {
            let ssl = Ssl::new(acceptor.context())?;
            let mut tls = tokio_openssl::SslStream::new(ssl, stream)?;
            Pin::new(&mut tls).accept().await?;

            let ssl = SslStream {
                inner: tls
            };

            yield ssl;
        }
    }
}

/// A `SslStream` wrapper type that implements tokio's io traits
/// and tonic's `Connected` trait.
#[derive(Debug)]
pub struct SslStream<S> {
    inner: tokio_openssl::SslStream<S>,
}

impl<S: Connected> Connected for SslStream<S> {
    type ConnectInfo = SslConnectInfo<S::ConnectInfo>;

    fn connect_info(&self) -> Self::ConnectInfo {
        let inner = self.inner.get_ref().connect_info();

        let ssl = self.inner.ssl();
        let certs = ssl
            .verified_chain()
            .map(|certs| {
                certs
                    .iter()
                    .filter_map(|c| c.to_pem().ok())
                    .map(Certificate::from_pem)
                    .collect()
            })
            .map(Arc::new);

        SslConnectInfo { inner, certs }
    }
}

impl<S> AsyncRead for SslStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for SslStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Connection info for SSL streams.
///
/// This type will be accessible through [request extensions](tonic::Request::extensions).
///
/// See [`Connected`](tonic::transport::server::Connected) for more details.
#[derive(Debug, Clone)]
pub struct SslConnectInfo<T> {
    inner: T,
    certs: Option<Arc<Vec<Certificate>>>,
}

impl<T> SslConnectInfo<T> {
    /// Get a reference to the underlying connection info.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Get a mutable reference to the underlying connection info.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Return the set of connected peer SSL certificates.
    pub fn peer_certs(&self) -> Option<Arc<Vec<Certificate>>> {
        self.certs.clone()
    }
}

use http::{Request, Response};
use hyper::{
    client::{HttpConnector, ResponseFuture},
    Client, Uri,
};
use hyper_openssl::HttpsConnector;
use openssl::ssl::{SslConnector, SslMethod};
use tonic::body::BoxBody;
use tonic::transport::Body;
use tower::Service;

/// a custom tonic channel that allows low-level
/// controls of http/https transport setttings and provides
/// a convenient tls capable tonic grpc client
#[derive(Clone, Debug)]
pub struct CustomChannel {
    /// the uri of the grpc server we are connecting to
    pub uri: Uri,
    /// a tls capable client/transport
    pub client: CustomClient,
    pub auth_token: Option<String>,
}

/// provides a tls or cleartext http transport for grpc clients
#[derive(Clone, Debug)]
pub enum CustomClient {
    /// uses unencrypted tcp
    ClearText(Client<HttpConnector, BoxBody>),
    /// uses tls encrypted tcp connections
    Tls(Client<HttpsConnector<HttpConnector>, BoxBody>),
}

impl CustomChannel {
    /// returns a new custom channel
    pub async fn new(
        tls: bool,
        uri: Uri,
        auth_token: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let client = match tls {
            false => CustomClient::ClearText(Client::builder().http2_only(true).build(http)),
            true => {
                let mut connector = SslConnector::builder(SslMethod::tls())?;
                connector.set_alpn_protos(ALPN_H2_WIRE)?;
                let mut https = HttpsConnector::with_connector(http, connector)?;
                https.set_callback(|c, _| {
                    //c.set_verify_hostname(false);
                    c.set_verify(openssl::ssl::SslVerifyMode::NONE);
                    Ok(())
                });
                CustomClient::Tls(Client::builder().http2_only(true).build(https))
            }
        };

        Ok(Self {
            client,
            uri,
            auth_token: if auth_token.is_empty() {
                None
            } else {
                Some(auth_token.to_string())
            },
        })
    }
}

// Check out this blog post for an introduction to Tower:
// https://tokio.rs/blog/2021-05-14-inventing-the-service-trait
impl Service<Request<BoxBody>> for CustomChannel {
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
            CustomClient::ClearText(client) => client.request(req),
            CustomClient::Tls(client) => client.request(req),
        }
    }
}
