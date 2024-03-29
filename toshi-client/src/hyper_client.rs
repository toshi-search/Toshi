use std::fmt::Display;

use async_trait::async_trait;
use http::Response;
use hyper::client::connect::Connect;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Uri};
use serde::{de::DeserializeOwned, Serialize};
use tantivy::schema::Schema;

use toshi_types::*;

use crate::Result;

#[derive(Debug, Clone)]
pub struct HyperToshi<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    host: String,
    client: Client<C, Body>,
}

impl HyperToshi<HttpConnector> {
    pub fn new<H: ToString>(host: H) -> Self {
        let client = Client::new();
        Self::with_client(host, client)
    }
}

#[cfg(feature = "rust_tls")]
#[cfg(not(feature = "hyper_tls"))]
impl HyperToshi<hyper_rustls::HttpsConnector<HttpConnector>> {
    pub fn with_tls<H: ToString>(host: H, connector: hyper_rustls::HttpsConnector<HttpConnector>) -> Self {
        let client = Client::builder().build(connector);
        Self::with_client(host, client)
    }
}

#[cfg(feature = "hyper_tls")]
#[cfg(not(feature = "rust_tls"))]
impl HyperToshi<hyper_tls::HttpsConnector<HttpConnector>> {
    pub fn with_tls<H: ToString>(host: H, connector: hyper_tls::HttpsConnector<HttpConnector>) -> Self {
        let client = Client::builder().build(connector);
        Self::with_client(host, client)
    }
}

impl<C> HyperToshi<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    pub fn with_client<H: ToString>(host: H, client: Client<C, Body>) -> Self {
        Self {
            host: host.to_string(),
            client,
        }
    }

    #[inline]
    fn uri<I>(&self, index: I) -> String
    where
        I: ToString,
    {
        format!("{}/{}", self.host, index.to_string())
    }

    #[inline]
    async fn make_request<R>(&self, request: Request<Body>) -> Result<R>
    where
        R: DeserializeOwned + Send + Sync,
    {
        let response = self.client.request(request).await?;
        let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
        serde_json::from_slice::<R>(&body_bytes).map_err(Into::into)
    }
}

#[async_trait]
impl<C> crate::AsyncClient for HyperToshi<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    type Body = hyper::Body;

    async fn index(&self) -> Result<Response<Body>> {
        let request = Request::get(&self.host).body(Body::empty())?;
        self.client.request(request).await.map_err(Into::into)
    }

    async fn list(&self) -> Result<Response<Self::Body>> {
        let uri: Uri = self.uri("_list").parse()?;
        self.client.get(uri).await.map_err(Into::into)
    }

    async fn index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
    {
        let uri = self.uri(format!("{}/_summary?include_sizes={}", index, include_sizes));
        let parsed_uri = uri.parse::<Uri>()?;
        self.client.get(parsed_uri).await.map_err(Into::into)
    }

    async fn create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
    {
        let uri = self.uri(format!("{}/_create", name));
        let body = serde_json::to_vec(&SchemaBody(schema))?;
        let request = Request::put(uri).body(Body::from(body))?;
        self.client.request(request).await.map_err(Into::into)
    }

    async fn add_document<I, D>(&self, index: I, document: D, options: Option<IndexOptions>) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
        D: Serialize + Send + Sync,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&AddDocument { options, document })?;
        let request = Request::put(uri).body(Body::from(body))?;
        self.client.request(request).await.map_err(Into::into)
    }

    async fn search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&search)?;
        let request = Request::post(uri).body(Body::from(body))?;
        self.make_request::<SearchResults<D>>(request).await
    }

    async fn all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync,
    {
        let uri = self.uri(index);
        let request = Request::get(uri).body(Body::empty())?;
        self.make_request::<SearchResults<D>>(request).await
    }
}
