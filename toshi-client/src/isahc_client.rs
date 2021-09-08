use isahc::prelude::*;
use isahc::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tantivy::schema::Schema;

use async_trait::async_trait;
use toshi_types::*;

use crate::{AsyncClient, Result, SyncClient};
use isahc::{HttpClient, Response};
use std::fmt::Display;

#[derive(Debug)]
pub struct ToshiClient {
    host: String,
    client: HttpClient,
}

impl ToshiClient {
    pub fn new<H: ToString>(host: H) -> Self {
        let client = HttpClient::new().unwrap();
        Self::with_client(host, client)
    }

    pub fn with_client<H: ToString>(host: H, client: HttpClient) -> Self {
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
}

#[async_trait]
impl AsyncClient for ToshiClient {
    type Body = isahc::AsyncBody;

    async fn index(&self) -> Result<Response<Self::Body>> {
        self.client.get_async(&self.host).await.map_err(Into::into)
    }

    async fn list(&self) -> Result<Response<Self::Body>> {
        let uri = self.uri("");
        self.client.get_async(uri).await.map_err(Into::into)
    }

    async fn index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
    {
        let uri = self.uri(format!("{}/_summary?include_sizes={}", index, include_sizes));
        self.client.get_async(uri).await.map_err(Into::into)
    }

    async fn create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
    {
        let uri = self.uri(format!("{}/_create", name));
        let body = serde_json::to_vec(&SchemaBody(schema))?;
        self.client.put_async(uri, body).await.map_err(Into::into)
    }

    async fn add_document<I, D>(&self, index: I, document: D, options: Option<IndexOptions>) -> Result<Response<AsyncBody>>
    where
        I: ToString + Send + Sync + Display,
        D: Serialize + Send + Sync,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&AddDocument { options, document })?;
        self.client.put_async(uri, body).await.map_err(Into::into)
    }

    async fn search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync + Unpin,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&search)?;
        self.client.post_async(uri, body).await?.json().await.map_err(Into::into)
    }

    async fn all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync + Unpin,
    {
        let uri = self.uri(index);
        self.client.get_async(uri).await?.json().await.map_err(Into::into)
    }
}

impl SyncClient for ToshiClient {
    type Body = isahc::Body;

    fn sync_index(&self) -> Result<Response<Self::Body>> {
        self.client.get(self.host.clone()).map_err(Into::into)
    }

    fn sync_index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Display,
    {
        let uri = self.uri(format!("{}/_summary?include_sizes={}", index, include_sizes));
        self.client.get(uri).map_err(Into::into)
    }

    fn sync_create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Display,
    {
        let uri = self.uri(format!("{}/_create", name));
        let body = serde_json::to_vec(&SchemaBody(schema))?;
        self.client.put(uri, body).map_err(Into::into)
    }

    fn sync_add_document<I, D>(&self, index: I, document: D, options: Option<IndexOptions>) -> Result<Response<Self::Body>>
    where
        I: ToString + Display,
        D: Serialize,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&AddDocument { options, document })?;
        self.client.put(uri, body).map_err(Into::into)
    }

    fn sync_search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Display,
        D: DeserializeOwned + Clone,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&search)?;
        self.client.post(uri, body)?.json().map_err(Into::into)
    }

    fn sync_all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Display,
        D: DeserializeOwned + Clone,
    {
        let uri = self.uri(index);
        self.client.get(uri)?.json().map_err(Into::into)
    }
}
