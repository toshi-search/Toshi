use isahc::prelude::*;
use isahc::HttpClientBuilder;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tantivy::schema::Schema;

use async_trait::async_trait;
use toshi_types::*;

use crate::{Client, Result};
use std::fmt::Display;

#[derive(Debug)]
pub struct ToshiClient {
    host: String,
    client: HttpClient,
}

impl ToshiClient {
    pub fn new<H>(host: H) -> Result<Self>
    where
        H: ToString,
    {
        Ok(Self {
            host: host.to_string(),
            client: HttpClientBuilder::default().build()?,
        })
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
impl Client for ToshiClient {
    type Body = isahc::Body;

    async fn index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
    {
        let uri = self.uri(format!("{}/_summary?include_sizes={}", index, include_sizes));
        self.client.get(uri).map_err(Into::into)
    }

    async fn create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
    {
        let uri = self.uri(format!("{}/_create", name));
        let body = serde_json::to_vec(&SchemaBody(schema))?;
        self.client.put(uri, body).map_err(Into::into)
    }

    async fn add_document<I, D>(&self, index: I, options: Option<IndexOptions>, document: D) -> Result<Response<Body>>
    where
        I: ToString + Send + Sync + Display,
        D: Serialize + Send + Sync,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&AddDocument { options, document })?;
        self.client.put(uri, body).map_err(Into::into)
    }

    async fn search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&search)?;
        self.client.post(uri, body)?.json().map_err(Into::into)
    }

    async fn all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync,
    {
        let uri = self.uri(index);
        self.client.get(uri)?.json().map_err(Into::into)
    }
}
