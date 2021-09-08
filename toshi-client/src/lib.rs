use std::fmt::Display;

use http::Response;
use serde::{de::DeserializeOwned, Serialize};
use tantivy::schema::Schema;

use async_trait::async_trait;

pub use toshi_types::*;

pub use crate::error::ToshiClientError;

pub mod error;

#[cfg(feature = "isahc_client")]
pub use isahc_client::ToshiClient;
#[cfg(feature = "isahc_client")]
mod isahc_client;

#[cfg(feature = "hyper_client")]
pub use hyper_client::HyperToshi;
#[cfg(feature = "hyper_client")]
mod hyper_client;

pub type Result<T> = std::result::Result<T, ToshiClientError>;

#[async_trait]
pub trait AsyncClient {
    type Body;

    async fn index(&self) -> Result<Response<Self::Body>>;

    async fn list(&self) -> Result<Response<Self::Body>>;

    async fn index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display;

    async fn create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display;

    async fn add_document<I, D>(&self, index: I, document: D, options: Option<IndexOptions>) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
        D: Serialize + Send + Sync;

    async fn search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync + Unpin;

    async fn all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync + Unpin;
}

pub trait SyncClient {
    type Body;

    fn sync_index(&self) -> Result<Response<Self::Body>>;

    fn sync_index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Display;

    fn sync_create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Display;

    fn sync_add_document<I, D>(&self, index: I, document: D, options: Option<IndexOptions>) -> Result<Response<Self::Body>>
    where
        I: ToString + Display,
        D: Serialize;

    fn sync_search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Display,
        D: DeserializeOwned + Clone;

    fn sync_all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Display,
        D: DeserializeOwned + Clone;
}
