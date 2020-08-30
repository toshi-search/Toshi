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
pub trait Client {
    type Body;
    async fn index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display;

    async fn create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display;

    async fn add_document<I, D>(&self, index: I, options: Option<IndexOptions>, document: D) -> Result<Response<Self::Body>>
    where
        I: ToString + Send + Sync + Display,
        D: Serialize + Send + Sync;

    async fn search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync;

    async fn all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString + Send + Sync + Display,
        D: DeserializeOwned + Clone + Send + Sync;
}
