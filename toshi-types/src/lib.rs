#![warn(clippy::all)]
//! Toshi-Types
//! These are the high level types available in the Toshi search engine.
//! The client for Toshi as well as Toshi itself is built on top of these types. If you are
//! looking for Toshi's protobuf types then you will want to look in the toshi-proto module
//! of Toshi's source code.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hyper::{Body, Response};
use serde_json::Value as SerdeValue;
use tantivy::schema::Schema;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Index, IndexWriter};
use tokio::sync::Mutex;

pub use client::{ScoredDoc, SearchResults, SummaryResponse};
pub use error::{Error, ErrorResponse};
pub use query::{
    boolean::BoolQuery, facet::FacetQuery, fuzzy::FuzzyQuery, fuzzy::FuzzyTerm, phrase::PhraseQuery, phrase::TermPair, range::RangeQuery,
    range::Ranges, regex::RegexQuery, term::ExactTerm, CreateQuery, FlatNamedDocument, KeyValue, Query, QueryOptions, Search,
};
pub use server::*;

/// Toshi client result type
pub type Result<T> = std::result::Result<T, error::Error>;

/// Types related to the response Toshi gives back to requests
mod client;

/// Errors associated with Toshi's responses
mod error;

/// Types related to Toshi's Query DSL
mod query;

/// Types related to the POST bodies that Toshi accepts for requests
mod server;

/// Extra error conversions Toshi uses, if users want they can omit this feature to not pull in
/// hyper and tonic dependencies
#[cfg(feature = "extra-errors")]
mod extra_errors;

/// Defines an interface on how operations are done on indexes inside Toshi
#[async_trait::async_trait]
pub trait IndexHandle: Clone {
    /// The human-readable name of the index
    fn get_name(&self) -> String;
    /// Return the underlying index
    fn get_index(&self) -> Index;
    /// Return index writer
    fn get_writer(&self) -> Arc<Mutex<IndexWriter>>;
    /// Get size of an index
    fn get_space(&self) -> SearcherSpaceUsage;
    /// The agreed upon raft commit ID this index is currently at.
    fn get_opstamp(&self) -> usize;
    /// Set that opstamp
    fn set_opstamp(&self, opstamp: usize);
    /// Commit the current index writes
    async fn commit(&self) -> Result<u64>;
    /// Search for documents in this index
    async fn search_index(&self, search: Search) -> Result<SearchResults<FlatNamedDocument>>;
    /// Add documents to this index
    async fn add_document(&self, doc: AddDocument<SerdeValue>) -> Result<()>;
    /// Delete terms/documents from this index
    async fn delete_term(&self, term: DeleteDoc) -> Result<DocsAffected>;
}

/// Defines the interface for obtaining a handle from a catalog to an index
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + 'static {
    /// The type of handle the catalog returns when the index is local
    type Handle: IndexHandle + Send + Sync;

    /// The base path for local indexes, useless for remote
    fn base_path(&self) -> String;
    /// Return the entire collection of handles
    fn get_collection(&self) -> &dashmap::DashMap<String, Self::Handle>;
    /// Add a local index to the catalog
    async fn add_index(&self, name: &str, schema: Schema) -> Result<()>;
    /// Return a list of index names
    async fn list_indexes(&self) -> Vec<String>;
    /// Return a handle to a single index
    fn get_index(&self, name: &str) -> Result<Self::Handle>;
    /// Determine if an index exists locally
    fn exists(&self, index: &str) -> bool;
}

#[allow(missing_docs)]
#[async_trait::async_trait]
pub trait Serve<C>
where
    C: crate::Catalog,
{
    async fn list_indexes(&self, catalog: Arc<C>) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn create_index(catalog: Arc<C>, body: Body, idx: &str) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn index_summary(catalog: Arc<C>, idx: &str, options: QueryOptions) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn flush(catalog: Arc<C>, idx: &str) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn bulk_insert(
        catalog: Arc<C>,
        watcher: Arc<AtomicBool>,
        mut body: Body,
        index: &str,
        num_threads: usize,
    ) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn doc_search(catalog: Arc<C>, body: Body, idx: &str) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn add_document(catalog: Arc<C>, body: Body, idx: &str) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn delete_term(catalog: Arc<C>, body: Body, idx: &str) -> std::result::Result<Response<Body>, hyper::Error>;

    async fn all_docs(catalog: Arc<C>, idx: &str) -> std::result::Result<Response<Body>, hyper::Error>;
}
