#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms, unreachable_pub)]

//! Toshi-Types
//! These are the high level types available in the Toshi search engine.
//! The client for Toshi as well as Toshi itself is built on top of these types. If you are
//! looking for Toshi's protobuf types then you will want to look in the toshi-proto module
//! of Toshi's source code.

use serde_json::Value as SerdeValue;

pub use client::{ScoredDoc, SearchResults, SummaryResponse};
pub use error::{Error, ErrorResponse};
pub use query::{
    boolean::BoolQuery, facet::FacetQuery, fuzzy::FuzzyQuery, fuzzy::FuzzyTerm, phrase::PhraseQuery, phrase::TermPair, range::RangeQuery,
    range::Ranges, regex::RegexQuery, term::ExactTerm, CreateQuery, FlatNamedDocument, KeyValue, Query, Search,
};
pub use server::*;
use std::sync::Arc;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Index, IndexWriter};
use tokio::sync::Mutex;

type Result<T> = std::result::Result<T, error::Error>;

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

/// Determines whether or not the index is local to this machine or if the handle has to go to another
/// node in order to get it's data.
#[derive(Debug)]
pub enum IndexLocation {
    /// This index is in local storage on this node
    LOCAL,
    /// Toshi has to make a request to another server for this index
    REMOTE,
}

/// Defines an interface on how operations are done on indexes inside Toshi
#[async_trait::async_trait]
pub trait IndexHandle: Clone {
    /// The human-readable name of the index
    fn get_name(&self) -> String;
    /// Whether the index is local or remote
    fn index_location(&self) -> IndexLocation;
    /// Return the underlying index
    fn get_index(&self) -> Index;
    /// Return index writer
    fn get_writer(&self) -> Arc<Mutex<IndexWriter>>;
    /// Get size of an index
    fn get_space(&self) -> SearcherSpaceUsage;
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
    type Local: IndexHandle + Send + Sync;

    /// The base path for local indexes, useless for remote
    fn base_path(&self) -> String;
    /// Return the entire collection of handles
    fn get_collection(&self) -> &dashmap::DashMap<String, Self::Local>;
    /// Add a local index to the catalog
    fn add_index(&self, name: String, index: Index) -> Result<()>;
    /// Return a list of index names
    async fn list_indexes(&self) -> Vec<String>;
    /// Return a handle to a single index
    fn get_index(&self, name: &str) -> Result<Self::Local>;
    /// Determine if an index exists locally
    fn exists(&self, index: &str) -> bool;
    /// The current catalog's raft_id
    fn raft_id(&self) -> u64;
}
