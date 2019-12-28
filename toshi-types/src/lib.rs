#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms, unreachable_pub)]

//! Toshi-Types
//! These are the high level types available in the Toshi search engine.
//! The client for toshi as well as Toshi itself is built on top of these types. If you are
//! looking for Toshi's protobuf types then you will want to look in the toshi-proto module
//! of Toshi's source code.

pub use client::{ScoredDoc, SearchResults, SummaryResponse};
pub use error::{Error, ErrorResponse};
pub use query::{
    boolean::BoolQuery, facet::FacetQuery, fuzzy::FuzzyQuery, fuzzy::FuzzyTerm, phrase::PhraseQuery, phrase::TermPair, range::RangeQuery,
    range::Ranges, regex::RegexQuery, term::ExactTerm, CreateQuery, KeyValue, Query, Search,
};
pub use server::*;

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
