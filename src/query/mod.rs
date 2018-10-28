<<<<<<< HEAD
use super::settings::Settings;
pub use {
    self::aggregate::{summary_schema, SumCollector, SummaryDoc},
    self::bool::BoolQuery,
    self::range::{RangeQuery, Ranges},
};

use log::{info, warn};

use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;

use std::collections::HashMap;

mod aggregate;
mod bool;
mod bucket;
mod range;

pub trait CreateQuery {
    fn create_query(self, schema: &Schema) -> Box<TantivyQuery>;
}

pub trait AggregateQuery<T> {
    fn result(&self) -> T;
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Query {
    Boolean { bool: BoolQuery },
    Range(RangeQuery),
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Metrics {
    SumAgg { field: String },
}

#[derive(Deserialize, Debug)]
pub struct Request {
    aggs: Option<Metrics>,
    query: Option<Query>,
    #[serde(default = "Settings::default_result_limit")]
    pub limit: usize,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct ExactTerm {
    term: HashMap<String, String>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct FuzzyTerm {
    value: String,
    #[serde(default)]
    distance: u8,
    #[serde(default)]
    transposition: bool,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum TermQueries {
    Fuzzy { fuzzy: HashMap<String, FuzzyTerm> },
    Exact(ExactTerm),
    Range { range: HashMap<String, Ranges> },
=======
use log::info;
use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;

pub mod aggregate;
pub mod bool;
pub mod bucket;

pub trait CreateQuery {
    fn create_query(&self, schema: &Schema) -> Box<TantivyQuery>;
>>>>>>> Query DSL refactoring...
}
