use super::settings::Settings;
use super::{Error, Result};

use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;
use tantivy::Term;

pub use {
    self::aggregate::{summary_schema, SumCollector, SummaryDoc},
    self::bool::BoolQuery,
    self::fuzzy::{FuzzyQuery, FuzzyTerm},
    self::phrase::PhraseQuery,
    self::range::{RangeQuery, Ranges},
    self::regex::RegexQuery,
    self::term::ExactTerm,
};

mod aggregate;
mod bool;
mod fuzzy;
mod phrase;
mod range;
mod regex;
mod term;

pub trait CreateQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<TantivyQuery>>;
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
#[serde(untagged)]
pub enum TermQueries {
    Fuzzy(FuzzyQuery),
    Exact(ExactTerm),
    Phrase(PhraseQuery),
    Range(RangeQuery),
    Regex(RegexQuery),
}

fn make_field_value(schema: &Schema, k: &str, v: &str) -> Result<Term> {
    let field = schema
        .get_field(k)
        .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", k)))?;
    Ok(Term::from_field_text(field, v))
}
