use std::fmt;
use std::marker::PhantomData;

use dashmap::DashMap;
use serde::de::{DeserializeOwned, Deserializer, Error as SerdeError, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tantivy::query::Query as TantivyQuery;
use tantivy::schema::{NamedFieldDocument, Schema};
use tantivy::Term;

use crate::error::Error;
use crate::query::{
    boolean::BoolQuery, facet::FacetQuery, fuzzy::FuzzyQuery, phrase::PhraseQuery, range::RangeQuery, regex::RegexQuery, term::ExactTerm,
};

pub(crate) mod boolean;
pub(crate) mod facet;
pub(crate) mod fuzzy;
pub(crate) mod phrase;
pub(crate) mod range;
pub(crate) mod regex;
pub(crate) mod term;

/// Trait that generically represents Tantivy queries
pub trait CreateQuery {
    /// Consume the implementing struct to generate a Tantivy query
    fn create_query(self, schema: &Schema) -> crate::Result<Box<dyn TantivyQuery>>;
}

/// The possible Tantivy Queries to issue
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Query {
    /// [`tantivy::query::FuzzyQuery`]: FuzzyQuery
    Fuzzy(FuzzyQuery),
    /// [`tantivy::query::TermQuery`]: TermQuery
    Exact(ExactTerm),
    /// [`tantivy::query::PhraseQuery`]: PhraseQuery
    Phrase(PhraseQuery),
    /// [`tantivy::query::RegexQuery`]: RegexQuery
    Regex(RegexQuery),
    /// [`tantivy::query::RangeQuery`]: RangeQuery
    Range(RangeQuery),
    /// [`tantivy::query::BooleanQuery`]: BooleanQuery
    Boolean {
        /// Collection of boolean clauses
        bool: BoolQuery,
    },
    /// Raw is a query that passes by the query parser and is just executed directly against the index
    Raw {
        /// The actual query to be ran
        raw: String,
    },
    /// [`tantivy::query::AllQuery`]: AllQuery
    All,
}

/// The request body of a search POST in Toshi
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Search {
    /// Optional query
    pub query: Option<Query>,
    /// Optional facets of a query
    pub facets: Option<FacetQuery>,
    /// Max number of documents to return
    #[serde(default = "Search::default_limit")]
    pub limit: usize,
    /// Field to sort results by
    #[serde(default)]
    pub sort_by: Option<String>,
}

impl Search {
    /// Construct a new Search query
    pub fn new(query: Option<Query>, facets: Option<FacetQuery>, limit: usize, sort_by: Option<String>) -> Self {
        Search {
            query,
            facets,
            limit,
            sort_by,
        }
    }

    /// Construct a builder to create the Search with
    pub fn builder() -> SearchBuilder {
        SearchBuilder::new()
    }

    /// Construct a search with a known Query
    pub fn with_query(query: Query) -> Self {
        Self::new(Some(query), None, Self::default_limit(), None)
    }

    /// The default limit for docs to return
    pub const fn default_limit() -> usize {
        100
    }

    pub(crate) fn all_query() -> Option<Query> {
        Some(Query::All)
    }

    /// A shortcut for querying for all documents in an Index
    pub fn all_docs() -> Self {
        Self {
            query: Self::all_query(),
            facets: None,
            limit: Self::default_limit(),
            sort_by: None,
        }
    }

    pub fn all_limit(limit: usize) -> Self {
        let mut all = Self::all_docs();
        all.limit = limit;
        all
    }
}

#[derive(Debug)]
pub struct SearchBuilder {
    query: Query,
    facets: Option<FacetQuery>,
    limit: usize,
    sort_by: Option<String>,
}

impl Default for SearchBuilder {
    fn default() -> Self {
        SearchBuilder::new()
    }
}

impl SearchBuilder {
    fn new() -> Self {
        Self {
            query: Query::All,
            facets: None,
            limit: 100,
            sort_by: None,
        }
    }

    pub fn with_query(mut self, query: Query) -> Self {
        self.query = query;
        self
    }
    pub fn with_facets(mut self, facets: FacetQuery) -> Self {
        self.facets = Some(facets);
        self
    }
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
    pub fn sort_by<V>(mut self, field: V) -> Self
    where
        V: ToString,
    {
        self.sort_by = Some(field.to_string());
        self
    }
    pub fn build(self) -> Search {
        Search::new(Some(self.query), self.facets, self.limit, self.sort_by)
    }
}

fn make_field_value(schema: &Schema, k: &str, v: &str) -> crate::Result<Term> {
    let field = schema
        .get_field(k)
        .ok_or_else(|| Error::QueryError(format!("Unknown field: {}", k)))?;
    Ok(Term::from_field_text(field, v))
}

/// A single key/value pair, this struct is used when we want to accept only single key/value pairs
/// for a query and a Map would not allow that.
#[derive(Debug, Clone)]
pub struct KeyValue<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    /// Key
    pub field: K,
    /// Value
    pub value: V,
}

impl<K, V> KeyValue<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    /// Construct a key value pair from known values
    pub fn new(field: K, value: V) -> Self {
        Self { field, value }
    }
}

struct KVVisitor<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    marker: PhantomData<fn() -> KeyValue<K, V>>,
}

impl<K, V> KVVisitor<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    fn new() -> Self {
        KVVisitor { marker: PhantomData }
    }
}

impl<'de, K, V> Visitor<'de> for KVVisitor<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Value = KeyValue<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object with a single string value of any key name")
    }

    fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        if let Some((field, value)) = access.next_entry()? {
            if access.next_entry::<String, V>()?.is_some() {
                Err(M::Error::custom("too many values"))
            } else {
                Ok(KeyValue { field, value })
            }
        } else {
            Err(M::Error::custom("not enough values"))
        }
    }
}

impl<'de, K, V> Deserialize<'de> for KeyValue<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(KVVisitor::new())
    }
}

impl<'de, K, V> Serialize for KeyValue<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut m = serializer.serialize_map(Some(1))?;
        m.serialize_entry(&self.field, &self.value)?;
        m.end()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlatNamedDocument(pub DashMap<String, Value>);

impl Into<FlatNamedDocument> for NamedFieldDocument {
    fn into(self) -> FlatNamedDocument {
        let map = DashMap::with_capacity(self.0.len());
        for (k, v) in self.0 {
            if v.len() == 1 {
                map.insert(k, serde_json::to_value(&v[0]).unwrap());
                continue;
            }
            map.insert(k, serde_json::to_value(v).unwrap());
        }
        FlatNamedDocument(map)
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::*;

    use super::*;

    #[test]
    fn test_doc_deserialize() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let author = schema_builder.add_text_field("text", TEXT);
        let likes = schema_builder.add_u64_field("num_u64", FAST);
        let schema: Schema = schema_builder.build();
        let doc = tantivy::doc!(
            title => "Life Aquatic",
            author => "Wes Anderson",
            likes => 4u64
        );
        let named: FlatNamedDocument = schema.to_named_doc(&doc).into();

        println!("{}", serde_json::to_string_pretty(&named).unwrap());
    }

    #[test]
    fn test_kv_serialize() {
        let kv = KeyValue::new("test_field".to_string(), 1);
        let expected = r#"{"test_field":1}"#;
        assert_eq!(expected, serde_json::to_string(&kv).unwrap());
    }

    #[test]
    fn test_builder() {
        let query_builder = FuzzyQuery::builder().for_field("text").with_distance(20).with_value("Hi!").build();
        let builder = Search::builder().with_limit(50).with_query(query_builder).sort_by("text");
        let query = builder.build();

        assert_eq!(query.query.is_some(), true);
        assert_eq!(query.limit, 50);
        assert_eq!(query.sort_by.is_some(), true);
        assert_eq!(query.sort_by.unwrap(), "text");
    }
}
