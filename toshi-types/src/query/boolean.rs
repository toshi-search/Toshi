use serde::{Deserialize, Serialize};
use tantivy::query::{BooleanQuery, Occur, Query as TQuery};
use tantivy::schema::Schema;

use crate::error::Error;
use crate::query::{CreateQuery, Query};
use crate::Result;

/// A boolean query parallel to Tantivy's [`tantivy::query::BooleanQuery`]: BooleanQuery
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoolQuery {
    #[serde(default = "Vec::new")]
    must: Vec<Query>,
    #[serde(default = "Vec::new")]
    must_not: Vec<Query>,
    #[serde(default = "Vec::new")]
    should: Vec<Query>,
    #[serde(default)]
    minimum_should_match: Option<u64>,
    #[serde(default)]
    boost: Option<f64>,
}

impl BoolQuery {
    pub(crate) fn new(
        must: Vec<Query>,
        must_not: Vec<Query>,
        should: Vec<Query>,
        minimum_should_match: Option<u64>,
        boost: Option<f64>,
    ) -> Self {
        Self {
            must,
            must_not,
            should,
            minimum_should_match,
            boost,
        }
    }

    /// Create a builder instance for a BoolQuery
    pub fn builder() -> BoolQueryBuilder {
        BoolQueryBuilder::default()
    }
}

impl CreateQuery for BoolQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn TQuery>> {
        let mut all_queries: Vec<(Occur, Box<dyn TQuery>)> = Vec::new();
        if !self.must.is_empty() {
            all_queries.append(&mut parse_queries(schema, Occur::Must, self.must)?);
        }
        if !self.must_not.is_empty() {
            all_queries.append(&mut parse_queries(schema, Occur::MustNot, self.must_not)?);
        }
        if !self.should.is_empty() {
            all_queries.append(&mut parse_queries(schema, Occur::Should, self.should)?);
        }
        Ok(Box::new(BooleanQuery::from(all_queries)))
    }
}

fn parse_queries(schema: &Schema, occur: Occur, queries: Vec<Query>) -> Result<Vec<(Occur, Box<dyn TQuery>)>> {
    queries
        .into_iter()
        .map(|q| match q {
            Query::Fuzzy(f) => Ok((occur, f.create_query(&schema)?)),
            Query::Exact(q) => Ok((occur, q.create_query(&schema)?)),
            Query::Range(r) => Ok((occur, r.create_query(&schema)?)),
            Query::Phrase(p) => Ok((occur, p.create_query(&schema)?)),
            Query::Regex(r) => Ok((occur, r.create_query(&schema)?)),
            _ => Err(Error::QueryError("Invalid type for boolean query".into())),
        })
        .collect::<Result<Vec<(Occur, Box<dyn TQuery>)>>>()
}

#[derive(Debug, Default)]
pub struct BoolQueryBuilder {
    must: Vec<Query>,
    must_not: Vec<Query>,
    should: Vec<Query>,
    minimum_should_match: u64,
    boost: f64,
}

impl BoolQueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn must_match(mut self, query: Query) -> Self {
        self.must.push(query);
        self
    }

    pub fn must_not_match(mut self, query: Query) -> Self {
        self.must_not.push(query);
        self
    }

    pub fn should_match(mut self, query: Query) -> Self {
        self.should.push(query);
        self
    }

    pub fn with_minimum_should_match(mut self, amount: u64) -> Self {
        self.minimum_should_match = amount;
        self
    }

    pub fn with_boost(mut self, amount: f64) -> Self {
        self.boost = amount;
        self
    }

    pub fn build(self) -> Query {
        Query::Boolean {
            bool: BoolQuery::new(
                self.must,
                self.must_not,
                self.should,
                Some(self.minimum_should_match),
                Some(self.boost),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;
    use tantivy::schema::*;

    use crate::query::Search;

    #[test]
    fn test_bool_query() {
        let test_json = r#"
        {"query": {
            "bool": {
                "must":     [ {"term": {"user": "kimchy"}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}} ],
                "must_not": [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gt": -10, "lte": 20}}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}} ],
                "should":   [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gte": 10, "lte": 20}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}} ],
                "minimum_should_match": 1,
                "boost": 1.0
              }
            },
            "limit": 10
        }"#;
        let mut builder = SchemaBuilder::new();
        let _text_field = builder.add_text_field("user", STORED | TEXT);
        let _u_field = builder.add_i64_field("age", FAST);
        let _schema = builder.build();

        let _result = serde_json::from_str::<Search>(test_json).unwrap();
    }
}
