use serde::{Deserialize, Serialize};
use tantivy::query::{BooleanQuery, Occur, Query as TQuery};
use tantivy::schema::Schema;

use crate::error::Error;
use crate::query::{CreateQuery, Query};
use crate::Result;

#[derive(Serialize, Deserialize, Debug)]
pub struct BoolQuery<'a> {
    #[serde(borrow = "'a")]
    #[serde(default = "Vec::new")]
    must: Vec<Query<'a>>,
    #[serde(borrow = "'a")]
    #[serde(default = "Vec::new")]
    must_not: Vec<Query<'a>>,
    #[serde(borrow = "'a")]
    #[serde(default = "Vec::new")]
    should: Vec<Query<'a>>,
    minimum_should_match: Option<u64>,
    boost: Option<f64>,
}

impl<'a> CreateQuery for BoolQuery<'a> {
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

#[cfg(test)]
mod tests {
    use super::super::*;
    use serde_json;
    use tantivy::schema::*;

    #[test]
    fn test_bool_query() {
        let test_json = r#"
        {"query": {
            "bool": {
                "must":     [ {"term": {"user": "kimchy"}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}} ],
                "must_not": [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gt": -10, "lte": 20}}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}}, ],
                "should":   [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gte": 10, "lte": 20}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}},{"fuzzy": {"user": {"value": "kimchy", "distance": 214}}}, ],
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

        let result = serde_json::from_str::<Search>(test_json).unwrap();
        println!("{:#?}", result);
    }
}
