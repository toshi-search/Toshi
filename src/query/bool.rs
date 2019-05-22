use crate::query::{CreateQuery, Query};
use crate::{error::Error, Result};

use serde::{Deserialize, Serialize};
use tantivy::query::{BooleanQuery, Occur, Query as TQuery};
use tantivy::schema::Schema;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct BoolQuery {
    #[serde(default = "Vec::new")]
    must: Vec<Query>,
    #[serde(default = "Vec::new")]
    must_not: Vec<Query>,
    #[serde(default = "Vec::new")]
    should: Vec<Query>,
    minimum_should_match: Option<u64>,
    boost: Option<f64>,
}

impl CreateQuery for BoolQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<TQuery>> {
        let mut all_queries: Vec<(Occur, Box<TQuery>)> = Vec::new();
        all_queries.append(&mut parse_queries(schema, Occur::Must, &self.must)?);
        all_queries.append(&mut parse_queries(schema, Occur::MustNot, &self.must_not)?);
        all_queries.append(&mut parse_queries(schema, Occur::Should, &self.should)?);
        Ok(Box::new(BooleanQuery::from(all_queries)))
    }
}

fn parse_queries(schema: &Schema, occur: Occur, queries: &[Query]) -> Result<Vec<(Occur, Box<TQuery>)>> {
    queries
        .iter()
        .map(|q| match q {
            Query::Fuzzy(f) => Ok((occur, f.clone().create_query(&schema)?)),
            Query::Exact(q) => Ok((occur, q.clone().create_query(&schema)?)),
            Query::Range(r) => Ok((occur, r.clone().create_query(&schema)?)),
            Query::Phrase(p) => Ok((occur, p.clone().create_query(&schema)?)),
            Query::Regex(r) => Ok((occur, r.clone().create_query(&schema)?)),
            _ => Err(Error::QueryError("Invalid type for boolean query".into())),
        })
        .collect::<Result<Vec<(Occur, Box<TQuery>)>>>()
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use serde_json;
    use tantivy::schema::*;

    #[test]
    fn test_bool_query() {
        let test_json = r#"
        {"query": {
            "bool": {
                "must":     [ {"term": {"user": "kimchy"}}, {"fuzzy": {"user": {"value": "kimchy", "distance": 214}}} ],
                "must_not": [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gt": -10, "lte": 20}}} ],
                "should":   [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gte": 10, "lte": 20}}} ],
                "minimum_should_match": 1,
                "boost": 1.0
              }
            }
        }"#;
        let mut builder = SchemaBuilder::new();
        let _text_field = builder.add_text_field("user", STORED | TEXT);
        let _u_field = builder.add_i64_field("age", FAST);
        let _schema = builder.build();

        let result = serde_json::from_str::<Search>(test_json).unwrap();
        assert_eq!(result.query.is_some(), true);

        if let super::super::Query::Boolean { bool } = result.query.unwrap() {
            assert_eq!(bool.should.is_empty(), false);
            assert_eq!(bool.must_not.len(), 2);
            let query = bool.create_query(&_schema).unwrap().downcast::<BooleanQuery>().unwrap();
            assert_eq!(query.clauses().len(), 6);
        }
    }
}
