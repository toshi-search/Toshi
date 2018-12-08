use super::{CreateQuery, Result, TermQueries};

use tantivy::query::{BooleanQuery, Occur, Query};
use tantivy::schema::Schema;

#[derive(Deserialize, Debug, PartialEq)]
pub struct BoolQuery {
    #[serde(default = "Vec::new")]
    must: Vec<TermQueries>,
    #[serde(default = "Vec::new")]
    filter: Vec<TermQueries>,
    #[serde(default = "Vec::new")]
    must_not: Vec<TermQueries>,
    #[serde(default = "Vec::new")]
    should: Vec<TermQueries>,
    minimum_should_match: Option<u64>,
    boost: Option<f64>,
}

impl CreateQuery for BoolQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        let mut all_queries: Vec<(Occur, Box<Query>)> = Vec::new();
        all_queries.append(&mut parse_queries(schema, Occur::Must, &self.must)?);
        all_queries.append(&mut parse_queries(schema, Occur::MustNot, &self.must_not)?);
        all_queries.append(&mut parse_queries(schema, Occur::Should, &self.should)?);
        Ok(Box::new(BooleanQuery::from(all_queries)))
    }
}

fn parse_queries(schema: &Schema, occur: Occur, queries: &[TermQueries]) -> Result<Vec<(Occur, Box<Query>)>> {
    queries
        .iter()
        .map(|q| match q {
            TermQueries::Fuzzy(f) => Ok((occur, f.clone().create_query(&schema)?)),
            TermQueries::Exact(q) => Ok((occur, q.clone().create_query(&schema)?)),
            TermQueries::Range(r) => Ok((occur, r.clone().create_query(&schema)?)),
            TermQueries::Phrase(p) => Ok((occur, p.clone().create_query(&schema)?)),
            TermQueries::Regex(r) => Ok((occur, r.clone().create_query(&schema)?)),
        })
        .collect::<Result<Vec<(Occur, Box<Query>)>>>()
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
                "filter":   [ {"range": {"age": {"gt": -10,"lte": 20}}} ],
                "must_not": [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gt": -10, "lte": 20}}} ],
                "should":   [ {"term": {"user": "kimchy"}}, {"range": {"age": {"gte": 10,"lte": 20}}} ],
                "minimum_should_match": 1,
                "boost": 1.0
              }
            }
        }"#;
        let mut builder = SchemaBuilder::new();
        let _text_field = builder.add_text_field("user", STORED | TEXT);
        let _u_field = builder.add_i64_field("age", FAST);
        let _schema = builder.build();

        let result = serde_json::from_str::<Request>(test_json).unwrap();
        assert_eq!(result.query.is_some(), true);

        if let super::super::Query::Boolean { bool } = result.query.unwrap() {
            assert_eq!(bool.should.is_empty(), false);
            assert_eq!(bool.must_not.len(), 2);
            let query = bool.create_query(&_schema).unwrap().downcast::<BooleanQuery>().unwrap();
            assert_eq!(query.clauses().len(), 6);
        }
    }
}
