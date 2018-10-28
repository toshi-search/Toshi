use super::*;

use tantivy::query::Query as TantivyQuery;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema};
use tantivy::Term;

use super::CreateQuery;
use query::range::RangeQuery;
use std::collections::HashMap;

use std::collections::HashMap;
use std::ops::Bound;
use std::ops::Bound::*;

macro_rules! type_range {
        ($($n:ident $t:ty),*) => {
            #[derive(Deserialize, Debug, PartialEq)]
            #[serde(untagged)]
            pub enum Ranges {
                $($n {
                    gte: Option<$t>,
                    lte: Option<$t>,
                    lt: Option<$t>,
                    gt: Option<$t>
                },)*
            }
        };
    }

type_range!(U64Range u64, I64Range i64);

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
    fn create_query(self, schema: &Schema) -> Box<TantivyQuery> {
        let mut must = parse_queries(schema, Occur::Must, &self.must);
        let mut must_not = parse_queries(schema, Occur::MustNot, &self.must_not);
        //let _filter = parse_queries(schema, Occur::Should, &self.filter); // I don't think tantivy has this, but ES Does?
        let mut should = parse_queries(schema, Occur::Should, &self.should);
        let mut all_queries: Vec<(Occur, Box<TantivyQuery>)> = Vec::with_capacity(must.len() + must_not.len() + should.len());
        all_queries.append(&mut must);
        all_queries.append(&mut must_not);
        all_queries.append(&mut should);
        let query: Box<TantivyQuery> = Box::new(BooleanQuery::from(all_queries));
        query
    }
}

#[inline]
fn make_field_value(schema: &Schema, k: &str, v: &str) -> Term {
    let field = schema.get_field(k).unwrap_or_else(|| panic!("Field: {} does not exist", k));
    Term::from_field_text(field, v)
}

#[inline]
fn create_ranges<T>(gte: Option<T>, lte: Option<T>, lt: Option<T>, gt: Option<T>) -> (Bound<T>, Bound<T>) {
    let lower = if let Some(b) = gt {
        Excluded(b)
    } else if let Some(b) = gte {
        Included(b)
    } else {
        panic!("No lower bound specified ");
    };
    let upper = if let Some(b) = lt {
        Excluded(b)
    } else if let Some(b) = lte {
        Included(b)
    } else {
        panic!("No lower bound specified ");
    };
    (upper, lower)
}

fn parse_queries(schema: &Schema, occur: Occur, queries: &[TermQueries]) -> Vec<(Occur, Box<TantivyQuery>)> {
    queries
        .into_iter()
        .map(|q| match q {
            TermQueries::Fuzzy { fuzzy } => create_fuzzy_query(&schema, occur, &fuzzy),
            TermQueries::Exact(q) => create_exact_query(&schema, occur, &q.term),
            TermQueries::Range { range } => vec![(occur, RangeQuery::new(range.clone()).create_query(&schema))],
        })
        .flatten()
        .collect()
}

fn create_fuzzy_query(schema: &Schema, occur: Occur, m: &HashMap<String, FuzzyTerm>) -> Vec<(Occur, Box<TantivyQuery>)> {
    m.into_iter()
        .map(|(k, v)| {
            let term = make_field_value(schema, &k, &v.value);
            let query: Box<TantivyQuery> = Box::new(FuzzyTermQuery::new(term, v.distance, v.transposition));
            (occur, query)
        })
        .collect()
}

fn create_exact_query(schema: &Schema, occur: Occur, m: &HashMap<String, String>) -> Vec<(Occur, Box<TantivyQuery>)> {
    m.into_iter()
        .map(|(k, v)| {
            let term = make_field_value(schema, &k, &v);
            let query: Box<TantivyQuery> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
            (occur, query)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use tantivy::schema::*;

    #[test]
    fn test_bool_query() {
        let test_json = r#"{"query":{"bool":{"must":[{"term":{"user":"kimchy"}}],"filter":[{"fuzzy":{"user":{"value":"kimchy"}}},{"range":{"age":{"gt":-10,"lte":20}}}],"must_not":[{"term":{"user":"kimchy"}},{"range":{"age":{"gt":-10,"lte":20}}}],"should":[{"term":{"user":"kimchy"}},{"range":{"age":{"gte":10,"lte":20}}}],"minimum_should_match":1,"boost":1.0}}}"#;
        let mut builder = SchemaBuilder::new();
        let _text_field = builder.add_text_field("user", STORED | TEXT);
        let _u_field = builder.add_i64_field("age", FAST);
        let _schema = builder.build();

        let result = serde_json::from_str::<Request>(test_json).unwrap();
        assert_eq!(result.query.is_some(), true);

        if let Query::Boolean { bool } = result.query.unwrap() {
            assert_eq!(bool.should.is_empty(), false);
            assert_eq!(bool.must_not.len(), 2);
            let query = bool.create_query(&_schema).downcast::<BooleanQuery>().unwrap();
            assert_eq!(query.clauses().len(), 5);
        }
    }
}