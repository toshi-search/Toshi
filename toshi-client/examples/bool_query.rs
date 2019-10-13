use serde::Deserialize;

use toshi::*;

#[derive(Clone, Deserialize)]
pub struct Wiki {
    title: String,
    url: String,
    text: String,
    rating: i32,
}

pub fn main() -> Result<()> {
    let client = ToshiClient::new("http://localhost:8080")?;
    let fuzzy_terms = FuzzyTermBuilder::new().with_value("bears").with_distance(2).build();
    let fuzzy_query = Query::Fuzzy(FuzzyQuery::new(KeyValue::new("text".into(), fuzzy_terms)));
    let query = BoolQuery::builder().must_match(fuzzy_query).build();

    let search = Search::with_query(query);
    let _results: SearchResults<Wiki> = client.search("wiki", search)?;

    Ok(())
}
