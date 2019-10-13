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
    let query = RangeQuery::builder().gte(3).lte(5).for_field("rating").build();

    let search = Search::with_query(query);
    let _results: SearchResults<Wiki> = client.search("wiki", search)?;

    Ok(())
}
