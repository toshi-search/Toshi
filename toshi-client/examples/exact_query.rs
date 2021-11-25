#![allow(dead_code)]

use serde::Deserialize;
use toshi::*;

#[derive(Clone, Deserialize)]
pub struct Wiki {
    title: String,
    url: String,
    text: String,
    rating: i32,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let c = ToshiClient::new("http://localhost:8080");
    let query = Query::Exact(ExactTerm::with_term("body", "born"));
    let search = Search::from_query(query);
    let _docs: SearchResults<Wiki> = c.search("wiki", search).await?;

    Ok(())
}
