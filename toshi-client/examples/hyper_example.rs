use serde::Deserialize;

use toshi::{Client, HyperToshi};
use toshi_types::{ExactTerm, Query, Search, SearchResults};

#[derive(Clone, Deserialize)]
pub struct Wiki {
    title: String,
    url: String,
    text: String,
    rating: i32,
}

#[tokio::main]
pub async fn main() -> toshi::Result<()> {
    let client = hyper::Client::default();
    let c = HyperToshi::with_client("http://localhost:8080", client);
    let query = Query::Exact(ExactTerm::with_term("body", "born"));
    let search = Search::with_query(query);
    let _docs: SearchResults<Wiki> = c.search("wiki", search).await?;

    Ok(())
}
