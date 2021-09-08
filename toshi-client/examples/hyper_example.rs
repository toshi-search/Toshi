use serde::Deserialize;

use toshi::{AsyncClient, HyperToshi};
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
    let c = HyperToshi::new("http://localhost:8080");
    let query = Query::Exact(ExactTerm::with_term("body", "born"));
    let search = Search::from_query(query);
    let _docs: SearchResults<Wiki> = c.search("wiki", search).await?;

    Ok(())
}
