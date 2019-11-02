use isahc::prelude::*;
use isahc::HttpClientBuilder;
use serde::{de::DeserializeOwned, Serialize};
use tantivy::schema::Schema;

pub use toshi_types::{
    client::{ScoredDoc, SearchResults},
    query::*,
    server::{AddDocument, IndexOptions, SchemaBody},
};

pub use crate::error::ToshiClientError;

pub mod error;

pub type Result<T> = std::result::Result<T, ToshiClientError>;

#[derive(Debug)]
pub struct ToshiClient {
    host: String,
    client: HttpClient,
}

impl ToshiClient {
    pub fn new<H>(host: H) -> Result<Self>
    where
        H: ToString,
    {
        Ok(Self {
            host: host.to_string(),
            client: HttpClientBuilder::default().build()?,
        })
    }

    pub fn with_client<H: ToString>(host: H, client: HttpClient) -> Self {
        Self {
            host: host.to_string(),
            client,
        }
    }

    #[inline]
    fn uri<I>(&self, index: I) -> String
    where
        I: ToString,
    {
        format!("{}/{}", self.host, index.to_string())
    }

    pub fn all_docs<I, D>(&self, index: I) -> Result<SearchResults<D>>
    where
        I: ToString,
        D: DeserializeOwned + Clone,
    {
        let uri = self.uri(index);
        self.client.get(uri)?.json().map_err(Into::into)
    }

    pub fn search<I, D>(&self, index: I, search: Search) -> Result<SearchResults<D>>
    where
        I: ToString,
        D: DeserializeOwned + Clone,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&search)?;
        self.client.post(uri, body)?.json().map_err(Into::into)
    }

    pub fn add_document<I, D>(&self, index: String, options: Option<IndexOptions>, document: D) -> Result<Response<Body>>
    where
        I: ToString,
        D: Serialize,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&AddDocument { options, document })?;
        self.client.post(uri, body).map_err(Into::into)
    }

    pub fn create_index<I>(&self, name: I, schema: Schema) -> Result<Response<Body>>
    where
        I: ToString,
    {
        let uri = self.uri(format!("{}/_create", name.to_string()));
        let body = serde_json::to_vec(&SchemaBody(schema))?;
        self.client.put(uri, body).map_err(Into::into)
    }

    pub fn index_summary<I>(&self, index: I, include_sizes: bool) -> Result<Response<Body>>
    where
        I: ToString,
    {
        let uri = self.uri(format!("{}/_summary?include_sizes={}", index.to_string(), include_sizes));
        self.client.get(uri).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[derive(Deserialize, Debug, Clone)]
    struct Wiki {
        url: Vec<String>,
        body: Vec<String>,
        title: Vec<String>,
    }

    //    #[ignore]
    //    fn test_client() {
    //        let c = ToshiClient::new("http://localhost:8080").unwrap();
    //        let query = Query::Exact(ExactTerm::with_term("body", "born"));
    //        let search = Search::with_query(query);
    //        let _docs: SearchResults<Wiki> = c.search("wiki", search).unwrap();
    //    }
}
