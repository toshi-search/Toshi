use std::fmt;

use isahc::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use tantivy::{schema::Schema, IndexMeta};

use toshi_types::{
    client::SearchResults,
    error::ToshiClientError,
    query::*,
    server::{AddDocument, IndexOptions, SchemaBody},
};

pub type Result<T> = std::result::Result<T, ToshiClientError>;

#[derive(Debug, Default)]
pub struct ToshiClient {
    host: String,
    client: HttpClient,
}

impl ToshiClient {
    pub fn new<H>(host: H) -> Self
    where
        H: fmt::Display,
    {
        Self {
            host: host.to_string(),
            client: HttpClient::default(),
        }
    }

    pub fn with_client<H>(host: H, client: HttpClient) -> Self
    where
        H: fmt::Display,
    {
        Self {
            host: host.to_string(),
            client,
        }
    }

    #[inline]
    fn uri(&self, index: String) -> String {
        format!("{}/{}", self.host, index)
    }

    pub fn all_docs<D>(&self, index: String) -> Result<SearchResults<D>>
    where
        D: DeserializeOwned,
    {
        let uri = self.uri(index);
        self.client.get(uri)?.json().map_err(Into::into)
    }

    pub fn search<D>(&self, index: String, search: Search) -> Result<SearchResults<D>>
    where
        D: DeserializeOwned,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&search)?;
        self.client.post(uri, body)?.json().map_err(Into::into)
    }

    pub fn add_document<D: Serialize>(&self, index: String, options: Option<IndexOptions>, document: D) -> Result<Response<Body>>
    where
        D: Serialize,
    {
        let uri = self.uri(index);
        let body = serde_json::to_vec(&AddDocument { options, document })?;
        self.client.post(uri, body).map_err(Into::into)
    }

    pub fn create_index(&self, name: String, schema: Schema) -> Result<Response<Body>> {
        let uri = self.uri(name + "/_create");
        let body = serde_json::to_vec(&SchemaBody(schema))?;
        self.client.put(uri, body).map_err(Into::into)
    }

    pub fn index_summary(&self, index: String, include_sizes: bool) -> Result<IndexMeta> {
        let uri = self.uri(format!("{}/_summary?include_sizes={}", index, include_sizes));
        self.client.get(uri)?.json().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize, Debug)]
    struct Wiki {
        url: Vec<String>,
        body: Vec<String>,
        title: Vec<String>,
    }

    #[test]
    fn test_client() {
        let c = ToshiClient::new("http://localhost:8080");
        let query = Query::Exact(ExactTerm::with_term("body", "born".into()));
        let search = Search::new(Some(query), None, 10);
        let docs: SearchResults<Wiki> = c.search("wiki".into(), search).unwrap();
        //            c.all_docs("wiki".into()).unwrap();

        println!("{:#?}", docs);
    }
}
