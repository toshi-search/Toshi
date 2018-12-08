use crate::handle::IndexHandle;
use crate::query::{CreateQuery, Query, Request};
use crate::results::*;
use crate::settings::Settings;
use crate::{Error, Result};

use log::{debug, info};
use tantivy::collector::TopCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::Index;

use std::collections::HashMap;
use std::fs::read_dir;
use std::iter::Iterator;
use std::path::PathBuf;

pub struct IndexCatalog {
    pub settings: Settings,
    base_path: PathBuf,
    collection: HashMap<String, IndexHandle>,
}

impl IndexCatalog {
    pub fn with_path(base_path: PathBuf) -> Result<Self> {
        IndexCatalog::new(base_path, Settings::default())
    }

    pub fn new(base_path: PathBuf, settings: Settings) -> Result<Self> {
        let mut index_cat = IndexCatalog {
            settings,
            base_path,
            collection: HashMap::new(),
        };
        index_cat.refresh_catalog()?;
        info!("Indexes: {:?}", index_cat.collection.keys());
        Ok(index_cat)
    }

    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_index(name: String, index: Index) -> Result<Self> {
        let mut map = HashMap::new();
        let new_index = IndexHandle::new(index, Settings::default(), &name)
            .unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        map.insert(name, new_index);
        Ok(IndexCatalog {
            settings: Settings::default(),
            base_path: PathBuf::new(),
            collection: map,
        })
    }

    pub fn load_index(path: &str) -> Result<Index> {
        let p = PathBuf::from(path);
        if p.exists() {
            Index::open_in_dir(&p)
                .map_err(|_| Error::UnknownIndex(p.display().to_string()))
                .and_then(Ok)
        } else {
            Err(Error::UnknownIndex(path.to_string()))
        }
    }

    pub fn add_index(&mut self, name: String, index: Index) {
        let handle = IndexHandle::new(index, self.settings.clone(), &name)
            .unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        self.collection.entry(name).or_insert(handle);
    }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, IndexHandle> {
        &self.collection
    }

    pub fn get_mut_collection(&mut self) -> &mut HashMap<String, IndexHandle> {
        &mut self.collection
    }

    pub fn exists(&self, index: &str) -> bool {
        self.get_collection().contains_key(index)
    }

    pub fn get_mut_index(&mut self, name: &str) -> Result<&mut IndexHandle> {
        self.collection.get_mut(name).ok_or_else(|| Error::UnknownIndex(name.to_string()))
    }

    pub fn get_index(&self, name: &str) -> Result<&IndexHandle> {
        self.collection.get(name).ok_or_else(|| Error::UnknownIndex(name.to_string()))
    }

    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.collection.clear();

        for dir in read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            if let Some(entry_str) = entry.to_str() {
                if !entry_str.ends_with(".node_id") {
                    let pth: String = entry_str.rsplit('/').take(1).collect();
                    let idx = IndexCatalog::load_index(entry_str)?;
                    self.add_index(pth.clone(), idx);
                }
            } else {
                return Err(Error::IOError(format!("Path {} is not a valid unicode path", entry.display())));
            }
        }
        Ok(())
    }

    pub fn search_index(&self, index: &str, search: Request) -> Result<SearchResults> {
        match self.get_index(index) {
            Ok(hand) => {
                let idx = hand.get_index();
                idx.load_searchers()?;
                let searcher = idx.searcher();
                let schema = idx.schema();
                let mut collector = TopCollector::with_limit(search.limit);
                if let Some(query) = search.query {
                    match query {
                        Query::Regex(regex) => {
                            let regex_query = regex.create_query(&schema)?;
                            searcher.search(&*regex_query, &mut collector)?
                        }
                        Query::Phrase(phrase) => {
                            let phrase_query = phrase.create_query(&schema)?;
                            searcher.search(&*phrase_query, &mut collector)?
                        }
                        Query::Fuzzy(fuzzy) => {
                            let fuzzy_query = fuzzy.create_query(&schema)?;
                            searcher.search(&*fuzzy_query, &mut collector)?
                        }
                        Query::Exact(term) => {
                            let exact_query = term.create_query(&schema)?;
                            searcher.search(&*exact_query, &mut collector)?
                        }
                        Query::Boolean { bool } => {
                            let bool_query = bool.create_query(&schema)?;
                            searcher.search(&*bool_query, &mut collector)?
                        }
                        Query::Range(range) => {
                            debug!("{:#?}", range);
                            let range_query = range.create_query(&schema)?;
                            debug!("{:?}", range_query);
                            searcher.search(&*range_query, &mut collector)?
                        }
                        Query::Raw { raw } => {
                            let fields: Vec<Field> = schema.fields().iter().filter_map(|e| schema.get_field(e.name())).collect();
                            let query_parser = QueryParser::for_index(idx, fields);
                            let query = query_parser.parse_query(&raw)?;
                            debug!("{:#?}", query);
                            searcher.search(&*query, &mut collector)?
                        }
                        Query::All => searcher.search(&AllQuery, &mut collector)?,
                    }
                }

                let scored_docs: Vec<ScoredDoc> = collector
                    .top_docs()
                    .into_iter()
                    .map(|(score, doc)| {
                        let d = searcher.doc(doc).expect("Doc not found in segment");
                        ScoredDoc::new(Some(score), schema.to_named_doc(&d))
                    })
                    .collect();

                Ok(SearchResults::new(scored_docs))
            }
            Err(e) => Err(e),
        }
    }

    pub fn clear(&mut self) {
        self.collection.clear();
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use gotham::test::{TestClient, TestServer};
    use std::sync::{Arc, RwLock};
    use tantivy::doc;

    pub fn create_test_index() -> Index {
        let mut builder = SchemaBuilder::new();
        let test_text = builder.add_text_field("test_text", STORED | TEXT);
        let test_int = builder.add_i64_field("test_i64", INT_STORED | INT_INDEXED);
        let test_unsign = builder.add_u64_field("test_u64", INT_STORED | INT_INDEXED);
        let test_unindexed = builder.add_text_field("test_unindex", STORED);

        let schema = builder.build();
        let idx = Index::create_in_ram(schema);
        let mut writer = idx.writer(30_000_000).unwrap();
        writer.add_document(doc! { test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64, test_unindexed => "no" });
        writer.add_document(doc! { test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64, test_unindexed => "yes" });
        writer.add_document(doc! { test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64, test_unindexed => "noo" });
        writer.add_document(doc! { test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64, test_unindexed => "yess" });
        writer.add_document(doc! { test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64, test_unindexed => "nooo" });
        writer.commit().unwrap();

        idx
    }

    pub fn create_test_client(catalog: &Arc<RwLock<IndexCatalog>>) -> TestClient {
        let server = create_test_server(catalog);
        server.client()
    }

    pub fn create_test_server(catalog: &Arc<RwLock<IndexCatalog>>) -> TestServer {
        TestServer::new(crate::router::router_with_catalog(catalog)).unwrap()
    }
}
