use super::*;

use std::collections::HashMap;
use std::fs::{create_dir, read_dir};
use std::iter::Iterator;
use std::path::PathBuf;

use serde_json::Value as JValue;
use tantivy::collector::TopCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::Index;

use handle::IndexHandle;
use results::*;
use settings::Settings;

#[derive(Deserialize, Debug)]
pub struct Search {
    pub query: Queries,

    #[serde(default = "Settings::default_result_limit")]
    pub limit: usize,
}

impl Search {
    pub fn all() -> Self {
        Search {
            query: Queries::AllDocs,
            limit: Settings::default_result_limit(),
        }
    }
}

#[derive(Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
pub enum Queries {
    TermSearch { term: HashMap<String, JValue> },
    TermsSearch { terms: HashMap<String, Vec<String>> },
    RangeSearch { range: HashMap<String, HashMap<String, i64>> },
    RawSearch { raw: String },
    AllDocs,
}

pub struct IndexCatalog {
    pub settings: Settings,
    base_path:    PathBuf,
    collection:   HashMap<String, IndexHandle>,
}

impl IndexCatalog {
    pub fn with_path(base_path: PathBuf) -> Result<Self> { IndexCatalog::new(base_path, Settings::default()) }

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

    pub fn base_path(&self) -> &PathBuf { &self.base_path }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_index(name: String, index: Index) -> Result<Self> {
        let mut map = HashMap::new();
        let h =
            IndexHandle::new(index, Settings::default()).unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        map.insert(name, h);
        Ok(IndexCatalog {
            settings:   Settings::default(),
            base_path:  PathBuf::new(),
            collection: map,
        })
    }

    pub fn load_index(path: &str) -> Result<Index> {
        let p = PathBuf::from(path);
        if p.exists() {
            Index::open_in_dir(p)
                .map_err(|_| Error::UnknownIndex(path.to_string()))
                .and_then(Ok)
        } else {
            Err(Error::UnknownIndex(path.to_string()))
        }
    }

    pub fn add_index(&mut self, name: String, index: Index) {
        let handle =
            IndexHandle::new(index, self.settings.clone()).unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        self.collection.insert(name, handle);
    }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, IndexHandle> { &self.collection }

    pub fn get_mut_collection(&mut self) -> &mut HashMap<String, IndexHandle> { &mut self.collection }

    pub fn exists(&self, index: &str) -> bool { self.get_collection().contains_key(index) }

    pub fn get_mut_index(&mut self, name: &str) -> Result<&mut IndexHandle> {
        self.collection.get_mut(name).ok_or_else(|| Error::UnknownIndex(name.to_string()))
    }

    pub fn get_index(&self, name: &str) -> Result<&IndexHandle> {
        self.collection.get(name).ok_or_else(|| Error::UnknownIndex(name.to_string()))
    }

    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.collection.clear();
        if !self.base_path.exists() {
            info!("Base data path {} does not exist, creating it...", self.base_path.display());
            create_dir(self.base_path.clone())?;
        }
        for dir in read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            if let Some(entry_str) = entry.to_str() {
                let pth: String = entry_str.rsplit('/').take(1).collect();
                let idx = IndexCatalog::load_index(entry_str)?;
                self.add_index(pth.clone(), idx);
            } else {
                return Err(Error::IOError(format!("Path {} is not a valid unicode path", entry.display())));
            }
        }
        Ok(())
    }

    pub fn search_index(&self, index: &str, search: &Search) -> Result<SearchResults> {
        match self.get_index(index) {
            Ok(hand) => {
                let idx = hand.get_index();
                idx.load_searchers()?;
                let searcher = idx.searcher();
                let schema = idx.schema();
                let fields: Vec<Field> = schema.fields().iter().filter_map(|e| schema.get_field(e.name())).collect();

                let mut collector = TopCollector::with_limit(search.limit);
                let mut query_parser = QueryParser::for_index(idx, fields);
                query_parser.set_conjunction_by_default();

                match &search.query {
                    Queries::TermSearch { term } => {
                        let terms = term.iter().map(|(t, v)| format!("{}:{}", t, v)).collect::<Vec<String>>().join(" ");

                        let query = query_parser.parse_query(&terms)?;
                        info!("{}", terms);
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::RangeSearch { range } => {
                        info!("{:#?}", range);
                        let terms = range
                            .iter()
                            .map(|(field, value)| {
                                let mut term_query = format!("{}:", field);
                                let upper_inclusive = value.contains_key("lte");
                                let lower_inclusive = value.contains_key("gte");

                                if lower_inclusive {
                                    term_query += "[";
                                    if let Some(v) = value.get("gte") {
                                        term_query += &v.to_string();
                                    }
                                } else {
                                    term_query += "{";
                                    if let Some(v) = value.get("gt") {
                                        term_query += &v.to_string();
                                    }
                                }
                                term_query += " TO ";
                                if upper_inclusive {
                                    if let Some(v) = value.get("lte") {
                                        term_query += &v.to_string();
                                    }
                                    term_query += "]";
                                } else {
                                    if let Some(v) = value.get("lt") {
                                        term_query += &v.to_string();
                                    }
                                    term_query += "}";
                                }
                                term_query
                            })
                            .collect::<Vec<String>>()
                            .join(" ");

                        let query = query_parser.parse_query(&terms)?;
                        info!("{}", terms);
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::RawSearch { raw } => {
                        let query = query_parser.parse_query(&raw)?;
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::AllDocs => searcher.search(&AllQuery, &mut collector)?,
                    _ => unimplemented!(),
                };

                let scored_docs: Vec<ScoredDoc> = collector
                    .top_docs()
                    .into_iter()
                    .map(|(score, doc)| {
                        let d = searcher.doc(doc).expect("Doc not found in segment");
                        ScoredDoc::new(score, schema.to_named_doc(&d))
                    })
                    .collect();

                Ok(SearchResults::new(scored_docs))
            }
            Err(e) => Err(e),
        }
    }

    pub fn clear(&mut self) { self.collection.clear(); }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use gotham::router::Router;
    use gotham::test::{TestClient, TestServer};
    use std::sync::{Arc, RwLock};
    use std::fs::remove_dir;

    pub fn create_test_index() -> Index {
        let mut builder = SchemaBuilder::new();
        let test_text = builder.add_text_field("test_text", STORED | TEXT);
        let test_int = builder.add_i64_field("test_i64", INT_STORED | INT_INDEXED);
        let test_unsign = builder.add_u64_field("test_u64", INT_STORED | INT_INDEXED);
        let test_unindexed = builder.add_text_field("test_unindex", STORED);

        let schema = builder.build();
        let idx = Index::create_in_ram(schema);
        let mut writer = idx.writer(30_000_000).unwrap();
        writer.add_document(doc!{ test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64, test_unindexed => "no" });
        writer.add_document(doc!{ test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64, test_unindexed => "yes" });
        writer.add_document(doc!{ test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64, test_unindexed => "noo" });
        writer.add_document(doc!{ test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64, test_unindexed => "yess" });
        writer.add_document(doc!{ test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64, test_unindexed => "nooo" });
        writer.commit().unwrap();

        idx
    }

    pub fn create_test_client(catalog: &Arc<RwLock<IndexCatalog>>) -> TestClient<Router> {
        let server = create_test_server(catalog);
        server.client()
    }

    pub fn create_test_server(catalog: &Arc<RwLock<IndexCatalog>>) -> TestServer<Router> {
        TestServer::new(router::router_with_catalog(catalog)).unwrap()
    }

    #[test]
    fn test_catalog_create_data_dir() {
        let path = PathBuf::from("data_dir");
        assert_eq!(path.exists(), false);

        let _catalog = IndexCatalog::with_path(path.clone()).unwrap();

        assert_eq!(path.exists(), true);
        assert_eq!(path.is_dir(), true);
        remove_dir(path).unwrap();
    }
}
