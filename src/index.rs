use super::*;

use std::collections::HashMap;
use std::fs::read_dir;
use std::iter::Iterator;
use std::path::PathBuf;

use tantivy::collector::TopCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::Index;

use handlers::search::{Queries, Search};

#[derive(Clone, Debug)]
pub struct IndexCatalog {
    base_path:  PathBuf,
    collection: HashMap<String, Index>,
}

#[derive(Serialize)]
pub struct SearchResults {
    // TODO: Add Timing
    // TODO: Add Shard Information
    hits: usize,
    docs: Vec<ScoredDoc>,
}

impl SearchResults {
    pub fn new(docs: Vec<ScoredDoc>) -> Self { SearchResults { hits: docs.len(), docs } }

    pub fn len(&self) -> usize { self.docs.len() }

    pub fn is_empty(&self) -> bool { self.docs.is_empty() }
}

#[derive(Serialize)]
pub struct ScoredDoc {
    score: f32,
    doc:   NamedFieldDocument,
}

impl ScoredDoc {
    pub fn new(score: f32, doc: NamedFieldDocument) -> Self { ScoredDoc { score, doc } }
}

impl IndexCatalog {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        let mut index_cat = IndexCatalog {
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
        map.insert(name, index);
        Ok(IndexCatalog {
            base_path:  PathBuf::new(),
            collection: map,
        })
    }

    pub fn load_index(path: &str) -> Result<Index> {
        let p = PathBuf::from(path);
        if p.exists() {
            Index::open_in_dir(p)
                .map_err(|_| Error::UnknownIndex(format!("No Index exists at path: {}", path)))
                .and_then(Ok)
        } else {
            Err(Error::UnknownIndex(format!("No Index exists at path: {}", path)))
        }
    }
}

impl IndexCatalog {
    pub fn add_index(&mut self, name: String, index: Index) { self.collection.insert(name, index); }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, Index> { &self.collection }

    pub fn exists(&self, index: &str) -> bool { self.get_collection().contains_key(index) }

    pub fn get_index(&self, name: &str) -> Result<&Index> { self.collection.get(name).ok_or_else(|| Error::UnknownIndex(name.to_string())) }

    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.collection.clear();

        for dir in read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            if let Some(entry_str) = entry.to_str() {
                let pth: String = entry_str.rsplit('/').take(1).collect();
                let idx = IndexCatalog::load_index(entry_str)?;
                self.add_index(pth.clone(), idx);
            } else {
                return Err(Error::IOError(format!("Path {:?} is not a valid unicode path", entry)));
            }
        }
        Ok(())
    }

    pub fn search_index(&self, index: &str, search: &Search) -> Result<SearchResults> {
        match self.get_index(index) {
            Ok(index) => {
                index.load_searchers()?;
                let searcher = index.searcher();
                let schema = index.schema();
                let fields: Vec<Field> = schema.fields().iter().filter_map(|e| schema.get_field(e.name())).collect();

                let mut collector = TopCollector::with_limit(search.limit);
                let mut query_parser = QueryParser::for_index(index, fields);
                query_parser.set_conjunction_by_default();

                match &search.query {
                    Queries::TermQuery { term } => {
                        let terms = term.iter().map(|(t, v)| format!("{}:{}", t, v)).collect::<Vec<String>>().join(" ");

                        let query = query_parser.parse_query(&terms)?;
                        info!("{}", terms);
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::RangeQuery { range } => {
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
                            }).collect::<Vec<String>>()
                            .join(" ");

                        let query = query_parser.parse_query(&terms)?;
                        info!("{}", terms);
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::AllQuery => {
                        info!("Retrieving all docs...");
                        searcher.search(&AllQuery, &mut collector)?;
                    }
                    Queries::RawQuery { raw } => {
                        let query = query_parser.parse_query(&raw)?;
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    _ => unimplemented!(),
                };

                let scored_docs: Vec<ScoredDoc> = collector
                    .score_docs()
                    .iter()
                    .map(|(score, doc)| {
                        let d = searcher.doc(&doc).expect("Doc not found in segment");
                        ScoredDoc::new(*score, schema.to_named_doc(&d))
                    }).collect();

                Ok(SearchResults::new(scored_docs))
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use gotham::router::Router;
    use gotham::test::{TestClient, TestServer};
    use std::sync::Arc;
    use std::sync::RwLock;

    pub fn create_test_index() -> Index {
        let mut builder = SchemaBuilder::new();
        let test_text = builder.add_text_field("test_text", STORED | TEXT);
        let test_int = builder.add_i64_field("test_i64", INT_STORED | INT_INDEXED);
        let test_unsign = builder.add_u64_field("test_u64", INT_STORED | INT_INDEXED);

        let schema = builder.build();
        let idx = Index::create_in_ram(schema);
        let mut writer = idx.writer(30_000_000).unwrap();
        writer.add_document(doc!{ test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64 });
        writer.add_document(doc!{ test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64 });
        writer.add_document(doc!{ test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64 });
        writer.add_document(doc!{ test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64 });
        writer.add_document(doc!{ test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64 });
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

    #[cfg(not(target_family = "windows"))]
    #[test]
    fn test_catalog_errors() {
        let catalog = IndexCatalog::new(PathBuf::from("asdf1234"));

        match catalog {
            Ok(_) => {}
            Err(Error::IOError(e)) => assert_eq!("No such file or directory (os error 2)", e),
            _ => {}
        }
    }
}
