use handlers::search::Search;

use std::collections::HashMap;
use std::fs::read_dir;
use std::io;
use std::path::PathBuf;

use tantivy::collector::TopCollector;
use tantivy::query::FuzzyTermQuery;
use tantivy::schema::*;
use tantivy::Index;

use super::{Error, Result};

#[derive(Serialize, Debug, Clone)]
pub struct IndexCatalog {
    base_path: PathBuf,

    #[serde(skip_serializing)]
    collection: HashMap<String, Index>,
}

impl IndexCatalog {
    pub fn new(base_path: PathBuf) -> io::Result<Self> {
        let mut index_cat = IndexCatalog {
            base_path:  base_path.clone(),
            collection: HashMap::new(),
        };
        index_cat.refresh_catalog()?;
        info!("Indexes: {:?}", index_cat.collection.keys());
        Ok(index_cat)
    }

    pub fn load_index(path: &str) -> Result<Index> {
        let p = PathBuf::from(path);
        if p.exists() {
            Index::open_in_dir(p)
                .map_err(|_| Error::UnknownIndex(format!("No Index exists at path: {}", path)))
                .and_then(|r| Ok(r))
        } else {
            Err(Error::UnknownIndex(format!("No Index exists at path: {}", path)))
        }
    }
}

impl IndexCatalog {
    pub fn add_index(&mut self, name: String, index: Index) { self.collection.insert(name, index); }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, Index> { &self.collection }

    pub fn get_index(&self, name: &str) -> Result<&Index> { self.collection.get(name).ok_or_else(|| Error::UnknownIndex(name.to_string())) }

    pub fn refresh_catalog(&mut self) -> io::Result<()> {
        self.collection.clear();

        for dir in read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            let entry_str = entry.to_str().unwrap();
            let pth: String = entry_str.rsplit("/").take(1).collect();
            info!("Adding Index: {}", pth);
            let idx = IndexCatalog::load_index(entry_str).unwrap();
            self.add_index(pth.clone(), idx);
        }
        Ok(())
    }

    pub fn search_index(&self, index: &str, search: &Search) -> Result<Vec<NamedFieldDocument>> {
        match self.get_index(index) {
            Ok(index) => {
                index.load_searchers()?;
                let searcher = index.searcher();
                let schema = index.schema();
                let field = schema.get_field(search.get_field()).unwrap();
                let term = Term::from_field_text(field, search.get_term());
                let query = FuzzyTermQuery::new(term, 2, true);
                let mut collector = TopCollector::with_limit(search.get_limit());
                searcher.search(&query, &mut collector)?;

                Ok(collector
                    .docs()
                    .into_iter()
                    .map(|d| searcher.doc(&d).unwrap())
                    .map(|d| schema.to_named_doc(&d))
                    .collect())
            }
            Err(e) => Err(e),
        }
    }

    #[allow(dead_code)]
    pub fn create_index(&mut self, _path: &str, _schema: &Schema) -> Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_env_logger;
    use std::env;
    use std::fs::create_dir_all;

    #[test]
    fn test_path_splitting() {
        env::set_var("RUST_LOG", "info");
        pretty_env_logger::init();

        let test_path = PathBuf::from("./indexes/test_index");
        create_dir_all("./indexes").unwrap();
        let mut schema = SchemaBuilder::new();
        schema.add_text_field("field", TEXT | STORED);
        let built = schema.build();
        Index::create_in_dir(&test_path, built).unwrap();

        let cat = IndexCatalog::new(PathBuf::from("./indexes")).unwrap();

        assert_eq!(cat.get_collection().contains_key("test_index"), true);
    }
}
