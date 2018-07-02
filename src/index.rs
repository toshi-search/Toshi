use handlers::Search;
use settings::SETTINGS;
use std::collections::HashMap;
use std::fs::{create_dir, read_dir, DirEntry};
use std::io;
use std::path::PathBuf;
use tantivy::collector::TopCollector;
use tantivy::query::FuzzyTermQuery;
use tantivy::schema::*;
use tantivy::ErrorKind;
use tantivy::{Error, Index, Result};

#[derive(Serialize, Debug, Clone)]
pub struct IndexCatalog {
    base_path: PathBuf,

    #[serde(skip_serializing)]
    collection: HashMap<String, Index>,
}

impl IndexCatalog {
    pub fn new(base_path: &PathBuf) -> io::Result<Self> {
        let mut index_cat = IndexCatalog {
            base_path:  base_path.clone(),
            collection: HashMap::new(),
        };
        for dir in read_dir(base_path)? {
            let entry = dir?.path();
            let pth: String = entry.to_str().unwrap().rsplit("/").take(1).collect();
            let idx = get_index(&pth, None).unwrap();
            index_cat.add_index(pth.clone(), idx);
        }
        Ok(index_cat)
    }

    pub fn add_index(&mut self, name: String, index: Index) { self.collection.insert(name, index); }
}

pub fn get_index(path: &str, schema: Option<&Schema>) -> Result<Index> {
    let p = PathBuf::from(path);
    if p.exists() {
        Index::open_in_dir(p)
    } else {
        if let Some(s) = schema {
            create_dir(p).unwrap();
            Index::create_in_dir(path, s.clone())
        } else {
            Err(Error::from_kind(ErrorKind::PathDoesNotExist(p)))
        }
    }
}

pub fn search_index(s: &Search) -> Result<Vec<Document>> {
    info!("Search: {:?}", s);
    let index = get_index(&SETTINGS.path, None)?;
    index.load_searchers()?;
    let searcher = index.searcher();
    let schema = index.schema();
    let field = schema.get_field(&s.field).unwrap();
    let term = Term::from_field_text(field, &s.term);
    let query = FuzzyTermQuery::new(term, 2, true);
    let mut collector = TopCollector::with_limit(s.limit);
    searcher.search(&query, &mut collector)?;

    Ok(collector.docs().into_iter().map(|d| searcher.doc(&d).unwrap()).collect())
}
