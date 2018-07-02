use std::fs::create_dir;
use std::path::PathBuf;
use tantivy::collector::TopCollector;
use tantivy::query::FuzzyTermQuery;
use tantivy::schema::*;
use tantivy::{Index, Result, Error};
use tantivy::ErrorKind;

use handlers::Search;
use settings::SETTINGS;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexCatalog {
    base_path: PathBuf,
    collection: HashMap<String, PathBuf>
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
