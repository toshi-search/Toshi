use std::collections::HashMap;
use std::fs;
use std::iter::Iterator;
use std::path::PathBuf;

use log::info;
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::Index;

use crate::handle::{IndexHandle, LocalIndexHandle};
use crate::query::Request;
use crate::results::*;
use crate::settings::Settings;
use crate::{Error, Result};

pub struct IndexCatalog {
    pub settings: Settings,
    base_path: PathBuf,
    collection: HashMap<String, LocalIndexHandle>,
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
        let new_index = LocalIndexHandle::new(index, Settings::default(), &name)
            .unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        map.insert(name, new_index);
        Ok(IndexCatalog {
            settings: Settings::default(),
            base_path: PathBuf::new(),
            collection: map,
        })
    }

    pub fn create_from_managed(mut base_path: PathBuf, index_path: &str, schema: Schema) -> Result<Index> {
        base_path.push(index_path);
        if !base_path.exists() {
            fs::create_dir(&base_path).map_err(|e| Error::IOError(e.to_string()))?;
        }
        let dir = MmapDirectory::open(base_path).map_err(|e| Error::IOError(e.to_string()))?;
        Index::open_or_create(dir, schema).map_err(|e| Error::IOError(e.to_string()))
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

    pub fn add_index(&mut self, name: String, index: Index) -> Result<()> {
        let handle = LocalIndexHandle::new(index, self.settings.clone(), &name)?;
        self.collection.entry(name).or_insert(handle);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, LocalIndexHandle> {
        &self.collection
    }

    pub fn get_mut_collection(&mut self) -> &mut HashMap<String, LocalIndexHandle> {
        &mut self.collection
    }

    pub fn exists(&self, index: &str) -> bool {
        self.get_collection().contains_key(index)
    }

    pub fn get_mut_index(&mut self, name: &str) -> Result<&mut LocalIndexHandle> {
        self.collection.get_mut(name).ok_or_else(|| Error::UnknownIndex(name.to_string()))
    }

    pub fn get_index(&self, name: &str) -> Result<&LocalIndexHandle> {
        self.collection.get(name).ok_or_else(|| Error::UnknownIndex(name.to_string()))
    }

    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.collection.clear();

        for dir in fs::read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            if let Some(entry_str) = entry.to_str() {
                if !entry_str.ends_with(".node_id") {
                    let pth: String = entry_str.rsplit('/').take(1).collect();
                    let idx = IndexCatalog::load_index(entry_str)?;
                    self.add_index(pth.clone(), idx)?;
                }
            } else {
                return Err(Error::IOError(format!("Path {} is not a valid unicode path", entry.display())));
            }
        }
        Ok(())
    }

    pub fn search_index(&self, index: &str, search: Request) -> Result<SearchResults> {
        match self.get_index(index) {
            Ok(hand) => hand.search_index(search),
            Err(e) => Err(e),
        }
    }

    pub fn clear(&mut self) {
        self.collection.clear();
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::{Arc, RwLock};

    use tantivy::doc;
    use tantivy::schema::*;

    use super::*;

    pub fn create_test_catalog(name: &str) -> Arc<RwLock<IndexCatalog>> {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index(name.into(), idx).unwrap();
        Arc::new(RwLock::new(catalog))
    }

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
}
