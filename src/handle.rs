use crate::settings::Settings;
use crate::Result;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tantivy::{Index, IndexWriter};

pub struct IndexHandle {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    current_opstamp: AtomicUsize,
    settings: Settings,
    name: String,
}

impl IndexHandle {
    pub fn new(index: Index, settings: Settings, name: &str) -> Result<Self> {
        let i = index.writer(settings.writer_memory)?;
        i.set_merge_policy(settings.get_merge_policy());
        let current_opstamp = AtomicUsize::new(0);
        let writer = Arc::new(Mutex::new(i));
        Ok(Self {
            index,
            writer,
            current_opstamp,
            settings,
            name: name.into(),
        })
    }

    pub fn get_index(&self) -> &Index {
        &self.index
    }

    /// Returns the name of the Index
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn recreate_writer(self) -> Result<Self> {
        IndexHandle::new(self.index, self.settings.clone(), &self.name)
    }

    pub fn get_writer(&self) -> Arc<Mutex<IndexWriter>> {
        Arc::clone(&self.writer)
    }

    pub fn get_opstamp(&self) -> usize {
        self.current_opstamp.load(Ordering::Relaxed)
    }

    pub fn set_opstamp(&self, opstamp: usize) {
        self.current_opstamp.store(opstamp, Ordering::Relaxed)
    }
}
