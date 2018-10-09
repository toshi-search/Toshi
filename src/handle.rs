use settings::SETTINGS;
use tantivy::{Index, IndexWriter};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct IndexHandle {
    index:               Index,
    writer:              Arc<Mutex<IndexWriter>>,
    current_opstamp: AtomicUsize,
}

impl IndexHandle {
    pub fn new(index: Index) -> Self {
        let i = index.writer(SETTINGS.writer_memory).unwrap();
        i.set_merge_policy(SETTINGS.get_merge_policy());
        let current_opstamp = AtomicUsize::new(0);
        let writer = Arc::new(Mutex::new(i));
        Self {
            index,
            writer,
            current_opstamp,
        }
    }

    pub fn get_index(&self) -> &Index { &self.index }

    pub fn recreate_writer(self) -> Self { IndexHandle::new(self.index) }

    pub fn get_writer(&self) -> Arc<Mutex<IndexWriter>> { Arc::clone(&self.writer) }

    pub fn get_opstamp(&self) -> usize { self.current_opstamp.load(Ordering::Relaxed) }

    pub fn set_opstamp(&self, opstamp: usize) { self.current_opstamp.store(opstamp, Ordering::Relaxed) }
}
