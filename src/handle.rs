use settings::SETTINGS;
use tantivy::{Index, IndexWriter};

use atomic::{Atomic, Ordering};
use std::sync::{Arc, Mutex};

pub struct IndexHandle {
    index:               Index,
    writer:              Arc<Mutex<IndexWriter>>,
    pub current_opstamp: Atomic<u64>,
}

impl IndexHandle {
    pub fn new(index: Index) -> Self {
        let i = index.writer(SETTINGS.writer_memory).unwrap();
        i.set_merge_policy(SETTINGS.get_merge_policy());
        let current_opstamp = Atomic::new(i.commit_opstamp());
        let writer = Arc::new(Mutex::new(i));
        Self {
            index,
            writer,
            current_opstamp,
        }
    }

    pub fn get_index(&self) -> &Index { &self.index }

    pub fn get_writer(&self) -> Arc<Mutex<IndexWriter>> { Arc::clone(&self.writer) }

    pub fn get_opstamp(&self) -> u64 { self.current_opstamp.load(Ordering::SeqCst) }

    pub fn set_opstamp(&self, opstamp: u64) {
        info!("Setting opstamp={}", opstamp);
        self.current_opstamp.store(opstamp, Ordering::SeqCst)
    }
}
