use crate::index::IndexCatalog;

use futures::{Future, Stream};
use log::info;
use tokio::timer::Interval;

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

pub struct IndexWatcher {
    commit_duration: u64,
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl IndexWatcher {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>, commit_duration: u64) -> Self {
        IndexWatcher { catalog, commit_duration }
    }

    pub fn start(self) {
        let catalog = Arc::clone(&self.catalog);
        let task = Interval::new_interval(Duration::from_secs(self.commit_duration))
            .for_each(move |_| {
                if let Ok(mut cat) = catalog.write() {
                    cat.get_mut_collection().iter_mut().for_each(|(key, index)| {
                        let writer = index.get_writer();
                        let current_ops = index.get_opstamp();
                        if current_ops == 0 {
                            info!("No update to index={}, opstamp={}", key, current_ops);
                        } else if let Ok(mut w) = writer.lock() {
                            w.commit().unwrap();
                            index.set_opstamp(0);
                        }
                    });
                }
                Ok(())
            })
            .map_err(|e| panic!("Error in commit-watcher={:?}", e));

        tokio::spawn(task);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use futures::future;

    use crate::handlers::index::AddDocument;
    use crate::handlers::{IndexHandler, SearchHandler};
    use crate::index::tests::*;
    use tokio::runtime::Runtime;

    #[test]
    pub fn test_auto_commit() {
        let idx = create_test_index();
        let mut rt = Runtime::new().unwrap();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let arc = Arc::new(RwLock::new(catalog));
        let watcher = IndexWatcher::new(Arc::clone(&arc), 1);
        let handler = IndexHandler::new(Arc::clone(&arc));
        let search = SearchHandler::new(Arc::clone(&arc));

        let fut = future::lazy(|| {
            watcher.start();
            future::ok::<(), ()>(())
        });
        rt.spawn(fut);

        let body = r#"{ "document": { "test_text": "Babbaboo!", "test_u64": 10 , "test_i64": -10, "test_unindex": "asdf1234" } }"#;
        let add: AddDocument = serde_json::from_str(body).unwrap();
        handler.add(add, "test_index".into()).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        let docs = search.get_all_docs("test_index".into()).unwrap();
        println!("{}", docs.hits);
        assert_eq!(6, docs.hits);
        rt.shutdown_now();
    }
}
