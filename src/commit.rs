use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::{Future, Stream};
use tokio::timer::Interval;

use crate::index::IndexCatalog;

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
                if let Ok(cat) = catalog.read() {
                    cat.get_collection().into_iter().for_each(|(key, index)| {
                        let writer = index.get_writer();
                        let current_ops = index.get_opstamp();
                        if current_ops == 0 {
                            log::debug!("No update to index={}, opstamp={}", key, current_ops);
                        } else if let Ok(mut w) = writer.lock() {
                            log::debug!("Committing {}...", key);
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
    use futures::future;
    use hyper::Body;
    use tokio::runtime::Runtime;

    use crate::handlers::{IndexHandler, SearchHandler};
    use crate::index::tests::*;

    use super::*;

    #[test]
    pub fn test_auto_commit() {
        let mut rt = Runtime::new().unwrap();
        let catalog = create_test_catalog("test_index");

        let watcher = IndexWatcher::new(Arc::clone(&catalog), 1);
        let handler = IndexHandler::new(Arc::clone(&catalog));
        let search = SearchHandler::new(Arc::clone(&catalog));

        let fut = future::lazy(|| {
            watcher.start();
            future::ok::<(), ()>(())
        });
        rt.spawn(fut);

        let body = r#"{"document": { "test_text": "Babbaboo!", "test_u64": 10 , "test_i64": -10, "test_unindex": "asdf1234" } }"#;
        handler.add_document(Body::from(body), "test_index".into()).wait().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        let docs = search.all_docs("test_index".into()).wait();
        assert_eq!(true, docs.is_ok());
        assert_eq!(6, docs.unwrap().into_body().concat2().wait());
        rt.shutdown_now();
    }
}
