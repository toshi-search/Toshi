use futures::{Future, Stream};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::timer::Interval;

use super::*;
use index::IndexCatalog;

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
    use handlers::search::tests::*;
    use hyper::StatusCode;
    use index::tests::*;
    use std::thread::sleep;
    use std::time::Duration;

    use futures::future;
    use serde_json;

    #[test]
    pub fn test_auto_commit() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let arc = Arc::new(RwLock::new(catalog));
        let watcher = IndexWatcher::new(Arc::clone(&arc), 1);

        let fut = future::lazy(|| {
            watcher.start();
            future::ok::<(), ()>(())
        });

        let body = r#"
            {
              "document": {
                "test_text":    "Babbaboo!",
                "test_u64":     10 ,
                "test_i64":     -10,
                "test_unindex": "asdf1234"
              }
            }"#;
        //        assert_eq!(6, results.hits);
    }
}
