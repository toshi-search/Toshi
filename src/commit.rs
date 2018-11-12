use super::*;
use index::IndexCatalog;

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::prelude::*;
use tokio::runtime::{Builder as RtBuilder, Runtime};
use tokio::timer::Interval;

pub struct IndexWatcher {
    commit_duration: u64,
    catalog: Arc<RwLock<IndexCatalog>>,
    runtime: Runtime,
}

impl IndexWatcher {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>, commit_duration: u64) -> Self {
        let runtime = RtBuilder::new()
            .core_threads(2)
            .name_prefix("toshi-index-committer")
            .build()
            .unwrap();
        IndexWatcher {
            catalog,
            runtime,
            commit_duration,
        }
    }

    pub fn start(mut self) {
        let catalog = Arc::clone(&self.catalog);
        let task = Interval::new(Instant::now(), Duration::from_secs(self.commit_duration))
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
            }).map_err(|e| panic!("Error in commit-watcher={:?}", e));
        self.runtime.spawn(future::lazy(|| task));
        self.runtime.shutdown_on_idle();
    }

    pub fn shutdown(self) {
        self.runtime.shutdown_now();
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

    use mime;
    use serde_json;

    #[test]
    pub fn test_auto_commit() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let arc = Arc::new(RwLock::new(catalog));
        let test_server = create_test_client(&arc);
        let watcher = IndexWatcher::new(Arc::clone(&arc), 1);
        watcher.start();

        let body = r#"
        {
          "document": {
            "test_text":    "Babbaboo!",
            "test_u64":     10 ,
            "test_i64":     -10,
            "test_unindex": "asdf1234"
          }
        }"#;

        let response = test_server
            .put("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        sleep(Duration::from_secs(2));

        let check_request = create_test_client(&arc)
            .get("http://localhost/test_index?pretty=true")
            .perform()
            .unwrap();

        let body = check_request.read_body().unwrap();
        let results: TestResults = serde_json::from_slice(&body).unwrap();
        assert_eq!(6, results.hits);
    }

}
