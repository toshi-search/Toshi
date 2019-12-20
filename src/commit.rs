use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tracing::*;

use crate::index::SharedCatalog;

#[allow(irrefutable_let_patterns)]
pub async fn watcher(cat: SharedCatalog, commit_duration: u64, lock: Arc<AtomicBool>) -> Result<(), ()> {
    while let _ = time::interval(Duration::from_secs(commit_duration)).tick().await {
        let cat = cat.lock().await;
        for (key, index) in cat.get_collection().into_iter() {
            let writer = index.get_writer();
            let current_ops = index.get_opstamp();
            if current_ops == 0 {
                debug!("No update to index={}, opstamp={}", key, current_ops);
            } else if !lock.load(Ordering::SeqCst) {
                let mut w = writer.lock().await;
                debug!("Committing {}...", key);
                w.commit().unwrap();
                index.set_opstamp(0);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use bytes::Buf;
    use hyper::Body;
    use tokio::runtime::Builder;

    use crate::handlers::{add_document, all_docs};
    use crate::index::tests::*;
    use crate::SearchResults;

    use super::*;

    #[test]
    pub fn test_auto_commit() {
        let mut rt = Builder::new().threaded_scheduler().core_threads(4).enable_all().build().unwrap();
        let catalog = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));
        let watcher = watcher(Arc::clone(&catalog), 1, Arc::clone(&lock));

        rt.spawn(watcher);

        let body = r#"{"document": { "test_text": "Babbaboo!", "test_u64": 10 , "test_i64": -10, "test_unindex": "asdf1234" } }"#;
        rt.block_on(add_document(Arc::clone(&catalog), Body::from(body), "test_index".into()))
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        let req = rt.block_on(all_docs(Arc::clone(&catalog), "test_index".into())).unwrap();
        let body = rt.block_on(hyper::body::aggregate(req.into_body())).unwrap();
        let docs: SearchResults = serde_json::from_slice(body.bytes()).unwrap();

        assert_eq!(6, docs.hits);
    }
}
