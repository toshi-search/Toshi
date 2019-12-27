use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tracing::*;

use crate::index::SharedCatalog;

#[allow(irrefutable_let_patterns)]
pub async fn watcher(cat: SharedCatalog, commit_duration: f32, lock: Arc<AtomicBool>) -> Result<(), ()> {
    while let _ = time::interval(Duration::from_secs_f32(commit_duration)).tick().await {
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
    use hyper::Body;

    use toshi_test::read_body;

    use crate::handlers::{add_document, all_docs};
    use crate::index::tests::*;
    use crate::SearchResults;

    use super::*;

    #[tokio::test]
    pub async fn test_auto_commit() {
        let catalog = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));
        let watcher = watcher(Arc::clone(&catalog), 0.1, Arc::clone(&lock));

        tokio::spawn(watcher);

        let body = r#"{"document": { "test_text": "Babbaboo!", "test_u64": 10 , "test_i64": -10, "test_unindex": "asdf1234" } }"#;

        add_document(Arc::clone(&catalog), Body::from(body), "test_index".into())
            .await
            .unwrap();

        let expected = 6;
        for _ in 0..2 {
            let req = all_docs(Arc::clone(&catalog), "test_index".into()).await.unwrap();
            let body = read_body(req).await.unwrap();
            let docs: SearchResults = serde_json::from_slice(body.as_bytes()).unwrap();
            if docs.hits == expected {
                break;
            }
        }
    }
}
