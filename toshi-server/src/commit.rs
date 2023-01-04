use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::trace;
use tokio::time;

use toshi_types::{Catalog, IndexHandle};

#[allow(irrefutable_let_patterns)]
pub async fn watcher<C: Catalog>(cat: Arc<C>, commit_duration: f32, lock: Arc<AtomicBool>) -> Result<(), ()> {
    while let _ = time::interval(Duration::from_secs_f32(commit_duration)).tick().await {
        for e in cat.get_collection().iter() {
            let (k, v) = e.pair();
            let writer = v.get_writer();
            let current_ops = v.get_opstamp();
            if current_ops == 0 {
                trace!("No update to index={}, opstamp={}", k, current_ops);
            } else if !lock.load(Ordering::SeqCst) {
                let mut w = writer.lock().await;
                trace!("Committing: {}...", k);
                w.commit().unwrap();
                v.set_opstamp(0);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use hyper::Body;

    use crate::handlers::{add_document, all_docs};
    use crate::index::create_test_catalog;
    use crate::SearchResults;

    use super::*;
    use http::Response;
    use serde::de::DeserializeOwned;
    use tantivy::schema::*;
    use tantivy::{doc, Index};

    pub fn create_test_index() -> Index {
        let mut builder = SchemaBuilder::new();
        let test_text = builder.add_text_field("test_text", STORED | TEXT);
        let test_int = builder.add_i64_field("test_i64", STORED | INDEXED);
        let test_unsign = builder.add_u64_field("test_u64", STORED | INDEXED);
        let test_unindexed = builder.add_text_field("test_unindex", STORED);
        let test_facet = builder.add_facet_field("test_facet", INDEXED | STORED);

        let schema = builder.build();
        let idx = Index::create_in_ram(schema);
        let mut writer = idx.writer(30_000_000).unwrap();

        writer.add_document(doc! { test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64, test_unindexed => "no", test_facet => Facet::from("/cat/cat2") }).unwrap();
        writer.add_document(doc! { test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64, test_unindexed => "yes", test_facet => Facet::from("/cat/cat2") }).unwrap();
        writer.add_document(doc! { test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64, test_unindexed => "noo", test_facet => Facet::from("/cat/cat3") }).unwrap();
        writer.add_document(doc! { test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64, test_unindexed => "yess", test_facet => Facet::from("/cat/cat4") }).unwrap();
        writer.add_document(doc! { test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64, test_unindexed => "nooo", test_facet => Facet::from("/dog/cat2") }).unwrap();
        writer.commit().unwrap();

        idx
    }

    pub async fn wait_json<T: DeserializeOwned>(r: Response<Body>) -> T {
        let bytes = read_body(r).await.unwrap();
        serde_json::from_slice::<T>(bytes.as_bytes()).unwrap_or_else(|e| panic!("Could not deserialize JSON: {:?}", e))
    }

    pub fn cmp_float(a: f32, b: f32) -> bool {
        let abs_a = a.abs();
        let abs_b = b.abs();
        let diff = (a - b).abs();
        if diff == 0.0 {
            return true;
        } else if a == 0.0 || b == 0.0 || (abs_a + abs_b < f32::MIN_POSITIVE) {
            return diff < (f32::EPSILON * f32::MIN_POSITIVE);
        }
        diff / (abs_a + abs_b).min(f32::MAX) < f32::EPSILON
    }

    pub async fn read_body(resp: Response<Body>) -> Result<String, Box<dyn std::error::Error>> {
        let b = hyper::body::to_bytes(resp.into_body()).await?;
        Ok(String::from_utf8(b.to_vec())?)
    }

    #[tokio::test]
    pub async fn test_auto_commit() {
        let catalog = create_test_catalog("test_index");
        let lock = Arc::new(AtomicBool::new(false));
        let watcher = watcher(Arc::clone(&catalog), 0.1, Arc::clone(&lock));

        tokio::spawn(watcher);

        let body = r#"{"document": { "test_text": "Babbaboo!", "test_u64": 10 , "test_i64": -10, "test_unindex": "asdf1234" } }"#;

        add_document(Arc::clone(&catalog), Body::from(body), "test_index").await.unwrap();

        let expected = 6;
        for _ in 0..2 {
            let req = all_docs(Arc::clone(&catalog), "test_index").await.unwrap();
            let body = read_body(req).await.unwrap();
            let docs: SearchResults = serde_json::from_slice(body.as_bytes()).unwrap();
            if docs.hits == expected {
                break;
            }
        }
    }
}
