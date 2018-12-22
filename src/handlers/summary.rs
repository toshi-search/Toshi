use crate::index::IndexCatalog;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct SummaryHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl SummaryHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        SummaryHandler { catalog }
    }
}

impl_web! {
    impl SummaryHandler {
        #[get("/:index/_summary")]
        #[content_type("application/json")]
        fn handle(&self, index: String) -> Result<String, ()> {
            let index_lock = self.catalog.read().unwrap();
            if index_lock.exists(&index) {
                let index = index_lock.get_index(&index).map_err(|_| ())?;
                let metas = index.get_index().load_metas().map_err(|_| ())?;
                let payload = serde_json::to_string(&metas).map_err(|_| ())?;
                Ok(payload)
            } else {
                Err(())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use index::tests::*;

    #[test]
    fn get_summary_data() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = Arc::new(RwLock::new(catalog));

        //        assert_eq!(hyper::StatusCode::OK, req.status());
    }

}
