use crate::error::Error;
use crate::index::IndexCatalog;

use std::sync::{Arc, RwLock};
use tower_web::*;

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
        fn handle(&self, index: String) -> Result<String, Error> {
            let index_lock = self.catalog.read().unwrap();
            if index_lock.exists(&index) {
                let index = index_lock.get_index(&index)?;
                let metas = index.get_index().load_metas()?;
                let payload = serde_json::to_string(&metas)?;
                Ok(payload)
            } else {
                Err(Error::IOError("Failed to obtain index lock".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::index::tests::*;

    #[test]
    fn get_summary_data() {
        let cat = create_test_catalog("test_index");
        let handler = SummaryHandler::new(Arc::clone(&cat));

        let resp = handler.handle("test_index".into());
        assert_eq!(resp.is_ok(), true)
    }

}
