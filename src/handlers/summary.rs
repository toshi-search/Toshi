use futures::future;

use std::io::Result as IOResult;

use super::super::Error;
use super::*;
use std::sync::RwLock;

#[derive(Clone, Debug)]
pub struct SummaryHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl SummaryHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self { SummaryHandler { catalog } }
}

impl Handler for SummaryHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let index_path = IndexPath::take_from(&mut state);
        let query_options = QueryOptions::take_from(&mut state);
        let index_lock = self.catalog.read().unwrap();

        if index_lock.exists(&index_path.index) {
            let index = index_lock.get_index(&index_path.index).unwrap();
            let metas = index.load_metas().unwrap();
            let payload = to_json(metas, query_options.pretty);
            let resp = create_response(&state, StatusCode::Ok, payload);
            Box::new(future::ok((state, resp)))
        } else {
            Box::new(handle_error(state, &Error::UnknownIndex(index_path.index)))
        }
    }
}

new_handler!(SummaryHandler);

#[cfg(test)]
mod tests {

    use super::*;
    use index::tests::*;

    #[test]
    fn get_summary_data() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));

        let req = client.get("http://localhost/test_index/_summary").perform().unwrap();

        assert_eq!(StatusCode::Ok, req.status());
    }

}
