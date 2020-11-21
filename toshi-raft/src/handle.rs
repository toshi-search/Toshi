use std::sync::Arc;

use raft::prelude::*;
use raft::Result;

use tokio::runtime::Runtime;

use serde_json::Value;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Index, IndexWriter};
use tokio::sync::Mutex;
use toshi_types::{AddDocument, DeleteDoc, DocsAffected, FlatNamedDocument, IndexHandle, IndexLocation, Search, SearchResults};

#[derive(Clone, Debug)]
pub struct RaftHandle<T>
where
    T: IndexHandle + Send + Sync,
{
    handle: Arc<T>,
    rt: Arc<Runtime>,
}

impl<T> Storage for RaftHandle<T>
where
    T: IndexHandle + Send + Sync,
{
    fn initial_state(&self) -> Result<RaftState> {
        unimplemented!()
    }

    fn entries(&self, _low: u64, _high: u64, _max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        unimplemented!()
    }

    fn term(&self, _idx: u64) -> Result<u64> {
        unimplemented!()
    }

    fn first_index(&self) -> Result<u64> {
        unimplemented!()
    }

    fn last_index(&self) -> Result<u64> {
        unimplemented!()
    }

    fn snapshot(&self, _request_index: u64) -> Result<Snapshot> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl<T> IndexHandle for RaftHandle<T>
where
    T: IndexHandle + Send + Sync,
{
    fn get_name(&self) -> String {
        unimplemented!()
    }

    fn index_location(&self) -> IndexLocation {
        unimplemented!()
    }

    fn get_index(&self) -> Index {
        unimplemented!()
    }

    fn get_writer(&self) -> Arc<Mutex<IndexWriter>> {
        unimplemented!()
    }

    fn get_space(&self) -> SearcherSpaceUsage {
        unimplemented!()
    }

    fn get_opstamp(&self) -> usize {
        unimplemented!()
    }

    fn set_opstamp(&self, opstamp: usize) {
        unimplemented!()
    }

    async fn search_index(&self, search: Search) -> std::result::Result<SearchResults<FlatNamedDocument>, toshi_types::Error> {
        self.handle.search_index(search).await
    }

    async fn add_document(&self, doc: AddDocument<Value>) -> std::result::Result<(), toshi_types::Error> {
        unimplemented!()
    }

    async fn delete_term(&self, term: DeleteDoc) -> std::result::Result<DocsAffected, toshi_types::Error> {
        unimplemented!()
    }
}
