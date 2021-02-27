use std::sync::Arc;

use raft::prelude::*;
use raft::Result;
use serde_json::Value;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Index, IndexWriter};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use toshi_types::Result as ToshiResult;
use toshi_types::*;

use crate::proposal::Proposal;

#[derive(Clone, Debug)]
pub struct RaftHandle<T>
where
    T: IndexHandle + Send + Sync,
{
    handle: Arc<T>,
    rt: Arc<Runtime>,
    raft_state: Option<RaftState>,
    prop_chan: Arc<Sender<Proposal>>,
}

impl<T: IndexHandle + Send + Sync> RaftHandle<T> {
    pub fn new(handle: T, prop_chan: Arc<Sender<Proposal>>) -> Self {
        Self {
            handle: Arc::new(handle),
            rt: Arc::new(Runtime::new().unwrap()),
            raft_state: None,
            prop_chan,
        }
    }
}

impl<T> Storage for RaftHandle<T>
where
    T: IndexHandle + Send + Sync,
{
    fn initial_state(&self) -> Result<RaftState> {
        if let Some(ref rs) = self.raft_state {
            Ok(rs.clone())
        } else {
            let rs = RaftState::new(HardState::default(), ConfState::default());
            Ok(rs)
        }
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let range = RangeQuery::builder().for_field("_id").gte(low).lte(high).build();
        let diff = high - low;
        let max = max_size.into().unwrap_or(diff) as usize;
        let search = Search::builder().with_query(range).with_limit(max).build();
        let result: Vec<Entry> = self
            .rt
            .block_on(Box::pin(self.search_index(search)))
            .unwrap_or_else(|_| panic!("Getting Documents for {}", self.get_name()))
            .get_docs()
            .iter()
            .flat_map(|doc| serde_json::to_string(&doc))
            .map(|doc| {
                let mut ent = Entry::new_();
                ent.set_data(doc.into_bytes());
                ent
            })
            .collect();

        Ok(result)
    }

    fn term(&self, _idx: u64) -> Result<u64> {
        Ok(self.raft_state.as_ref().map(|rs| rs.hard_state.term).unwrap_or(_idx))
    }

    fn first_index(&self) -> Result<u64> {
        Ok(1)
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.handle.get_opstamp() as u64)
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
        self.handle.get_name()
    }

    fn index_location(&self) -> IndexLocation {
        self.handle.index_location()
    }

    fn get_index(&self) -> Index {
        self.handle.get_index()
    }

    fn get_writer(&self) -> Arc<Mutex<IndexWriter>> {
        self.handle.get_writer()
    }

    fn get_space(&self) -> SearcherSpaceUsage {
        self.handle.get_space()
    }

    fn get_opstamp(&self) -> usize {
        self.handle.get_opstamp()
    }

    fn set_opstamp(&self, opstamp: usize) {
        self.handle.set_opstamp(opstamp);
    }

    async fn commit(&self) -> std::result::Result<u64, toshi_types::Error> {
        self.commit().await
    }

    async fn search_index(&self, search: Search) -> ToshiResult<SearchResults<FlatNamedDocument>> {
        self.handle.search_index(search).await
    }

    async fn add_document(&self, doc: AddDocument<Value>) -> ToshiResult<()> {
        // self.prop_chan.send()
        self.handle.add_document(doc).await
    }

    async fn delete_term(&self, term: DeleteDoc) -> ToshiResult<DocsAffected> {
        self.handle.delete_term(term).await
    }
}
