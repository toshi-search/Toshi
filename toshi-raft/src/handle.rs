use std::sync::Arc;

use raft::prelude::*;
use raft::Result;

use tokio::runtime::Runtime;

use toshi_types::IndexHandle;

#[derive(Clone, Debug)]
pub struct RaftHandle<T>
where
    T: IndexHandle,
{
    handle: T,
    rt: Arc<Runtime>,
}

impl<T> Storage for RaftHandle<T>
where
    T: IndexHandle,
{
    fn initial_state(&self) -> Result<RaftState> {
        unimplemented!()
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        unimplemented!()
        // let index = self.handle.get_index();
        // let searcher = index.reader().unwrap().searcher();
        // let max = (high - low) as usize;
        // let field = index.schema().get_field("_id").unwrap();
        //
        // // let query = RangeQuery::new_u64(field, low..high);
        // // let collector = TopDocs::with_limit(max_size.into().unwrap_or(max) as usize);
        // let q = RangeQuery::builder().for_field("_id").gte(low).lte(high).build();
        // let search = self.handle.search_index(Search::new(Some(q), None, max, None));
        // let result = Arc::clone(&self.rt).block_on(search);
        //
        // if let Ok(docs) = result {
        //     let result = docs
        //         .get_docs()
        //         .into_iter()
        //         .map(|d| Entry {
        //             entry_type: 0,
        //             term: 0,
        //             index: 0,
        //             data: serde_json::to_string(&d.doc).unwrap().into_bytes(),
        //             context: self.handle.get_name().into_bytes(),
        //
        //             sync_log: false,
        //         })
        //         .collect();
        //     Ok(result)
        // } else {
        //     Err(raft::Error::Store(StorageError::Unavailable))
        // }
    }

    fn term(&self, idx: u64) -> Result<u64> {
        unimplemented!()
    }

    fn first_index(&self) -> Result<u64> {
        unimplemented!()
    }

    fn last_index(&self) -> Result<u64> {
        unimplemented!()
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        unimplemented!()
    }
}
