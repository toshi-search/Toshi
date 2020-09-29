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
