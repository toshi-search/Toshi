use std::path::Path;

use raft::{RaftState, Storage};
use raft::prelude::*;
use sled::{open, Db};
use slog::{Logger, info};

use crate::state::SledRaftState;
use std::str::FromStr;

pub mod raft_node;
pub mod rpc_server;
pub mod state;

pub type SledStorageError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Error = SledStorageError;

pub struct SledStorage {
    state: SledRaftState,
    db: Db,
    logger: Option<Logger>
}

impl SledStorage {
    pub fn new_with_logger(path: &str, cfg: Option<raft::Config>, logger: Option<Logger>) -> Result<Self, SledStorageError> {
        if let Some(ref log) = logger {
            info!(log, "Init sled storage db at: {}", path);
        }
        let mut state = SledRaftState::default();
        let db = open(Path::new(path))?;
        Ok(Self {
            state,
            db,
            logger
        })
    }
    pub fn append(&mut self, entries: &[Entry]) -> Result<(), SledStorageError> {
        unimplemented!()
    }
    pub fn set_conf_state(&mut self, conf: ConfState) {
        unimplemented!()
    }
    pub fn append_single(&mut self, entry: Entry) -> Result<(), SledStorageError> {
        unimplemented!()
    }
    pub fn commit(&mut self, commit: u64) -> Result<(), SledStorageError> {
        unimplemented!()
    }
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), SledStorageError> {
        unimplemented!()
    }
}

impl Storage for SledStorage {
    fn initial_state(&self) -> Result<RaftState, raft::Error> {
        unimplemented!()
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>, raft::Error> {
        unimplemented!()
    }

    fn term(&self, idx: u64) -> Result<u64, raft::Error> {
        unimplemented!()
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        unimplemented!()
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        unimplemented!()
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot, raft::Error> {
        unimplemented!()
    }
}