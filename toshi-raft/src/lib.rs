use std::convert::TryInto;
use std::path::Path;

use bytes::BytesMut;
use prost::Message;
use raft::{RaftState, Storage, StorageError};
use raft::prelude::*;
use sled::{Db, open};
use slog::{info, Logger};
use raft::prelude::SnapshotMetadata;

use crate::state::SledRaftState;

pub mod raft_node;
pub mod rpc_server;
pub mod state;

pub type SledStorageError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Error = SledStorageError;

fn read_be_u64(input: &mut &[u8]) -> u64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    *input = rest;
    u64::from_be_bytes(int_bytes.try_into().unwrap())
}

pub struct SledStorage {
    state: SledRaftState,
    snapshot_metadata: SnapshotMetadata,
    db: Db,
    logger: Option<Logger>,
    last_idx: u64,
}

impl SledStorage {

    pub fn new_with_logger(path: &str, cfg: Option<raft::Config>, logger: Option<Logger>) -> Result<Self, SledStorageError> {
        if let Some(ref log) = logger {
            info!(log, "Init sled storage db at: {}", path);
        }
        let state = SledRaftState::default();
        let db = open(Path::new(path))?;
        let last_idx = if let Ok(Some(v)) = db.get(b"last_idx") {
            read_be_u64(&mut v.as_ref())
        } else {
            1u64
        };

        Ok(Self {
            snapshot_metadata: SnapshotMetadata::default(),
            state,
            db,
            logger,
            last_idx,
        })
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<(), SledStorageError> {
        let entry_tree = self.db.open_tree("entries")?;

        for (i, e) in entries.iter().enumerate() {
            let idx = e.index.to_be_bytes();
            let mut b = BytesMut::with_capacity(e.encoded_len());
            e.encode(&mut b)?;
            entry_tree.insert(idx, &b[..])?;
            self.last_idx = i as u64;
        }
        let last_idx_be = self.last_idx.to_be_bytes();
        self.db.insert(b"last_idx", &last_idx_be)?;
        Ok(())
    }

    pub fn set_conf_state(&mut self, conf: ConfState) {
        self.state.conf_state = conf;
    }

    pub fn commit(&mut self, commit: u64) -> Result<(), SledStorageError> {
        let flush = self.db.flush()?;
        if let Some(ref log) = self.logger {
            info!(log, "Flushed: {} bytes ", flush);
        }
        Ok(())
    }

    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), SledStorageError> {
        let mut meta: SnapshotMetadata = snapshot.take_metadata();
        let term = meta.term;
        let index = meta.index;

        if self.first_index()? > index {
            return Err(toshi_types::Error::IOError("".into()).into());
        }

        self.snapshot_metadata = meta.clone();
        self.state.hard_state.term = term;
        self.state.hard_state.commit = index;
        self.state.conf_state = meta.take_conf_state();

        Ok(())
    }
}

impl Storage for SledStorage {

    fn initial_state(&self) -> Result<RaftState, raft::Error> {
        unimplemented!()
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>, raft::Error> {
        if low <= 0 || high > self.last_idx {
            return Ok(vec![]);
        }
        let lower = low.to_be_bytes();
        let upper = high.to_be_bytes();
        let mut results: Vec<Entry> = vec![];
        for i in self.db.range(lower..upper) {
            match i {
                Ok((k, v)) => {
                    let dec = Entry::decode(&v[..]).unwrap();
                    results.push(dec);
                }
                Err(e) => panic!(e),
            }
        }
        Ok(results)
    }

    fn term(&self, idx: u64) -> Result<u64, raft::Error> {
        unimplemented!()
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        Ok(1)
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        Ok(self.last_idx)
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot, raft::Error> {
        let mut snapshot = Snapshot::default();

        // Use the latest applied_idx to construct the snapshot.
        let applied_idx = self.state.hard_state.commit;
        let term = self.state.hard_state.term;
        let meta = snapshot.mut_metadata();
        meta.index = applied_idx;
        meta.term = term;

        meta.set_conf_state(self.state.conf_state.clone());
        Ok(snapshot)
    }
}