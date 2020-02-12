use std::convert::TryInto;
use std::path::Path;

use bytes::BytesMut;
use prost::Message;
use raft::{RaftState, Storage, StorageError};
use raft::prelude::*;
use sled::{Db, open, IVec};
use slog::{info, Logger};
use raft::prelude::SnapshotMetadata;

use crate::state::SledRaftState;
use std::process::id;

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
    pub fn new_with_logger(path: &str, cfg: raft::Config, logger: Option<Logger>) -> Result<Self, SledStorageError> {
        if let Some(ref log) = logger {
            info!(log, "Init sled storage db at: {}", path);
        }
        let db = open(Path::new(path))?;
        let mut hard_state: HardState = Self::get(&db, b"hard_state")?;
        let mut conf_state: ConfState = Self::get(&db, b"conf_state")?;
        let last_idx_be = db.get(b"last_idx").unwrap_or(None);

        if !conf_state.voters.contains(&cfg.id) {
            conf_state.voters = vec![cfg.id];
        }
        let last_idx: u64 = if let Some(libe) = last_idx_be {
            read_be_u64(&mut libe.as_ref())
        } else { 1u64 };

        if !db.contains_key(b"hard_state")? {
            Self::insert(&db, b"hard_state", hard_state.clone())?;
        }
        if !db.contains_key(b"conf_state")? {
            Self::insert(&db, b"conf_state", conf_state.clone())?;
        }
        let state = SledRaftState::new(hard_state, conf_state);

        Ok(Self {
            snapshot_metadata: SnapshotMetadata::default(),
            state,
            db,
            logger,
            last_idx,
        })
    }

    pub fn get<K, V>(db: &Db, k: K) -> Result<V, SledStorageError>
        where K: AsRef<[u8]>,
              V: Message + Default {
        if let Ok(Some(v)) = db.get(k) {
            Ok(V::decode(v.as_ref())?)
        } else {
            Ok(V::default())
        }
    }

    pub fn insert<K, V>(db: &Db, k: K, v: V) -> Result<(), SledStorageError>
        where K: AsRef<[u8]>,
              V: Message {

        let mut buf = Vec::with_capacity(v.encoded_len());
        v.encode(&mut buf)?;
        db.insert(k, &buf[..])?;
        Ok(())
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

        Self::insert(&self.db, b"hard_state", self.state.hard_state.clone());
        Self::insert(&self.db, b"conf_state", self.state.conf_state.clone());
        let idx = self.last_idx.to_be_bytes();
        self.db.insert(b"last_idx", &idx[..])?;
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
        Ok(self.state.clone().into())
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>, raft::Error> {
        if low <= 0 || high > self.last_idx {
            return Ok(vec![]);
        }
        let lower = low.to_be_bytes();
        let upper = high.to_be_bytes();
        let tree = self.db.open_tree(b"entries").unwrap();
        let mut results: Vec<Entry> = vec![];
        for i in tree.range(lower..upper) {
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
        if idx == self.snapshot_metadata.index {
            return Ok(self.snapshot_metadata.term);
        }
        let idx_bytes = idx.to_be_bytes();
        let mut tree = self.db.open_tree("entries").unwrap();
        let term = if let Ok(Some(e)) = tree.get(idx_bytes) {
            let msg = Entry::decode(e.as_ref()).unwrap();
            msg.term
        } else { 1 };
        if let Some(log) = &self.logger {
            info!(log, "Term = {}, Idx = {}", msg.term, idx);
        }
        Ok(term)
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