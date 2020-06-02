use std::convert::TryInto;
use std::path::Path;

use bytes::BytesMut;
use prost::Message;
use raft::prelude::*;
use raft::{RaftState, Storage};
use sled::{open, Db};
use slog::{info, Logger};

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

pub fn decode<V>(v: &[u8]) -> Result<V, SledStorageError>
where
    V: Message + Default,
{
    Ok(V::decode(v)?)
}

pub fn encode<V>(v: V) -> Result<BytesMut, SledStorageError>
where
    V: Message,
{
    let mut buf = bytes::BytesMut::with_capacity(v.encoded_len());
    V::encode(&v, &mut buf)?;
    Ok(buf)
}

pub fn get<K, V>(db: &Db, k: K) -> Result<V, SledStorageError>
where
    K: AsRef<[u8]>,
    V: Message + Default,
{
    if let Ok(Some(v)) = db.get(k) {
        decode(v.as_ref())
    } else {
        Ok(V::default())
    }
}

pub fn insert<K, V>(db: &Db, k: K, v: V) -> Result<(), SledStorageError>
where
    K: AsRef<[u8]>,
    V: Message,
{
    let buf = encode(v)?;
    db.insert(k, &buf[..])?;
    Ok(())
}

/// toshi-raft - Toshi's raft consensus implementation
/// The structure of Toshi's raft implementation is as follows
/// root {
///   last_idx: u64
///   hard_state {
///     term: u64
///     vote: u64
///     commit: u64
///   }
///   conf_state {
///     voters: Vec<u64>
///     learners: Vec<u64>
///     voters_outgoing: Vec<u64>
///     listeners_next: Vec<u64>
///     auto_leave: bool
///   }
///   entries [
///     1: Entry {
///       entry_type: i32
///       term: u64
///       index: u64
///       data: Vec<u8>
///       context: Vec<u8>
///       sync_log: bool
///     }
///     2: Entry { ... }
///     n: Entry { ... }
///   ]
/// }

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
        let mut hard_state: HardState = get(&db, b"hard_state")?;
        let mut conf_state: ConfState = get(&db, b"conf_state")?;
        let last_idx_be = db.get(b"last_idx").unwrap_or(None);

        if !conf_state.voters.contains(&cfg.id) {
            conf_state.voters = vec![cfg.id];
        }
        let last_idx: u64 = if let Some(libe) = last_idx_be {
            read_be_u64(&mut libe.as_ref())
        } else {
            hard_state.commit
        };

        if hard_state.commit != last_idx {
            hard_state.commit = last_idx;
        }

        if !db.contains_key(b"hard_state")? {
            insert(&db, b"hard_state", hard_state.clone())?;
        }
        if !db.contains_key(b"conf_state")? {
            insert(&db, b"conf_state", conf_state.clone())?;
        }

        let state = SledRaftState::new(hard_state, conf_state);
        if let Some(ref log) = logger {
            info!(log, "Initial HardState = {:?}", state.hard_state);
            info!(log, "Initial ConfState = {:?}", state.conf_state);
            info!(log, "Last IDX = {:?}", last_idx);
            info!(log, "Initial DB = {:?}", db);
        }
        Ok(Self {
            snapshot_metadata: SnapshotMetadata::default(),
            state,
            db,
            logger,
            last_idx,
        })
    }

    pub fn append_entry(&mut self, entry: Entry) -> Result<(), SledStorageError> {
        if e.data.is_empty() || e.context.is_empty() {
            return Ok(());
        }

        let entry_tree = self.db.open_tree("entries")?;
        let idx = entry.index.to_be_bytes();
        let b = encode(entry)?;
        entry_tree.insert(idx, &b[..])?;
        self.last_idx = i as u64;

        let last_idx_be = self.last_idx.to_be_bytes();
        self.db.insert(b"last_idx", &last_idx_be)?;
        Ok(())
    }

    pub fn append_entries(&mut self, entries: &[Entry]) -> Result<(), SledStorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        for e in entries {
            self.append_entry(e.clone());
        }
        Ok(())
    }

    pub fn set_conf_state(&mut self, conf: ConfState) {
        self.state.conf_state = conf;
    }

    pub fn commit(&mut self) -> Result<(), SledStorageError> {
        if let Some(ref log) = self.logger {
            info!(log, "Commit HardState = {:?}", self.state.hard_state);
            info!(log, "Commit ConfState = {:?}", self.state.conf_state);
        }

        insert(&self.db, b"hard_state", self.state.hard_state.clone())?;
        insert(&self.db, b"conf_state", self.state.conf_state.clone())?;
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
            return Err(toshi_types::Error::IOError(std::io::Error::from_raw_os_error(1)).into());
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
        if high > self.last_idx {
            return Ok(vec![]);
        }
        let max: u64 = max_size.into().unwrap_or(high - low);
        let lower = low.to_be_bytes();
        let upper = max.to_be_bytes();
        let tree = self.db.open_tree(b"entries").unwrap();
        let mut results = vec![];
        for i in tree.range(lower..upper) {
            match i {
                Ok((_, v)) => {
                    let dec: Entry = decode(v.as_ref()).unwrap();
                    results.push(dec);
                }
                Err(e) => panic!(e),
            }
        }
        Ok(results.into_iter().take(max as usize).collect())
    }

    fn term(&self, idx: u64) -> Result<u64, raft::Error> {
        if idx == self.snapshot_metadata.index {
            return Ok(self.snapshot_metadata.term);
        }
        let idx_bytes = idx.to_be_bytes();
        let tree = self.db.open_tree("entries").unwrap();
        let term = if let Ok(Some(e)) = tree.get(idx_bytes) {
            let msg = Entry::decode(e.as_ref()).unwrap();
            msg.term
        } else {
            1
        };
        if let Some(log) = &self.logger {
            info!(log, "Term = {}, Idx = {}", term, idx);
        }
        Ok(term)
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        Ok(1)
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        Ok(self.last_idx)
    }

    fn snapshot(&self, _request_index: u64) -> Result<Snapshot, raft::Error> {
        let mut snapshot = Snapshot::default();
        let applied_idx = self.state.hard_state.commit;
        let term = self.state.hard_state.term;
        let meta = snapshot.mut_metadata();
        meta.index = applied_idx;
        meta.term = term;

        meta.set_conf_state(self.state.conf_state.clone());
        Ok(snapshot)
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::rpc_server::tests::create_test_catalog;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    pub fn test_storage() -> SledStorage {
        SledStorage::new_with_logger("./test_storage", Config::default(), None).unwrap()
    }

    fn test_entries() -> TestResult {
        let mut storage = test_storage();

        let entry = Entry {
            entry_type: 1,
            term: 1,
            index: 1,
            data: br#"{"asdf": 1}"#.to_vec(),
            context: br#"test_index"#.to_vec(),
            sync_log: false,
        };

        storage.append_entry(entry)?;
        storage.commit()?;
        Ok(())
    }

    #[test]
    pub fn test_last_idx() -> TestResult {
        let mut storage = test_storage();
        let _test_cat = create_test_catalog("test_index");

        assert_eq!(storage.last_index()?, 0);

        let entry = Entry::default();

        storage.append_entries(&[entry])?;
        storage.commit()?;
        assert_eq!(storage.last_index()?, 0);
        remove_dir_all::remove_dir_all("./test_storage").map_err(Into::into)
    }
}
