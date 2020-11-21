use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Mutex;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use prost::Message;
use raft::eraftpb::Message as RaftMessage;
use raft::prelude::*;

use proposal::Proposal;
use tokio::runtime::Runtime;
use toshi_types::{AddDocument, IndexHandle, IndexOptions, RangeQuery, Search};

pub mod handle;
pub mod proposal;
pub mod rpc_server;
pub mod rpc_utils;

pub type BoxErr = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, BoxErr>;

#[derive(Clone, Debug)]
pub struct RaftLocalState {
    pub hard_state: HardState,
    pub last_index: u64,
}

#[derive(Clone, Debug)]
pub struct ToshiRaftStorage<S>
where
    S: IndexHandle,
{
    storage: Arc<RwLock<S>>,
    raft_state: RaftLocalState,
    rt: Arc<Runtime>,
}

impl<S> Storage for ToshiRaftStorage<S>
where
    S: IndexHandle,
{
    fn initial_state(&self) -> raft::Result<RaftState> {
        // self.rl().initial_state()
        unimplemented!()
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> raft::Result<Vec<Entry>> {
        let range = RangeQuery::builder().for_field("_id").gte(low).lte(high).build();
        let diff = high - low;
        let search = Search::builder()
            .with_query(range)
            .with_limit(max_size.into().unwrap_or(diff) as usize)
            .build();
        let result: Vec<Entry> = self
            .rt
            .block_on(Box::pin(self.rl().search_index(search)))
            .unwrap()
            .get_docs()
            .into_iter()
            .map(|doc| serde_json::to_string(&doc).unwrap())
            .map(|doc| {
                let mut ent = Entry::new_();
                ent.set_data(doc.into_bytes());
                ent
            })
            .collect();

        raft::Result::Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        unimplemented!()
        // self.raft_state.hard_state.get_term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        unimplemented!()

        // self.rl().first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        unimplemented!()

        // self.rl().last_index()
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        unimplemented!()

        // self.rl().snapshot(request_index)
    }
}

impl<S> ToshiRaftStorage<S>
where
    S: IndexHandle,
{
    #[inline]
    pub fn committed_index(&self) -> u64 {
        self.raft_state.hard_state.get_commit()
    }

    pub fn wl(&self) -> RwLockWriteGuard<'_, S> {
        self.storage.write().unwrap()
    }

    pub fn rl(&self) -> RwLockReadGuard<'_, S> {
        self.storage.read().unwrap()
    }
}

pub struct ToshiPeer<H>
where
    H: IndexHandle,
{
    peer_id: u64,
    raft_group: RawNode<ToshiRaftStorage<H>>,
    pending_messages: Vec<RaftMessage>,
    proposals: Arc<Mutex<VecDeque<Proposal>>>,
}

impl<H> ToshiPeer<H>
where
    H: IndexHandle,
{
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.status().id == self.peer_id
    }

    #[inline]
    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    pub async fn run(&mut self) -> Result<()> {
        if !self.raft_group.has_ready() {
            return Ok(());
        }

        let mut ready = self.raft_group.ready();

        // if let Err(e) = self.raft_group.store().wl().append(ready.entries()) {
        //     log::error!("persist raft log fail: {:?}, need to retry or panic", e);
        //     return Ok(());
        // }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let _s = ready.snapshot().clone();
            // Apply the snapshot here.
        }

        // Send out the messages come from the node.
        for msg in ready.messages.drain(..) {
            let _to = msg.to;
            // Send messages to other nodes...
        }

        // Apply all committed proposals.
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    if let Err(_e) = cc.merge(&*entry.data) {}
                    let cs = self.raft_group.apply_conf_change(&cc)?;
                // self.raft_group.store().wl().set_conf_state(cs)?;
                } else {
                    // let doc = serde_json::from_slice(&entry.data).unwrap();
                    let commit = Some(IndexOptions { commit: true });
                    // self.index_handle.add_document(AddDocument::new(doc, commit)).await?;
                }
                if self.is_leader() {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    let proposals = self.proposals.lock().unwrap().pop_front().unwrap();
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                let mut s = self.raft_group.store().wl();
                // s.mut_hard_state().commit = last_committed.index;
                // s.mut_hard_state().term = last_committed.term;
            }
        }
        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group.advance(ready);
        Ok(())
    }

    pub fn step(&mut self, mut m: RaftMessage) -> Result<()> {
        // Here we hold up MsgReadIndex. If current peer has valid lease, then we could handle the
        // request directly, rather than send a heartbeat to check quorum.
        let msg_type = m.get_msg_type();
        let committed = self.raft_group.raft.raft_log.committed;
        let expected_term = self.raft_group.raft.raft_log.term(committed).unwrap_or(0);
        if msg_type == MessageType::MsgReadIndex && expected_term == self.raft_group.status().hs.term {
            // If the leader hasn't committed any entries in its term, it can't response read only
            // requests. Please also take a look at raft-rs.

            let mut resp = RaftMessage::default();
            resp.set_msg_type(MessageType::MsgReadIndexResp);
            resp.term = self.term();
            resp.to = m.from;
            resp.index = self.raft_group.store().committed_index();
            resp.set_entries(m.take_entries());

            self.pending_messages.push(resp);
            return Ok(());
        }
        if msg_type == MessageType::MsgTransferLeader {
            self.execute_transfer_leader(&m);
            return Ok(());
        }

        self.raft_group.step(m)?;
        Ok(())
    }

    pub fn execute_transfer_leader(&mut self, msg: &RaftMessage) {
        if msg.get_log_term() != self.term() {
            return;
        }

        if self.is_leader() {
            let from = msg.get_from();
            self.raft_group.transfer_leader(from);
            return;
        }

        let mut msg = RaftMessage::default();
        msg.set_from(self.peer_id);
        msg.set_to(self.leader_id());
        msg.set_msg_type(MessageType::MsgTransferLeader);
        // msg.set_index(self.get_store().applied_index());
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);
    }
}
