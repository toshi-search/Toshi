use std::fmt::Debug;

use raft::prelude::*;
use raft::{RaftState, RawNode, Result, Storage};

pub mod handle;
pub mod rpc_server;

pub struct RaftLocalState {
    pub hard_state: HardState,
    pub last_index: u64,
}

pub struct ToshiRaftStorage<S>
where
    S: Storage + Debug,
{
    storage: S,
    raft_state: RaftLocalState,
}

impl<S> Storage for ToshiRaftStorage<S>
where
    S: Storage + Debug,
{
    fn initial_state(&self) -> Result<RaftState> {
        self.storage.initial_state()
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        self.storage.entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        self.storage.term(idx)
    }

    fn first_index(&self) -> Result<u64> {
        self.storage.first_index()
    }

    fn last_index(&self) -> Result<u64> {
        self.storage.last_index()
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        self.storage.snapshot(request_index)
    }
}

impl<S> ToshiRaftStorage<S>
where
    S: Storage + Debug,
{
    #[inline]
    pub fn committed_index(&self) -> u64 {
        self.raft_state.hard_state.get_commit()
    }
}

pub struct ToshiPeer<S>
where
    S: Storage + Debug,
{
    peer_id: u64,
    raft_group: RawNode<ToshiRaftStorage<S>>,
    pending_messages: Vec<Message>,
}

impl<S> ToshiPeer<S>
where
    S: Storage + Debug,
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
    pub fn get_store(&self) -> &ToshiRaftStorage<S> {
        self.raft_group.store()
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    pub fn step(&mut self, mut m: Message) -> Result<()> {
        // Here we hold up MsgReadIndex. If current peer has valid lease, then we could handle the
        // request directly, rather than send a heartbeat to check quorum.
        let msg_type = m.get_msg_type();
        let committed = self.raft_group.raft.raft_log.committed;
        let expected_term = self.raft_group.raft.raft_log.term(committed).unwrap_or(0);
        if msg_type == MessageType::MsgReadIndex && expected_term == self.raft_group.status().hs.term {
            // If the leader hasn't committed any entries in its term, it can't response read only
            // requests. Please also take a look at raft-rs.

            let mut resp = Message::default();
            resp.set_msg_type(MessageType::MsgReadIndexResp);
            resp.term = self.term();
            resp.to = m.from;
            resp.index = self.get_store().committed_index();
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

    pub fn execute_transfer_leader(&mut self, msg: &Message) {
        if msg.get_log_term() != self.term() {
            return;
        }

        if self.is_leader() {
            let from = msg.get_from();
            self.raft_group.transfer_leader(from);
            return;
        }

        let mut msg = Message::default();
        msg.set_from(self.peer_id);
        msg.set_to(self.leader_id());
        msg.set_msg_type(MessageType::MsgTransferLeader);
        // msg.set_index(self.get_store().applied_index());
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);
    }
}
