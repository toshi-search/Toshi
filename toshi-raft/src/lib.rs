use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use http::Uri;
use prost::Message;
use raft::eraftpb::Message as RaftMessage;
use raft::prelude::*;
use raft::StateRole;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

use toshi_proto::cluster_rpc::RaftRequest;
use toshi_types::{AddDocument, Error, IndexHandle};

use crate::proposal::Proposal;
use crate::rpc_utils::create_client;

pub mod handle;
pub mod proposal;
pub mod rpc_server;
pub mod rpc_utils;

pub type BoxErr = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, BoxErr>;

pub async fn run<H>(
    mut raft_group: RawNode<H>,
    proposals: Arc<RwLock<Receiver<Proposal>>>,
    pending_messages: Arc<RwLock<Vec<RaftMessage>>>,
    nodes: Arc<DashMap<String, Uri>>,
) -> Result<()>
where
    H: Storage + IndexHandle + Send + Sync,
{
    let mut t = Instant::now();
    loop {
        let pending_messages = Arc::clone(&pending_messages);
        let mut pending = pending_messages.write().await;
        let proposals = Arc::clone(&proposals);
        let mut props = proposals.write().await;
        if !pending.is_empty() {
            if let Some(msg) = pending.pop() {
                step(msg, &mut raft_group, Arc::clone(&pending_messages)).await?;
            }
        }

        if t.elapsed() >= Duration::from_millis(100) {
            // Tick the raft.
            raft_group.tick();
            t = Instant::now();
        }

        // Let the leader pick pending proposals from the global queue.
        if raft_group.raft.state == StateRole::Leader {
            let p: Proposal = props.recv().await.unwrap();
            propose(p, &mut raft_group)
        }

        // Handle readies from the raft.

        on_ready(&mut raft_group, Arc::clone(&nodes), Arc::clone(&proposals)).await?;
    }
}

pub async fn on_ready<H>(
    raft_group: &mut RawNode<H>,
    nodes: Arc<DashMap<String, Uri>>,
    proposals: Arc<RwLock<Receiver<Proposal>>>,
) -> Result<()>
where
    H: Storage + IndexHandle + Send + Sync,
{
    if !raft_group.has_ready() {
        return Ok(());
    }
    let mut ready = raft_group.ready();
    handle_messages(ready.take_messages(), Arc::clone(&nodes)).await?;

    // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
    if *ready.snapshot() != Snapshot::default() {
        let _s = ready.snapshot().clone();
        // Apply the snapshot here.
    }

    handle_committed(ready.take_committed_entries(), raft_group, Arc::clone(&proposals)).await?;

    for msg in ready.entries() {
        let add = AddDocument::new(serde_json::from_slice(&msg.data)?, None);
        raft_group.store().add_document(add).await?;
    }
    raft_group.store().commit().await?;

    let mut light_rd = raft_group.advance(ready);
    handle_messages(light_rd.take_messages(), Arc::clone(&nodes)).await?;
    handle_committed(light_rd.take_committed_entries(), raft_group, Arc::clone(&proposals)).await?;
    // Call `RawNode::advance` interface to update position flags in the raft.
    raft_group.advance_apply();
    Ok(())
}

pub async fn handle_messages(msgs: Vec<Vec<raft::eraftpb::Message>>, nodes: Arc<DashMap<String, Uri>>) -> Result<()> {
    for vec_msg in msgs {
        for msg in vec_msg {
            let to = msg.to;
            let node = nodes
                .get(&to.to_string())
                .ok_or_else(|| Error::RPCError(format!("Unable to get node for: {}", &to)))?;

            let mut client = create_client(&node, None).await?;
            let req = RaftRequest { message: Some(msg) };
            client.raft_request(req).await?;
        }
    }
    Ok(())
}

pub async fn handle_committed<H>(entries: Vec<Entry>, raft_group: &mut RawNode<H>, proposals: Arc<RwLock<Receiver<Proposal>>>) -> Result<()>
where
    H: Storage + IndexHandle + Send + Sync,
{
    for entry in entries {
        if entry.data.is_empty() {
            // From new elected leaders.
            continue;
        }
        if let EntryType::EntryConfChange = entry.get_entry_type() {
            // For conf change messages, make them effective.
            let mut cc = ConfChange::default();
            cc.merge(&*entry.data)?;
            raft_group.apply_conf_change(&cc)?;
        } else {
            let doc = serde_json::from_slice(&entry.data)?;
            raft_group.store().add_document(AddDocument::new(doc, None)).await?;
        }
        if raft_group.raft.leader_id == raft_group.raft.id {
            // The leader should response to the clients, tell them if their proposals
            // succeeded or not.

            let prop = proposals.write().await.recv().await.unwrap();
            prop.propose_success.send(true).unwrap();
        }
    }
    Ok(())
}

pub async fn step<H>(mut m: RaftMessage, raft_group: &mut RawNode<H>, pending_messages: Arc<RwLock<Vec<RaftMessage>>>) -> Result<()>
where
    H: Storage + IndexHandle + Send + Sync,
{
    // Here we hold up MsgReadIndex. If current peer has valid lease, then we could handle the
    // request directly, rather than send a heartbeat to check quorum.
    let msg_type = m.get_msg_type();
    let committed = raft_group.raft.raft_log.committed;
    let expected_term = raft_group.raft.raft_log.term(committed).unwrap_or(1);
    if msg_type == MessageType::MsgReadIndex && expected_term == raft_group.status().hs.term {
        // If the leader hasn't committed any entries in its term, it can't response read only
        // requests. Please also take a look at raft-rs.

        let mut resp = RaftMessage::default();
        resp.set_msg_type(MessageType::MsgReadIndexResp);
        resp.term = raft_group.raft.term;
        resp.to = m.from;
        resp.index = raft_group.store().get_opstamp() as u64;
        resp.set_entries(m.take_entries());
        let mut pending = pending_messages.write().await;
        pending.push(resp);
        return Ok(());
    }
    if msg_type == MessageType::MsgTransferLeader {
        execute_transfer_leader(&m, raft_group);
        return Ok(());
    }

    raft_group.step(m)?;
    Ok(())
}

pub fn execute_transfer_leader<H>(msg: &RaftMessage, raft_group: &mut RawNode<H>)
where
    H: Storage + IndexHandle + Send + Sync,
{
    if msg.get_log_term() != raft_group.raft.term {
        return;
    }

    if raft_group.raft.leader_id == raft_group.raft.id {
        let from = msg.get_from();
        raft_group.transfer_leader(from);
        return;
    }

    let mut msg = RaftMessage::default();
    msg.set_from(raft_group.raft.id);
    msg.set_to(raft_group.raft.leader_id);
    msg.set_msg_type(MessageType::MsgTransferLeader);
    msg.set_index(raft_group.store().get_opstamp() as u64);
    msg.set_log_term(raft_group.raft.term);
    raft_group.raft.msgs.push(msg);
}

fn propose<H>(mut proposal: Proposal, raft_group: &mut RawNode<H>)
where
    H: Storage + IndexHandle + Send + Sync,
{
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some(ref data) = proposal.normal {
        let _ = raft_group.propose(vec![], data.to_vec());
    } else if let Some(ref cc) = proposal.conf_change {
        let _ = raft_group.propose_conf_change(vec![], cc.clone());
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO: implement transfer leader.
        unimplemented!();
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        proposal.propose_success.send(true).unwrap();
    } else {
        proposal.proposed = last_index1;
    }
}
