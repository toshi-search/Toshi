use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use http::Uri;
use prost::Message;
use raft::prelude::*;
use raft::{Config, RawNode};
use slog::Logger;
use tokio::sync::mpsc::*;
use tokio::time::*;
use tonic::Request;

use toshi_proto::cluster_rpc::{self, RaftRequest};
use toshi_types::{AddDocument, Catalog, IndexHandle};

use crate::rpc_server::{create_client, RpcClient};
use crate::{SledStorage, SledStorageError};

pub struct ToshiRaft<C>
where
    C: Catalog,
{
    pub node: RawNode<SledStorage>,
    pub logger: Logger,
    pub mailbox_sender: Sender<cluster_rpc::Message>,
    pub mailbox_recv: Receiver<cluster_rpc::Message>,
    pub conf_sender: Sender<ConfChange>,
    pub conf_recv: Receiver<ConfChange>,
    pub peers: Arc<DashMap<u64, RpcClient>>,
    pub heartbeat: usize,
    pub catalog: Arc<C>,
}

impl<C> ToshiRaft<C>
where
    C: Catalog,
{
    pub fn new(
        cfg: Config,
        mut base_path: String,
        logger: Logger,
        peers: Arc<DashMap<u64, RpcClient>>,
        catalog: Arc<C>,
        mailbox_sender: Sender<cluster_rpc::Message>,
        mailbox_recv: Receiver<cluster_rpc::Message>,
    ) -> Result<Self, crate::SledStorageError> {
        cfg.validate()?;

        assert!(!base_path.is_empty());
        if base_path.ends_with('/') {
            base_path.pop();
        }

        let path = format!("{}-wal", base_path);
        let db = SledStorage::new_with_logger(&path, cfg.clone(), Some(logger.clone()))?;
        let node = RawNode::new(&cfg, db, &logger)?;
        let (conf_sender, conf_recv) = channel(1024);

        Ok(Self {
            node,
            logger: logger.clone(),
            mailbox_sender,
            mailbox_recv,
            conf_sender,
            conf_recv,
            heartbeat: cfg.heartbeat_tick,
            peers,
            catalog,
        })
    }

    pub fn tick(&mut self) -> bool {
        self.node.tick()
    }

    pub fn propose_conf_change(&mut self, context: Vec<u8>, cc: ConfChange) -> Result<(), crate::Error> {
        Ok(self.node.propose_conf_change(context, cc)?)
    }

    pub fn become_leader(&mut self) {
        self.node.raft.raft_log.committed = 0;
        self.node.raft.become_candidate();
        self.node.raft.become_leader();
    }

    fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), crate::Error> {
        self.node.raft.mut_store().state.hard_state.commit = commit;
        self.node.raft.mut_store().state.hard_state.term = term;
        Ok(())
    }

    pub fn propose(&mut self, ctx: Vec<u8>, entry: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self.node.propose(ctx, entry)?)
    }

    pub async fn send(&mut self, msg: cluster_rpc::Message) -> Result<(), SledStorageError> {
        slog::info!(self.logger, "SEND = {:?}", msg);
        self.mailbox_sender.send(msg).await.unwrap();
        Ok(())
    }

    #[allow(irrefutable_let_patterns)]
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        while let _ = interval(Duration::from_millis(self.heartbeat as u64)).tick().await {
            let msg = match timeout(Duration::from_millis(100), self.mailbox_recv.recv()).await {
                Ok(Some(msg)) => Some(msg),
                Ok(None) => None,
                Err(_) => None,
            };

            if let Some(msg) = msg {
                slog::info!(self.logger, "Inbound raft message: {:?}", msg);
                self.node.step(msg.into())?;
            }

            match timeout(Duration::from_millis(100), self.conf_recv.recv()).await {
                Ok(Some(cc)) => {
                    let ccc = cc.clone();
                    let state = self.node.apply_conf_change(&cc)?;

                    self.node.mut_store().state.conf_state = state;
                    let p = self.peers.clone();
                    let logger = self.logger.clone();
                    tokio::spawn(async move {
                        let uri = Uri::try_from(&ccc.context[..]).unwrap();
                        let client: RpcClient = create_client(uri.clone(), Some(logger.clone())).await.unwrap();
                        p.insert(ccc.node_id, client);
                        slog::info!(logger, "Added client: {:?} - {:?}", ccc.node_id, &uri);
                    });
                }
                Ok(None) => (),
                Err(_) => (),
            };

            if self.node.has_ready() {
                slog::info!(self.logger, "I'm ready!");
                self.ready().await?;
            }
            self.node.tick();
        }

        Ok(())
    }

    pub async fn ready(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut ready = self.node.ready();

        let is_leader = self.node.raft.leader_id == self.node.raft.id;
        slog::info!(
            self.logger,
            "Leader ID: {}, Node ID: {}",
            self.node.raft.leader_id,
            self.node.raft.id
        );
        slog::info!(self.logger, "Am I leader?: {}", is_leader);

        if !ready.snapshot().is_empty() {
            let snap = ready.snapshot().clone();
            slog::info!(self.logger, "Got a snap: {:?}", snap);
            self.node.mut_store().apply_snapshot(snap)?;
        }

        if !ready.entries().is_empty() {
            let entries = ready
                .entries()
                .iter()
                .cloned()
                .filter(|e| !e.get_data().is_empty())
                .collect::<Vec<Entry>>();
            slog::info!(self.logger, "Entries?: {}", entries.len());
            self.node.mut_store().append_entries(&entries)?;
        }

        if let Some(hs) = ready.hs() {
            slog::info!(self.logger, "HS?: {:?}", hs);
            self.node.mut_store().state.hard_state = (*hs).clone();
            // self.node.mut_store().commit()?;
        }

        for mut msg in ready.messages.drain(..) {
            slog::info!(self.logger, "LOGMSG==={:?}", msg);
            let to = msg.to;
            msg.from = self.catalog.raft_id();
            msg.log_term = self.node.store().state.hard_state.term;
            msg.commit = self.node.store().state.hard_state.commit;
            if let Some(client) = self.peers.get(&to) {
                let mut msg_bytes = vec![];
                msg.encode(&mut msg_bytes).unwrap();
                let req = Request::new(RaftRequest {
                    tpe: 0,
                    message: msg_bytes,
                });
                let req = client.clone().raft_request(req).await?;
                slog::info!(self.logger, "RESP={:?}", req);
            }
            self.append_entries(&msg.entries).await?;
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            for mut entry in committed_entries.clone() {
                slog::info!(self.logger, "Committing: {:?}", entry);
                let index = std::str::from_utf8(&entry.context)?;
                let handle = self.catalog.get_index(&index)?;
                handle
                    .add_document(AddDocument::<serde_json::Value> {
                        options: None,
                        document: serde_json::to_value(entry.mut_data())?,
                    })
                    .await?;
            }

            if let Some(entry) = committed_entries.last() {
                self.set_hard_state(entry.index, entry.term)?;
            }
        }

        self.node.advance(ready);
        Ok(())
    }

    pub async fn append_entries(&mut self, entries: &[Entry]) -> Result<(), SledStorageError> {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }
            slog::info!(self.logger, "APPEND={:?}", entry);

            match EntryType::from_i32(entry.entry_type) {
                Some(EntryType::EntryConfChange) => {
                    let mut cc = ConfChange::default();
                    cc.merge(Bytes::from(entry.data.clone()))?;

                    let cs = self.node.apply_conf_change(&cc)?;
                    self.node.mut_store().set_conf_state(cs);
                }
                Some(EntryType::EntryNormal) => {
                    let mut e = Entry::default();

                    e.merge(Bytes::from(entry.data.clone()))?;

                    self.node.mut_store().append_entries(&[e])?;
                }
                Some(EntryType::EntryConfChangeV2) => panic!("Conf2"),
                None => panic!(":-("),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
