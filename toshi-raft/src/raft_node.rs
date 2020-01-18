use std::time::Duration;

use prost5::Message;
use raft::prelude::*;
use raft::{Config, RawNode};
use slog::Logger;
use tokio::sync::mpsc::*;
use tokio::time::{interval, timeout};

use crate::{SledStorage, SledStorageError};

pub struct ToshiRaft {
    pub node: RawNode<SledStorage>,
    pub logger: Logger,
    pub mailbox_sender: Sender<toshi_proto::cluster_rpc::Message>,
    pub mailbox_recv: Receiver<toshi_proto::cluster_rpc::Message>,
    pub heartbeat: usize,
}

impl ToshiRaft {
    pub fn new(cfg: &Config, logger: &Logger) -> Result<Self, crate::SledStorageError> {
        cfg.validate()?;

        let node = RawNode::new(cfg, SledStorage::new_with_logger("wal", Some(cfg), Some(logger))?)?.with_logger(logger);
        let (snd, recv) = channel(1024);
        Ok(Self {
            node,
            logger: logger.clone(),
            mailbox_sender: snd,
            mailbox_recv: recv,
            heartbeat: cfg.heartbeat_tick,
        })
    }

    pub fn tick(&mut self) -> bool {
        self.node.tick()
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

    pub async fn send(&mut self, msg: toshi_proto::cluster_rpc::Message) -> Result<(), SledStorageError> {
        self.mailbox_sender.send(msg).await.unwrap();
        Ok(())
    }

    #[allow(irrefutable_let_patterns)]
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        while let t = interval(Duration::from_millis(self.heartbeat as u64)).tick().await {
            let msg = match timeout(Duration::from_millis(100), self.mailbox_recv.recv()).await {
                Ok(Some(msg)) => Some(msg),
                Ok(None) => return Ok(()),
                Err(_) => None,
            };
            slog::info!(self.logger, "Inbound raft message: {:?}", msg);

            if let Some(msg) = msg {
                slog::info!(self.logger, "Step step...");
                self.node.step(msg.into())?;
            }

            self.node.tick();
            if self.node.has_ready() {
                slog::info!(self.logger, "I'm ready!");
                self.ready().await?;
            }
            slog::info!(self.logger, "Tick tock at: {:?}", t.elapsed());
        }

        Ok(())
    }

    pub async fn ready(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        if !self.node.has_ready() {
            panic!("Node is not ready");
        }
        let mut ready = self.node.ready();

        let is_leader = self.node.raft.leader_id == self.node.raft.id;
        slog::info!(self.logger, "Am I leader?: {}", is_leader);

        if !raft::is_empty_snap(ready.snapshot()) {
            let snap = ready.snapshot();
            slog::info!(self.logger, "Got a snap: {:?}", snap);
            self.node.mut_store().apply_snapshot(snap.clone())?;
        }

        if !ready.entries().is_empty() {
            let entries = ready
                .entries()
                .iter()
                .cloned()
                .filter(|e| !e.get_data().is_empty())
                .collect::<Vec<Entry>>();
            self.node.mut_store().append(&entries)?;
        }

        if let Some(hs) = ready.hs() {
            self.node.mut_store().state.hard_state = (*hs).clone();
            self.node.mut_store().commit(hs.commit)?;
        }
        if !is_leader {
            let msgs = ready.messages.drain(..);
            for msg in msgs {
                self.append_entries(&msg.entries).await?;
            }
        }
        if let Some(committed_entries) = ready.committed_entries.take() {
            self.append_entries(&committed_entries).await?;

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

            match EntryType::from_i32(entry.entry_type) {
                Some(EntryType::EntryConfChange) => {
                    let mut cc = raft::eraftpb::ConfChange::new_();

                    let cs = self.node.apply_conf_change(&cc)?;
                    self.node.mut_store().set_conf_state(cs);
                }
                Some(EntryType::EntryNormal) => {
                    let mut e = Entry::default();
                    e.merge(&entry.data)?;

                    self.node.mut_store().append_single(e)?;
                }
                None => panic!(":-("),
            }
        }
        Ok(())
    }
}
