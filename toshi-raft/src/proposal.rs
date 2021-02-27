use raft::prelude::*;
use tokio::sync::oneshot::{channel, Receiver, Sender};

#[derive(Debug)]
pub struct Proposal {
    pub normal: Option<Vec<u8>>,
    pub conf_change: Option<ConfChange>,
    pub transfer_leader: Option<u64>,
    pub proposed: u64,
    pub propose_success: Sender<bool>,
}

impl Proposal {
    pub fn new(entry: Vec<u8>) -> (Self, Receiver<bool>) {
        let (snd, rcv) = channel();
        let prop = Self {
            normal: Some(entry),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
            propose_success: snd,
        };
        (prop, rcv)
    }

    pub fn conf_change(conf: &ConfChange) -> (Self, Receiver<bool>) {
        let (snd, rcv) = channel();
        let prop = Self {
            normal: None,
            conf_change: Some(conf.clone()),
            transfer_leader: None,
            proposed: 0,
            propose_success: snd,
        };
        (prop, rcv)
    }
}
