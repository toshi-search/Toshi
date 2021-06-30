use std::net::SocketAddr;

use dashmap::DashMap;
use message_io::events::*;
use message_io::network::{split as net_split, Endpoint, NetEvent, NetworkController, NetworkProcessor, Transport};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftEvents {
    JoinCluster(u32),
}

pub struct RaftIO {
    sender: EventSender<RaftEvents>,
    receiver: EventReceiver<RaftEvents>,
    controller: NetworkController,
    processor: NetworkProcessor,
    peers: DashMap<u32, Endpoint>,
}

impl Default for RaftIO {
    fn default() -> Self {
        let (sender, receiver) = split();
        let (controller, processor) = net_split();
        Self {
            sender,
            receiver,
            peers: DashMap::default(),
            controller,
            processor,
        }
    }
}

impl RaftIO {
    pub fn new(id: u32, ep: Endpoint) -> Self {
        let (sender, receiver) = split();
        let (controller, processor) = net_split();

        let peers = DashMap::<u32, Endpoint>::new();
        peers.insert(id, ep);
        Self {
            sender,
            receiver,
            controller,
            processor,
            peers,
        }
    }

    fn join_cluster(&mut self, id: u32, addr: SocketAddr) {
        if let Ok((endpoint, _)) = self.controller.connect(Transport::Tcp, addr) {
            self.peers.insert(id, endpoint);
        }
    }

    pub fn run(self, endpoint: String) -> Result<(), Box<dyn std::error::Error>> {
        let (_, _) = self.controller.listen(Transport::Tcp, &endpoint)?;

        Ok(())
    }
    fn process_event(&mut self, msg: NetEvent) {
        match msg {
            NetEvent::Message(ep, payload) => {
                let msg = bincode::deserialize::<RaftEvents>(&payload).unwrap();
                match msg {
                    RaftEvents::JoinCluster(id) => self.join_cluster(id, ep.addr()),
                }
            }
            NetEvent::Connected(_, _) => {}
            NetEvent::Disconnected(_) => {}
            NetEvent::Accepted(_, _) => {}
        }
    }
}
