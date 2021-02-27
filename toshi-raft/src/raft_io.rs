use std::net::SocketAddr;

use dashmap::DashMap;
use message_io::events::EventQueue;
use message_io::network::{Endpoint, NetEvent, Network, Transport};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftEvents {
    JoinCluster(u32),
}

pub struct RaftIO {
    net: Network,
    events: EventQueue<NetEvent>,
    peers: DashMap<u32, Endpoint>,
}

impl Default for RaftIO {
    fn default() -> Self {
        let (events, net) = Network::split();
        Self {
            net,
            events,
            peers: DashMap::default(),
        }
    }
}

impl RaftIO {
    pub fn new(id: u32, ep: Endpoint) -> Self {
        let (events, net) = Network::split();
        let peers = DashMap::<u32, Endpoint>::new();
        peers.insert(id, ep);
        Self { net, events, peers }
    }

    fn join_cluster(&mut self, id: u32, addr: SocketAddr) {
        if let Ok((endpoint, _)) = self.net.connect(Transport::Tcp, addr) {
            self.peers.insert(id, endpoint);
        }
    }

    pub fn run(mut self, endpoint: String) -> Result<(), Box<dyn std::error::Error>> {
        self.net.listen(Transport::Tcp, &endpoint)?;

        loop {
            match self.events.receive() {
                NetEvent::Message(ep, payload) => {
                    let msg = bincode::deserialize::<RaftEvents>(&payload)?;
                    match msg {
                        RaftEvents::JoinCluster(id) => self.join_cluster(id, ep.addr()),
                    }
                }
                NetEvent::Connected(_) => {}
                NetEvent::Disconnected(_) => {}
            }
        }
    }
}
