use message_io::events::EventQueue;

use message_io::network::{Endpoint, NetEvent, Network, Transport};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftEvents {
    JoinCluster { id: u32, ep: SocketAddr },
}

pub struct RaftIO {
    net: Network,
    events: EventQueue<NetEvent<RaftEvents>>,
    peers: DashMap<u32, Endpoint>,
}

impl Default for RaftIO {
    fn default() -> Self {
        let (net, events) = Network::split();
        Self {
            net,
            events,
            peers: DashMap::default(),
        }
    }
}

impl RaftIO {
    pub fn new(id: u32, ep: Endpoint) -> Self {
        let (net, events) = Network::split();
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
                NetEvent::Message(ep, msg) => match msg {
                    RaftEvents::JoinCluster { ep, id } => self.join_cluster(id, ep),
                },
                NetEvent::Connected(_) => {}
                NetEvent::Disconnected(_) => {}
                NetEvent::DeserializationError(ep) => {
                    panic!("Deserialization Error: {:?}", ep)
                }
            }
        }
    }
}
