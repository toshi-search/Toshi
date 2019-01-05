use crate::cluster::{consul::Consul, ClusterError};
use futures::{sync::mpsc, try_ready, Future, Poll};
use futures_watch::{Store, Watch};
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::net::SocketAddr;
use tower_consul::ConsulService;
use tower_discover::Change;
use tower_service::Service;

pub struct Background {
    consul: Consul,
    store: Store<HashSet<SocketAddr>>,
    nodes: HashSet<SocketAddr>,
    state: State,
}

impl Background {
    pub fn new(consul: Consul) -> (Watch<HashSet<SocketAddr>>, Self) {
        let (watch, store) = Watch::new(HashSet::new());

        let bg = Background {
            consul,
            store,
            nodes: HashSet::new(),
            state: State::Start,
        };

        (watch, bg)
    }

    fn diff(&mut self, other: Vec<SocketAddr>) {
        for endpoint in other {
            if !self.nodes.contains(&endpoint) {
                // TODO: need to check if this change invalidates any other change
                // think if we marked a node as removed but we just got a message
                // that the node is back. We need to invalidate that previous message
                // though don't know how much this matters as it'll eventually
                // be consitient.
                // self.pending_changes.push_back(Change::Insert(endpoint, self.service.clone()));
                unimplemented!()
            }
        }
    }
}

impl Future for Background {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.state {
                State::Start => {
                    let fut = self.consul.nodes();
                    self.state = State::Fetching(Box::new(fut));
                    continue;
                }
                State::Fetching(ref mut fut) => {
                    let services = try_ready!(fut.poll());

                    let services = services.into_iter().map(|e| e.address.parse().unwrap()).collect::<Vec<_>>();
                    self.diff(services);
                    self.state = State::Waiting(());
                    continue;
                }
                _ => unimplemented!(),
            }
        }
    }
}

enum State {
    Start,
    Fetching(Box<Future<Item = Vec<tower_consul::ConsulService>, Error = ClusterError> + Send>),
    Sending,
    Waiting(()),
}

#[derive(Debug)]
pub enum Error {
    Cluster(ClusterError),
    Send,
}

impl From<ClusterError> for Error {
    fn from(e: ClusterError) -> Self {
        Error::Cluster(e)
    }
}

impl From<mpsc::SendError<()>> for Error {
    fn from(_: mpsc::SendError<()>) -> Self {
        Error::Send
    }
}
