use crate::cluster::{consul::Consul, ClusterError};
use futures::{sync::mpsc, try_ready, Future, Poll};
use futures_watch::{Store, Watch};
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::timer::Delay;
use tower_consul::ConsulService;
use tower_discover::Change;
use tower_service::Service;

pub struct Background {
    consul: Consul,
    // TODO: better D/S for this?
    store: Store<HashSet<SocketAddr>>,
    nodes: HashSet<SocketAddr>,
    state: State,
    interval: Duration,
}

impl Background {
    pub fn new(mut consul: Consul, interval: Duration) -> (Watch<HashSet<SocketAddr>>, Self) {
        let (watch, mut store) = Watch::new(HashSet::new());

        store.store(HashSet::new());

        let state = State::Fetching(Box::new(consul.nodes()));

        let bg = Background {
            consul,
            store,
            nodes: HashSet::new(),
            state,
            interval,
        };

        (watch, bg)
    }
}

impl Future for Background {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.state {
                State::Fetching(ref mut fut) => {
                    let services = try_ready!(fut.poll());

                    let services = services.into_iter().map(|e| e.address.parse().unwrap()).collect::<HashSet<_>>();

                    self.store.store(services);

                    let deadline = Instant::now() + self.interval;
                    let delay = Delay::new(deadline);

                    self.state = State::Waiting(delay);
                    continue;
                }
                _ => unimplemented!(),
            }
        }
    }
}

enum State {
    Fetching(Box<Future<Item = Vec<ConsulService>, Error = ClusterError> + Send>),
    Waiting(Delay),
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
