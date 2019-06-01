use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::{try_ready, Future, Poll};
use log::debug;
use tokio::sync::mpsc;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::timer::Delay;
use tower_consul::ConsulService;

use crate::cluster::{consul::Consul, ClusterError};

pub struct Background {
    consul: Consul,
    // TODO: better D/S for this?
    store: Sender<HashSet<SocketAddr>>,
    state: State,
    interval: Duration,
}

impl Background {
    pub fn new(mut consul: Consul, interval: Duration) -> (Receiver<HashSet<SocketAddr>>, Self) {
        let (mut store, watch) = channel(HashSet::new());

        store.broadcast(HashSet::new()).expect("Unable to store inital placement bg watch");

        let state = State::Fetching(Box::new(consul.nodes()));

        let bg = Background {
            consul,
            store,
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

                    debug!("Got {} services from consul", services.len());

                    let services = services.into_iter().map(|e| e.address.parse().unwrap()).collect::<HashSet<_>>();

                    self.store.broadcast(services).map_err(|_| ClusterError::UnableToStoreServices)?;

                    let deadline = Instant::now() + self.interval;

                    debug!("Waiting {:?} duration till next consul refresh", deadline);

                    let delay = Delay::new(deadline);

                    self.state = State::Waiting(delay);
                    continue;
                }
                State::Waiting(ref mut fut) => {
                    try_ready!(fut.poll());

                    self.state = State::Fetching(Box::new(self.consul.nodes()));
                }
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
    Timer(tokio::timer::Error),
    Send,
}

impl From<ClusterError> for Error {
    fn from(e: ClusterError) -> Self {
        Error::Cluster(e)
    }
}

impl From<mpsc::error::SendError> for Error {
    fn from(_: mpsc::error::SendError) -> Self {
        Error::Send
    }
}

impl From<tokio::timer::Error> for Error {
    fn from(err: tokio::timer::Error) -> Self {
        Error::Timer(err)
    }
}
