use crate::cluster::consul::Consul;
use crate::cluster::placement_proto::{server, PlacementReply, PlacementRequest};
use futures::{future, try_ready, Async, Future, Poll, Stream};
use futures_watch::Watch;
use log::error;
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;
use tower_discover::{Change, Discover};
use tower_grpc::{Error, Request, Response};
use tower_h2::Server;
use tower_service::Service;

pub mod background;

pub use self::background::Background;

type GrpcFuture<T> = Box<Future<Item = Response<T>, Error = Error> + Send + 'static>;

/// The placement service for toshi. Its role is to
/// tell the cluster where to place writes and reads.
#[derive(Clone)]
pub struct Place {
    consul: Consul,
    nodes: Watch<HashSet<SocketAddr>>,
}

impl Place {
    /// Bind a tcp listener on the provided address and
    /// spawn a new service on each incoming connection.
    pub fn serve(consul: Consul, nodes: Watch<HashSet<SocketAddr>>, addr: SocketAddr) -> impl Future<Item = (), Error = std::io::Error> {
        future::lazy(move || TcpListener::bind(&addr)).and_then(|bind| {
            let placer = Place { consul, nodes };
            let placement = server::PlacementServer::new(placer);
            let mut server = Server::new(placement, Default::default(), DefaultExecutor::current());

            bind.incoming().for_each(move |stream| {
                if let Err(e) = stream.set_nodelay(true) {
                    return Err(e);
                }

                let serve = server.serve(stream);
                tokio::spawn(serve.map_err(|e| error!("Placement Server Error: {:?}", e)));

                Ok(())
            })
        })
    }
}

impl server::Placement for Place {
    type GetPlacementFuture = GrpcFuture<PlacementReply>;

    fn get_placement(&mut self, request: Request<PlacementRequest>) -> Self::GetPlacementFuture {
        unimplemented!()
    }
}
