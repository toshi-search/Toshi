use std::collections::HashSet;
use std::net::SocketAddr;

use futures::{future, Future, Poll, Stream};
use futures_watch::Watch;
use hyper::server::conn::Http;
use hyper::Body;
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response, Status};

use tower_service::Service;

use crate::cluster::consul::Consul;
use crate::cluster::placement_proto::{server, PlacementReply, PlacementRequest};

pub use self::background::Background;
use tower_hyper::body::LiftBody;

pub mod background;

// TODO: replace this with an actual future
type GrpcFuture<T> = Box<Future<Item = Response<T>, Error = Status> + Send + 'static>;

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
            let mut placement = server::PlacementServer::new(placer);

            let mut hyp = tower_hyper::server::Server::new(placement);
            bind.incoming().for_each(move |stream| {
                if let Err(e) = stream.set_nodelay(true) {
                    return Err(e);
                }

                let serve = hyp.serve_with(stream, Http::new());
                hyper::rt::spawn(serve.map_err(|e| error!("Placement Server Error: {:?}", e)));

                Ok(())
            })
        })
    }
}

impl server::Placement for Place {
    type GetPlacementFuture = GrpcFuture<PlacementReply>;

    fn get_placement(&mut self, _request: Request<PlacementRequest>) -> Self::GetPlacementFuture {
        unimplemented!()
    }
}

impl<T> Service<http::Request<Body>> for server::PlacementServer<T> {
    type Response = http::Response<LiftBody<Body>>;
    type Error = h2::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        unimplemented!()
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        unimplemented!()
    }
}
