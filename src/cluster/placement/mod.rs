use std::collections::HashSet;
use std::net::SocketAddr;

use crate::cluster::consul::Consul;
use crate::cluster::placement_proto::{server, PlacementReply, PlacementRequest};
use crate::cluster::server::Server;
use futures::{future, Future, Poll, Stream};
use futures_watch::Watch;
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response, Status};

pub use self::background::Background;
use futures::future::FutureResult;
use hyper::server::conn::Http;
use hyper::Body;
use tower_service::Service;

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

            let mut hyp = Server::new(placement);
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

impl Service<http::Request<Body>> for server::PlacementServer<Place> {
    type Response = http::Response<Body>;
    type Error = hyper::Error;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        self.maker.make_service(()).call(req)
    }
}
