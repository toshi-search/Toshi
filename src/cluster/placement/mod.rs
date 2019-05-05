use std::collections::HashSet;
use std::net::SocketAddr;

use futures::{future, Future, Stream};
use futures_watch::Watch;
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response, Status};

use crate::cluster::consul::Consul;
use toshi_proto::placement_proto::{server, PlacementReply, PlacementRequest};

pub use self::background::Background;

pub mod background;

// TODO: replace this with an actual future
type GrpcFuture<T> = Box<Future<Item = Response<T>, Error = Status> + Send + 'static>;

/// The placement service for Toshi. Its role is to
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

            let mut hyp = tower_hyper::server::Server::new(placement);

            bind.incoming().for_each(move |stream| {
                if let Err(e) = stream.set_nodelay(true) {
                    return Err(e);
                }

                let serve = hyp.serve(stream);
                tokio::spawn(serve.map_err(|e| error!("Placement Server Error: {:?}", e)));

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
//
//impl<T> Service<http::Request<Body>> for server::PlacementServer<T> {
//    type Response = http::Response<LiftBody<Body>>;
//    type Error = h2::Error;
//    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;
//
//    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
//        unimplemented!()
//    }
//
//    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
//        unimplemented!()
//    }
//}
