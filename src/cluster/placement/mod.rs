use std::collections::HashSet;
use std::net::SocketAddr;

use futures::{future, Future, Stream};
use tokio::net::TcpListener;
use tokio::sync::watch::*;
use tower_grpc::{Request, Response, Status};
use tower_hyper::Server;
use tracing::*;

use toshi_proto::placement_proto::{server, PlacementReply, PlacementRequest};

use crate::cluster::consul::Consul;

pub use self::background::Background;

pub mod background;

// TODO: replace this with an actual future
type GrpcFuture<T> = Box<dyn Future<Item = Response<T>, Error = Status> + Send + 'static>;

#[derive(Clone)]
pub struct Place {
    consul: Consul,
    nodes: Receiver<HashSet<SocketAddr>>,
}

impl Place {
    pub fn serve(consul: Consul, nodes: Receiver<HashSet<SocketAddr>>, addr: SocketAddr) -> impl Future<Item = (), Error = std::io::Error> {
        future::lazy(move || TcpListener::bind(&addr)).and_then(|bind| {
            let placer = Place { consul, nodes };
            let placement = server::PlacementServer::new(placer);

            let mut hyp = Server::new(placement);

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
