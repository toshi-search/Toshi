use std::net::SocketAddr;

use futures::{future, Future, Stream};
use log::{error, info};

use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;

use tower_grpc::{Error, Request, Response};
use tower_h2::Server;

use crate::cluster::placement::{server, IndexKind, PlacementReply, PlacementRequest};

#[derive(Clone, Debug)]
pub struct Place;

impl server::Placement for Place {
    type GetPlacementFuture = future::FutureResult<Response<PlacementReply>, Error>;

    fn get_placement(&mut self, request: Request<PlacementRequest>) -> Self::GetPlacementFuture {
        info!("Request = {:?}", request);
        let response = Response::new(PlacementReply {
            host: "localhost:90189271876281".into(),
            kind: IndexKind::Shard.into(),
        });

        future::ok(response)
    }
}

impl Place {
    pub fn get_service(addr: SocketAddr) -> impl Future<Item = (), Error = ()> {
        let service = server::PlacementServer::new(Place);
        let executor = DefaultExecutor::current();
        let mut h2 = Server::new(service, Default::default(), executor);

        info!("Binding on port: {:?}", addr);
        let bind = TcpListener::bind(&addr).unwrap_or_else(|_| panic!("Failed to bind to host: {:?}", addr));

        info!("Bound to: {:?}", &bind.local_addr().unwrap());
        bind.incoming()
            .for_each(move |sock| {
                let req = h2.serve(sock).map_err(|err| error!("h2 error: {:?}", err));
                tokio::spawn(req);
                Ok(())
            })
            .map_err(|err| error!("Server Error: {:?}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;
    use tower_h2::client::Connect;

    pub struct Conn(SocketAddr);

    #[test]
    fn client_test() {
        let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let mut server = Place::get_service(addr.clone());

        let req = PlacementRequest {
            index: "test".into(),
            kind: IndexKind::Shard.into(),
        };
        let tcp_stream = Box::new(TcpStream::connect(&addr).and_then(|tcp| tcp.set_nodelay(true).map(move |_| tcp)));

        let mut c = Connect::new(tcp_stream, Default::default(), DefaultExecutor::current());
    }

}
