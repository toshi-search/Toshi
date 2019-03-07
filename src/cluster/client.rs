use futures::{Async, Poll};
use http::{Request, Response};
use hyper::body::Payload;
use hyper::client::connect::Connect as HyperConnect;
use hyper::client::ResponseFuture;
use hyper::Body;
use tower_service::Service;

#[derive(Debug)]
pub struct Client<C, B> {
    inner: hyper::Client<C, B>,
}

impl<C, B> Client<C, B> {
    /// Create a new client from a `hyper::Client`
    pub fn new(inner: hyper::Client<C, B>) -> Self {
        Self { inner }
    }
}

impl<C, B> Service<Request<B>> for Client<C, B>
where
    C: HyperConnect + Sync + 'static,
    C::Transport: 'static,
    C::Future: 'static,
    B: Payload + Send + 'static,
    B::Data: Send,
{
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = ResponseFuture;

    /// Poll to see if the service is ready, since `hyper::Client`
    /// already handles this internally this will always return ready
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    /// Send the sepcficied request to the inner `hyper::Client`
    fn call(&mut self, req: Request<B>) -> Self::Future {
        self.inner.request(req)
    }
}
