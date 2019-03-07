use futures::Poll;
use http::{Request, Response};
use hyper::body::Payload;
use hyper::client::conn;
use tower_service::Service;

#[derive(Debug)]
pub struct Connection<B>
where
    B: Payload,
{
    sender: conn::SendRequest<B>,
}

impl<B> Connection<B>
where
    B: Payload,
{
    pub(super) fn new(sender: conn::SendRequest<B>) -> Self {
        Connection { sender }
    }
}

impl<B> Service<Request<B>> for Connection<B>
where
    B: Payload,
{
    type Response = Response<hyper::Body>;
    type Error = hyper::Error;
    type Future = conn::ResponseFuture;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.sender.poll_ready()
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        self.sender.send_request(req)
    }
}
