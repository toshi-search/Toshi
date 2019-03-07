use bytes::Buf;
use futures::{Future, Poll};
use hyper::body::Payload;
use hyper::server::conn::Http;
use hyper::service::Service as HyperService;
use hyper::{Body, HeaderMap};
use hyper::{Request, Response};
use tokio_buf::{BufStream, SizeHint};
use tokio_io::{AsyncRead, AsyncWrite};
use tower_http_service::HttpService;
use tower_service::Service;
use tower_util::MakeService;

pub struct StreamBuf<S>(S);

impl<S> BufStream for StreamBuf<S>
where
    S: Payload + Send + 'static,
{
    type Item = S::Data;
    type Error = S::Error;

    fn poll_buf(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll_data()
    }
}

/// Server implementation for hyper
#[derive(Debug)]
pub struct Server<S> {
    maker: S,
}

impl<S> Server<S>
where
    S: MakeService<(), Request<Body>, Response = Response<Body>> + Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    S::Future: Send + 'static,
    S::Service: Send + 'static,
    <S::Service as Service<Request<Body>>>::Future: Send + 'static,
{
    /// Create a new server from a `MakeService`
    pub fn new(maker: S) -> Self {
        Server { maker }
    }

    /// Serve the `io` stream via default hyper http settings
    pub fn serve<I>(&mut self, io: I) -> Box<Future<Item = (), Error = hyper::Error> + Send + 'static>
    where
        I: AsyncRead + AsyncWrite + Send + 'static,
    {
        let http = Http::new();
        self.serve_with(io, http)
    }

    /// Serve the `io` stream via the provided hyper http settings
    pub fn serve_with<I>(&mut self, io: I, http: Http) -> Box<Future<Item = (), Error = hyper::Error> + Send + 'static>
    where
        I: AsyncRead + AsyncWrite + Send + 'static,
    {
        let fut = self.maker.make_service(()).map_err(|_| unimplemented!()).and_then(move |service| {
            let l = Lift::new(service);
            http.serve_connection(http, l)
        });

        Box::new(fut)
    }
}

struct Lift<T> {
    inner: T,
}

impl<T> Lift<T> {
    pub fn new(inner: T) -> Self {
        Lift { inner }
    }
}

impl<T> hyper::service::Service for Lift<T>
where
    T: HttpService<Body, ResponseBody = Body> + Send + 'static,
    T::Error: Into<Box<std::error::Error + Send + Sync + 'static>> + Send,
    T::Future: Send + 'static,
{
    type ReqBody = Body;
    type ResBody = Body;
    type Error = T::Error;
    type Future = Box<Future<Item = Response<Self::ResBody>, Error = Self::Error>>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let fut = self.inner.call(req).map(|b| Response::new(b)).map_err(|e| e.into());

        Box::new(fut)
    }
}
