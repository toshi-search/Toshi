use std::fmt;
use std::ops::{Deref, DerefMut};

use failure::{format_err, Error};
use futures::{future, Future, Stream};
use http::HttpTryFrom;
use hyper::client::{connect::Connect, Client};
use hyper::header::{HeaderValue, IntoHeaderName};
use hyper::{Body, Method, Request, Response, Uri};
use tokio::timer::Delay;
use tracing::warn;

use crate::{Result, CONTENT_TYPE};
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;

pub struct TestRequest<'a, C: Connect> {
    client: &'a TestClient<C>,
    request: Request<Body>,
}

impl<'a, C: Connect> Deref for TestRequest<'a, C> {
    type Target = Request<Body>;

    fn deref(&self) -> &Request<Body> {
        &self.request
    }
}

impl<'a, C: Connect> DerefMut for TestRequest<'a, C> {
    fn deref_mut(&mut self) -> &mut Request<Body> {
        &mut self.request
    }
}

impl<'a, C: Connect + 'static> TestRequest<'a, C> {
    pub(crate) fn new<U>(client: &'a TestClient<C>, method: Method, uri: U) -> Self
    where
        Uri: HttpTryFrom<U>,
    {
        TestRequest {
            client,
            request: Request::builder().method(method).uri(uri).body(Body::empty()).unwrap(),
        }
    }

    pub fn perform(self) -> Result<Response<Body>> {
        self.client.perform(self)
    }

    pub(crate) fn request(self) -> Request<Body> {
        self.request
    }

    pub fn with_header<N>(mut self, name: N, value: HeaderValue) -> Self
    where
        N: IntoHeaderName,
    {
        self.headers_mut().insert(name, value);
        self
    }
}

pub(crate) trait BodyReader {
    fn read_body(&mut self, response: Response<Body>) -> Result<Vec<u8>>;
}

pub trait Server: Clone {
    fn run_future<F, R, E>(&self, future: F) -> Result<R>
    where
        F: Send + 'static + Future<Item = R, Error = E>,
        R: Send + 'static,
        E: failure::Fail;

    fn request_expiry(&self) -> Delay;

    fn run_request<F>(&self, f: F) -> Result<F::Item>
    where
        F: Future + Send + 'static,
        F::Error: failure::Fail + Sized,
        F::Item: Send,
    {
        let might_expire = self.run_future(f.select2(self.request_expiry()).map_err(|either| {
            let e: failure::Error = match either {
                future::Either::A((req_err, _)) => {
                    warn!("run_request request error: {:?}", req_err);
                    req_err.into()
                }
                future::Either::B((times_up, _)) => {
                    warn!("run_request timed out");
                    times_up.into()
                }
            };
            e.compat()
        }))?;

        match might_expire {
            future::Either::A((item, _)) => Ok(item),
            future::Either::B(_) => Err(failure::err_msg("Timeout")),
        }
    }
}

impl<T: Server> BodyReader for T {
    fn read_body(&mut self, response: Response<Body>) -> Result<Vec<u8>> {
        let f = response.into_body().concat2().map(|chunk| chunk.into_iter().collect());
        self.run_future(f)
    }
}

pub struct TestClient<C: Connect> {
    pub(crate) client: Client<C, Body>,
    pub(crate) rt: Arc<RwLock<Runtime>>,
}

impl<C: Connect + 'static> TestClient<C> {
    pub fn get<U>(&self, uri: U) -> TestRequest<C>
    where
        Uri: HttpTryFrom<U>,
    {
        self.build_request(Method::GET, uri)
    }

    pub fn post<B, U>(&self, uri: U, body: B) -> TestRequest<C>
    where
        B: Into<Body>,
        Uri: HttpTryFrom<U>,
    {
        self.build_request_with_body(Method::POST, uri, body)
    }

    pub fn put<B, U>(&self, uri: U, body: B) -> TestRequest<C>
    where
        B: Into<Body>,
        Uri: HttpTryFrom<U>,
    {
        self.build_request_with_body(Method::PUT, uri, body)
    }

    pub fn build_request<U>(&self, method: Method, uri: U) -> TestRequest<C>
    where
        Uri: HttpTryFrom<U>,
    {
        TestRequest::new(self, method, uri)
    }

    pub fn build_request_with_body<B, U>(&self, method: Method, uri: U, body: B) -> TestRequest<C>
    where
        B: Into<Body>,
        Uri: HttpTryFrom<U>,
    {
        let mut request = self.build_request(method, uri);
        {
            let headers = request.headers_mut();
            headers.insert("CONTENT_TYPE", HeaderValue::from_str(CONTENT_TYPE).unwrap());
        }

        *request.body_mut() = body.into();
        request
    }

    pub fn perform(&self, req: TestRequest<C>) -> Result<Response<Body>> {
        let req_future = self.client.request(req.request()).map_err(|e| {
            warn!("Error from test client request {:?}", e);
            format_err!("request failed: {:?}", e).compat()
        });

        let (tx, rx) = futures::sync::oneshot::channel();
        self.rt
            .write()
            .expect("???")
            .spawn(req_future.then(move |r| tx.send(r).map_err(|_| unreachable!())));

        rx.wait().unwrap().map_err(Into::into)
    }
}

pub struct TestResponse {
    response: Response<Body>,
}

impl Deref for TestResponse {
    type Target = Response<Body>;

    fn deref(&self) -> &Response<Body> {
        &self.response
    }
}

impl DerefMut for TestResponse {
    fn deref_mut(&mut self) -> &mut Response<Body> {
        &mut self.response
    }
}

impl fmt::Debug for TestResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestResponse")
    }
}

impl TestResponse {
    pub fn read_utf8_body(self) -> Result<String> {
        let buf = self.response.into_body().concat2().wait()?;
        let s = String::from_utf8(buf.to_vec())?;
        Ok(s)
    }
}
