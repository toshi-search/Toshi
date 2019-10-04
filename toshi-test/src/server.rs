use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use failure::{self, Error};
use futures::Future;
use hyper::client::connect::{Connect, Connected, Destination};
use hyper::client::Client;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::timer::Delay;
use tracing::info;

use crate::{Result, Server, TestClient};

pub struct TestServer {
    timeout: u64,
    runtime: Arc<RwLock<Runtime>>,
}

impl Clone for TestServer {
    fn clone(&self) -> TestServer {
        TestServer {
            timeout: self.timeout,
            runtime: Arc::new(RwLock::new(Runtime::new().unwrap())),
        }
    }
}

impl Server for TestServer {
    fn run_future<F, R, E>(&self, future: F) -> Result<R>
    where
        F: Send + 'static + Future<Item = R, Error = E>,
        R: Send + 'static,
        E: failure::Fail,
    {
        let (tx, rx) = futures::sync::oneshot::channel();
        self.runtime
            .write()
            .expect("???")
            .spawn(future.then(move |r| tx.send(r).map_err(|_| unreachable!())));
        rx.wait().unwrap().map_err(Into::into)
    }

    fn request_expiry(&self) -> Delay {
        Delay::new(Instant::now() + Duration::from_secs(self.timeout))
    }
}

impl TestServer {
    pub fn new<F: Future<Item = (), Error = ()> + Send + 'static>(router: F) -> Result<TestServer> {
        TestServer::with_timeout(router, 10)
    }

    pub fn with_timeout<F: Future<Item = (), Error = ()> + Send + 'static>(router: F, timeout: u64) -> Result<TestServer> {
        let mut runtime = Runtime::new()?;

        runtime.spawn(router);

        let data = TestServer {
            timeout,
            runtime: Arc::new(RwLock::new(runtime)),
        };

        Ok(data)
    }

    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        self.runtime.write().expect("Can't spawn").spawn(fut);
    }

    pub fn client_with_address(&self, client_addr: SocketAddr) -> TestClient<TestConnect> {
        self.try_client_with_address(client_addr)
            .expect("TestServer: unable to spawn client")
    }

    fn try_client_with_address(&self, client_addr: SocketAddr) -> Result<TestClient<TestConnect>> {
        let client = Client::builder().build(TestConnect { addr: client_addr });

        Ok(TestClient {
            rt: Arc::clone(&self.runtime),
            client,
        })
    }
}

pub struct TestConnect {
    pub(crate) addr: SocketAddr,
}

impl Connect for TestConnect {
    type Transport = TcpStream;
    type Error = failure::Error;
    type Future = Box<dyn Future<Item = (Self::Transport, Connected), Error = Self::Error> + Send + Sync>;

    fn connect(&self, _dst: Destination) -> Self::Future {
        Box::new(
            TcpStream::connect(&self.addr)
                .inspect(|s| info!("Client TcpStream connected: {:?}", s))
                .map(|s| (s, Connected::new()))
                .map_err(Error::from),
        )
    }
}

#[cfg(test)]
mod tests {
    use futures::future;
    use hyper::service::service_fn;
    use hyper::{Body, Request, Response, StatusCode};
    use tokio::prelude::*;
    use tracing::info;

    use super::*;

    pub fn svc() -> impl Future<Item = (), Error = ()> + Send {
        let f = move || {
            service_fn(|req: Request<Body>| {
                let path = req.uri().path().to_owned();
                match path.as_str() {
                    "/" => {
                        info!("TestHandler responding to /");
                        let response = Response::builder().status(StatusCode::OK).body(Body::from("Response")).unwrap();

                        future::ok::<Response<Body>, hyper::Error>(response).into_future()
                    }
                    "/echo" => {
                        info!("TestHandler responding to /myaddr");
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from(format!("{}", req.uri())))
                            .unwrap();

                        future::ok(response).into_future()
                    }
                    _ => unreachable!(),
                }
            })
        };

        hyper::Server::bind(&"127.0.0.1:8080".parse::<SocketAddr>().unwrap())
            .tcp_nodelay(true)
            .http1_half_close(false)
            .serve(f)
            .map_err(|e| tracing::error!("HYPER ERROR = {:?}", e))
    }

    #[test]
    fn serves_addr() {
        let new_service = svc();
        let test_server = TestServer::new(new_service).unwrap();
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        let response = test_server
            .client_with_address(addr)
            .get("http://localhost:8080/echo")
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().concat2().wait().unwrap();
        let buf = std::str::from_utf8(&body).unwrap();
        assert_eq!(buf, "/echo");

        let response = test_server
            .client_with_address(addr)
            .get("http://localhost:8080/")
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().concat2().wait().unwrap();
        let buf = std::str::from_utf8(&body).unwrap();
        assert_eq!(buf, "Response");
    }
}
