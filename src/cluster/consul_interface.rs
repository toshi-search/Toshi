//! Provides an interface to a Consul cluster
use crate::cluster::shard::Shard;
use crate::cluster::ClusterError;
use crate::{Error, Result};

use crate::cluster::shard::PrimaryShard;
use crate::cluster::shard::ReplicaShard;
use futures::{future, stream::Stream, Async, Future, Poll};
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper::http::uri::Scheme;
use hyper::{Client, Method, Request, Response, Uri};
use hyper_tls::HttpsConnector;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use std::vec::IntoIter;
use tower_buffer::Buffer;
use tower_consul::{Consul, KVValue};
use tower_service::Service;

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub primaries: Vec<PrimaryShard>,
    pub shards: Vec<ReplicaShard>,
}

pub type ConsulClient = Consul<Buffer<HttpsService, Request<Vec<u8>>>>;

/// Stub struct for a connection to Consul
#[derive(Clone)]
pub struct ConsulInterface {
    address: String,
    scheme: Scheme,
    cluster_name: Option<String>,
    client: ConsulClient,
    pub node_id: Option<String>,
}

impl ConsulInterface {
    /// Sets the address of the consul service
    pub fn with_address(mut self, address: String) -> Self {
        self.address = address;
        self
    }

    /// Sets the scheme (http or https) for the Consul server
    pub fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = scheme;
        self
    }

    /// Sets the *Toshi* cluster name
    pub fn with_cluster_name(mut self, cluster_name: String) -> Self {
        self.cluster_name = Some(cluster_name);
        self
    }

    /// Sets the ID of this specific node in the Toshi cluster
    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn build_uri(self) -> Result<Uri> {
        Uri::builder()
            .scheme(self.scheme.clone())
            .authority(self.address.as_bytes())
            .build()
            .map_err(|err| Error::IOError(err.to_string()))
    }

    /// Registers this node with Consul via HTTP API
    pub fn register_node(&mut self) -> impl Future<Item = (), Error = ClusterError> {
        let key = "toshi/".to_owned() + &self.cluster_name() + "/" + &self.node_id();
        self.client
            .set(&key, Vec::new())
            .map(|_| ())
            .map_err(|err| ClusterError::FailedRegisteringNode(format!("{:?}", err)))
    }

    /// Registers a cluster with Consul via the HTTP API
    pub fn register_cluster(&mut self) -> impl Future<Item = (), Error = ClusterError> {
        let key = "toshi/".to_owned() + &self.cluster_name();
        self.client
            .set(&key, Vec::new())
            .map(|_| ())
            .map_err(|err| ClusterError::FailedRegisteringNode(format!("{:?}", err)))
    }

    /// Registers a shard with the Consul cluster
    pub fn register_shard<T: Shard + Serialize>(&mut self, shard: &T) -> impl Future<Item = (), Error = ClusterError> {
        let key = format!("toshi/{}/{}", self.cluster_name(), shard.shard_id().to_hyphenated_ref());
        let shard = serde_json::to_vec(&shard).unwrap();

        self.client
            .set(&key, shard)
            .map(|_| ())
            .map_err(|err| ClusterError::FailedCreatingPrimaryShard(format!("{:?}", err)))
    }

    /// Gets the specified index
    pub fn get_index(&mut self, index: String, recurse: bool) -> impl Future<Item = Vec<KVValue>, Error = ClusterError> {
        let key = format!("toshi/{}/{}?recurse={}", &self.cluster_name(), &index, recurse);
        self.client
            .get(&key)
            .map_err(|err| ClusterError::FailedGettingIndex(format!("{:?}", err)))
    }

    fn cluster_name(&self) -> String {
        self.cluster_name.clone().unwrap()
    }

    fn node_id(&self) -> String {
        self.node_id.clone().unwrap()
    }
}

impl Default for ConsulInterface {
    fn default() -> ConsulInterface {
        let client = match Buffer::new(HttpsService::new(), 100) {
            Ok(c) => c,
            Err(_) => panic!("Unable to spawn"),
        };

        ConsulInterface {
            address: "127.0.0.1:8500".into(),
            scheme: Scheme::HTTP,
            cluster_name: Some(String::from("kitsune")),
            node_id: Some(String::from("alpha")),
            client: Consul::new(client, "127.0.0.1:8500".into()),
        }
    }
}

pub struct HttpsService {
    client: Client<HttpsConnector<HttpConnector>>,
}

impl HttpsService {
    pub fn new() -> Self {
        let https = HttpsConnector::new(4).expect("Could not create TLS for Hyper");
        let client = Client::builder().build::<_, hyper::Body>(https);

        HttpsService { client }
    }
}

impl Service<Request<Vec<u8>>> for HttpsService {
    type Response = Response<Vec<u8>>;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Request<Vec<u8>>) -> Self::Future {
        let f = self
            .client
            .request(req.map(Body::from))
            .and_then(|res| {
                let status = res.status().clone();
                let headers = res.headers().clone();

                res.into_body().concat2().join(Ok((status, headers)))
            })
            .and_then(|(body, (status, _headers))| {
                Ok(Response::builder()
                    .status(status)
                    // .headers(headers)
                    .body(body.to_vec())
                    .unwrap())
            });

        Box::new(f)
    }
}
