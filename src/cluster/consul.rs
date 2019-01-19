//! Provides an interface to a Consul cluster

use bytes::Bytes;
use futures::{stream::Stream, Async, Future, Poll};
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper::http::uri::Scheme;
use hyper::{Client, Request, Response, Uri};
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use tower_consul::{Consul as TowerConsul, ConsulService, KVValue};
use tower_service::Service;

use crate::cluster::shard::PrimaryShard;
use crate::cluster::shard::ReplicaShard;
use crate::cluster::shard::Shard;
use crate::cluster::ClusterError;
use crate::{Error, Result};

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub primaries: Vec<PrimaryShard>,
    pub shards: Vec<ReplicaShard>,
}

pub type ConsulClient = TowerConsul<HttpsService>;

/// Consul connection client, clones here are cheap
/// since the entire backing of this is a tower Buffer.
#[derive(Clone)]
pub struct Consul {
    address: String,
    scheme: Scheme,
    cluster_name: String,
    client: ConsulClient,
    node_id: String,
}

impl Consul {
    /// Create a builder instance
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Build the consul uri
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

    pub fn nodes(&mut self) -> impl Future<Item = Vec<ConsulService>, Error = ClusterError> {
        self.client
            .service_nodes("toshi")
            .map_err(|err| ClusterError::FailedFetchingNodes(format!("{:?}", err)))
    }

    /// Gets the specified index
    pub fn get_index(&mut self, index: String, recurse: bool) -> impl Future<Item = Vec<KVValue>, Error = ClusterError> {
        let key = format!("toshi/{}/{}?recurse={}", &self.cluster_name(), &index, recurse);
        self.client
            .get(&key)
            .map_err(|err| ClusterError::FailedGettingIndex(format!("{:?}", err)))
    }

    /// Get a reference to the cluster name
    pub fn cluster_name(&self) -> &String {
        &self.cluster_name
    }

    /// Get a reference to the current node id
    pub fn node_id(&self) -> &String {
        &self.node_id
    }

    /// Set the node id for the current node
    pub fn set_node_id(&mut self, new_id: String) {
        self.node_id = new_id;
    }
}

#[derive(Default, Clone)]
/// Builder struct for Consul
pub struct Builder {
    address: Option<String>,
    scheme: Option<Scheme>,
    cluster_name: Option<String>,
    node_id: Option<String>,
}

impl Builder {
    /// Sets the address of the consul service
    pub fn with_address(mut self, address: String) -> Self {
        self.address = Some(address);
        self
    }

    /// Sets the scheme (http or https) for the Consul server
    pub fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = Some(scheme);
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

    pub fn build(self) -> Result<Consul> {
        let address = self.address.unwrap_or("127.0.0.1:8500".parse().unwrap());
        let scheme = self.scheme.unwrap_or(Scheme::HTTP);

        let client = TowerConsul::new(HttpsService::new(), 100, scheme.to_string(), address.to_string()).map_err(|_| Error::SpawnError)?;

        Ok(Consul {
            address,
            scheme,
            client,
            cluster_name: self.cluster_name.unwrap_or("kitsune".into()),
            node_id: self.node_id.unwrap_or("alpha".into()),
        })
    }
}

#[derive(Clone)]
pub struct HttpsService {
    client: Client<HttpsConnector<HttpConnector>>,
}

impl HttpsService {
    fn new() -> Self {
        let https = HttpsConnector::new(4).expect("Could not create TLS for Hyper");
        let client = Client::builder().build::<_, hyper::Body>(https);

        HttpsService { client }
    }
}

impl Service<Request<Bytes>> for HttpsService {
    type Response = Response<Bytes>;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let f = self
            .client
            .request(req.map(Body::from))
            .and_then(|res| {
                let status = res.status();
                let headers = res.headers().clone();

                res.into_body().concat2().join(Ok((status, headers)))
            })
            .and_then(|(body, (status, _headers))| Ok(Response::builder().status(status).body(body.into()).unwrap()));

        Box::new(f)
    }
}
