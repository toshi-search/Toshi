//! Provides an interface to a Consul cluster
use std::fmt::Display;
use std::str::from_utf8;

use bytes::Bytes;
use futures::{stream::Stream, Async, Future, Poll};
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper::http::uri::Scheme;
use hyper::{Client, Request, Response, Uri};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tower::Service;
use tower_consul::{Consul as TowerConsul, ConsulService, KVValue};

use crate::cluster::shard::{PrimaryShard, ReplicaShard, Shard};
use crate::cluster::ClusterError;
use crate::error::Error;
use crate::Result;

pub const SERVICE_NAME: &str = "toshi/";

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub primaries: Vec<PrimaryShard>,
    pub shards: Vec<ReplicaShard>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Hosts(pub Vec<String>);

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

pub trait ClusterOps {
    type Node: DeserializeOwned;
    type Index: DeserializeOwned;

    fn node_path(&self) -> String;
    fn register_node(&mut self) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn place_node_descriptor(&mut self, host: Hosts) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn register_cluster(&mut self) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn register_shard<S: Shard>(&mut self, shard: &S) -> Box<dyn Future<Item = (), Error = ClusterError> + Send>;
    fn get_index(&mut self, index: String, recurse: bool) -> Box<dyn Future<Item = Vec<Self::Index>, Error = ClusterError> + Send>;
    fn nodes(&mut self) -> Box<dyn Future<Item = Vec<Self::Node>, Error = ClusterError> + Send>;
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

impl ClusterOps for Consul {
    type Node = ConsulService;
    type Index = KVValue;

    #[inline]
    fn node_path(&self) -> String {
        SERVICE_NAME.to_string() + &self.cluster_name() + "/" + &self.node_id()
    }

    /// Registers this node with Consul via HTTP API
    fn register_node(&mut self) -> Box<dyn Future<Item = (), Error = ClusterError> + Send> {
        let key = SERVICE_NAME.to_string() + &self.cluster_name() + "/" + &self.node_id();
        Box::new(
            self.client
                .set(&key, Vec::new())
                .map(|_| ())
                .map_err(|err| ClusterError::FailedRegisteringNode(format!("{:?}", err))),
        )
    }

    fn place_node_descriptor(&mut self, hosts: Hosts) -> Box<dyn Future<Item = (), Error = ClusterError> + Send> {
        let key = self.node_path();
        let mut client = self.client.clone();
        Box::new(
            self.client
                .get(&key)
                .then(move |v| match v {
                    Ok(vals) => {
                        let mut h: Vec<Hosts> = vals
                            .iter()
                            .map(|kv| {
                                let decoded_data = base64::decode(&kv.value).unwrap();
                                serde_json::from_str::<Hosts>(from_utf8(&decoded_data).unwrap()).unwrap()
                            })
                            .collect();
                        h.push(hosts);
                        let kvs = serde_json::to_vec(&h).unwrap();
                        tokio::spawn(client.set(&key, kvs).map(|_| ()).map_err(|_| ()));
                        Ok(())
                    }
                    Err(e) => Err(e),
                })
                .map_err(|err| ClusterError::FailedRegisteringCluster(format!("{:?}", err))),
        )
    }

    /// Registers a cluster with Consul via the HTTP API
    fn register_cluster(&mut self) -> Box<dyn Future<Item = (), Error = ClusterError> + Send> {
        let key = SERVICE_NAME.to_string() + &self.cluster_name();
        Box::new(
            self.client
                .set(&key, Vec::new())
                .map(|_| ())
                .map_err(|err| ClusterError::FailedRegisteringCluster(format!("{:?}", err))),
        )
    }

    /// Registers a shard with the Consul cluster
    fn register_shard<S: Shard>(&mut self, shard: &S) -> Box<dyn Future<Item = (), Error = ClusterError> + Send> {
        let key = format!("toshi/{}/{}", self.cluster_name(), shard.shard_id().to_hyphenated_ref());
        let shard = serde_json::to_vec(&shard).unwrap();

        Box::new(
            self.client
                .set(&key, shard)
                .map(|_| ())
                .map_err(|err| ClusterError::FailedCreatingPrimaryShard(format!("{:?}", err))),
        )
    }

    /// Gets the specified index
    fn get_index(&mut self, index: String, recurse: bool) -> Box<dyn Future<Item = Vec<Self::Index>, Error = ClusterError> + Send> {
        let key = format!("toshi/{}/{}?recurse={}", &self.cluster_name(), &index, recurse);
        Box::new(
            self.client
                .get(&key)
                .map_err(|err| ClusterError::FailedGettingIndex(format!("{:?}", err))),
        )
    }

    fn nodes(&mut self) -> Box<dyn Future<Item = Vec<Self::Node>, Error = ClusterError> + Send> {
        Box::new(
            self.client
                .service_nodes("toshi")
                .map_err(|err| ClusterError::FailedFetchingNodes(format!("{:?}", err))),
        )
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
    pub fn with_address<S: Display>(mut self, address: S) -> Self {
        self.address = Some(address.to_string());
        self
    }

    /// Sets the scheme (http or https) for the Consul server
    pub fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = Some(scheme);
        self
    }

    /// Sets the *Toshi* cluster name
    pub fn with_cluster_name<S: Display>(mut self, cluster_name: S) -> Self {
        self.cluster_name = Some(cluster_name.to_string());
        self
    }

    /// Sets the ID of this specific node in the Toshi cluster
    pub fn with_node_id<S: Display>(mut self, node_id: S) -> Self {
        self.node_id = Some(node_id.to_string());
        self
    }

    pub fn build(self) -> Result<Consul> {
        let address = self.address.unwrap_or_else(|| "127.0.0.1:8500".into());
        let scheme = self.scheme.unwrap_or(Scheme::HTTP);

        let client = TowerConsul::new(HttpsService::new(), 100, scheme.to_string(), address.clone()).map_err(|e| {
            dbg!(e);
            Error::SpawnError
        })?;

        Ok(Consul {
            address,
            scheme,
            client,
            cluster_name: self.cluster_name.unwrap_or_else(|| "kitsune".into()),
            node_id: self.node_id.unwrap_or_else(|| "alpha".into()),
        })
    }
}

#[derive(Clone)]
pub struct HttpsService {
    client: Client<HttpConnector>,
}

impl HttpsService {
    fn new() -> Self {
        let https = HttpConnector::new(4);
        let client = Client::builder().build::<_, hyper::Body>(https);

        HttpsService { client }
    }
}

impl Service<Request<Bytes>> for HttpsService {
    type Response = Response<Bytes>;
    type Error = hyper::Error;
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error> + Send>;

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
