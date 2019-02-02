//! Provides an interface to a Consul cluster

use bytes::Bytes;
use futures::{future, stream::Stream, Async, Future, Poll};
use http::uri::Scheme;
use hyper::client::{conn, connect::Destination, HttpConnector};
use hyper::{Body, Request, Response, Uri};
use hyper_tls::{HttpsConnector, MaybeHttpsStream};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio::net::TcpStream;
use tower_consul::{Consul as TowerConsul, ConsulService, KVValue};
use tower_direct_service::DirectService;
use tower_hyper::{util::MakeService, Connect, ConnectError, Connection};
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

pub type ConsulClient = TowerConsul<BytesService>;

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
            .map_err(|err| ClusterError::FailedRegisteringCluster(format!("{:?}", err)))
    }

    /// Registers a shard with the Consul cluster
    pub fn register_shard<T>(&mut self, shard: &T) -> impl Future<Item = (), Error = ClusterError>
    where
        T: Shard + Serialize,
    {
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
    destination: Option<Uri>,
    cluster_name: Option<String>,
    node_id: Option<String>,
}

impl Builder {
    /// Sets the Uri for the consul destination
    ///
    /// Must contain Schema and Authority, and must be
    /// absolute or else build will fail.
    pub fn with_destination(mut self, uri: Uri) -> Self {
        self.destination = Some(uri);
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

    pub fn build(self) -> impl Future<Item = Consul, Error = ClusterError> {
        future::lazy(|| {
            let uri = self.destination.unwrap_or_else(|| Uri::from_static("http://127.0.0.1:8500"));
            let destination = Destination::try_from_uri(uri).expect("Unable to build consul uri");
            let addr = destination.host().to_string();
            let scheme = destination.scheme();

            let cluster_name = self.cluster_name.unwrap_or_else(|| "kitsune".into());
            let node_id = self.node_id.unwrap_or_else(|| "alpha".into());
            let scheme = Scheme::from_str(scheme).expect("This is a bug because this should be a valid scheme");

            let https_connector = HttpsConnector::new(1).expect("Unable to spawn https connector");
            let mut connector = Connect::new(https_connector, conn::Builder::new());

            connector
                .make_service(destination.clone())
                .map_err(|e| ClusterError::FailedConnectingToConsul(format!("{}", e)))
                .and_then(move |conn| {
                    let http_client = BytesService::new(conn);

                    let client =
                        TowerConsul::new(http_client, 100, scheme.to_string(), addr.clone()).expect("Unable to spawn consul client");

                    Ok(Consul {
                        address: addr,
                        scheme,
                        client,
                        cluster_name,
                        node_id,
                    })
                })
        })
    }
}

pub struct BytesService {
    conn: Connection<MaybeHttpsStream<TcpStream>, Body>,
}

impl BytesService {
    fn new(conn: Connection<MaybeHttpsStream<TcpStream>, Body>) -> Self {
        BytesService { conn }
    }
}

impl Service<Request<Bytes>> for BytesService {
    type Response = Response<Bytes>;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.conn.poll_ready()
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let f = self
            .conn
            .call(req.map(Body::from))
            .and_then(|res| {
                let status = res.status();
                let headers = res.headers().clone();

                res.into_body().concat2().join(Ok((status, headers)))
            })
            .and_then(|(body, (status, _headers))| Ok(Response::builder().status(status).body(body.into()).unwrap()));

        Box::new(f)
    }
}
