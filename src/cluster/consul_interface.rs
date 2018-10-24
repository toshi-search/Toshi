/// Provides an interface to a Consul cluster
use std::io::{self, Write};
use hyper::{Client, Request};
use hyper::client::Builder;
use hyper::rt::{self, Future, Stream};
use hyper::body::Body;

use cluster::ClusterError;

static CONSUL_PREFIX: &'static str = "services/toshi/";

/// Stub struct for a connection to Consul
pub struct ConsulInterface {
    address: String,
    port: String,
    scheme: String,
    cluster_name: Option<String>,
    pub node_id: Option<String>,
}

impl ConsulInterface {
    /// Sets the address of the consul service
    pub fn with_address(mut self, address: String) -> Self {
        self.address = address;
        self
    }

    /// Sets the port for the Consul HTTP library
    pub fn with_port(mut self, port: String) -> Self {
        self.port = port;
        self
    }

    /// Sets the scheme (http or https) for the Consul server
    pub fn with_scheme(mut self, scheme: String) -> Self {
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

    /// Registers this node with Consul via HTTP API
    pub fn register_node(&mut self) -> impl Future<Item=(), Error=()> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/" + &self.node_id() + "/";
        let client = Client::new();
        let req = self.put_request(&uri);
        client.request(req)
                .map(|_| {
                    ()
                })
                .map_err(|_| {
                    ()
                })
    }

    /// Registers a cluster with Consul via the HTTP API
    pub fn register_cluster(&mut self) -> impl Future<Item=(), Error=()> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/";
        let client = Client::new();
        let req = self.put_request(&uri);
        client.request(req)
                .map(|_| {
                    ()
                })
                .map_err(|_| {
                    ()
                })
    }

    fn base_consul_url(&self) -> String {
        self.scheme.clone() + "://" + &self.address + ":" + &self.port + "/v1/kv/" + CONSUL_PREFIX
    }

    fn put_request(&self, uri: &str) -> Request<Body> {
        Request::builder()
            .method("PUT")
            .uri(uri)
            .body(Body::empty())
            .unwrap()
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
        ConsulInterface {
            address: String::from("127.0.0.1"),
            port: String::from("8500"),
            scheme: String::from("http"),
            cluster_name: None,
            node_id: None
        }
    }
}
