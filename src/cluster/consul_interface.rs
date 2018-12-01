//! Provides an interface to a Consul cluster

use log::error;

use hyper::body::Body;
use hyper::rt::Future;
use hyper::{Client, Request};
use serde_json;

use cluster::shard::Shard;
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
    pub fn register_node(&mut self) -> impl Future<Item = (), Error = ClusterError> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/" + &self.node_id() + "/";
        let client = Client::new();
        let req = self.put_request(&uri, Body::empty());
        client.request(req).map(|_| ()).map_err(|e| {
            error!("Error registering node: {:?}", e);
            std::process::exit(1);
        })
    }

    /// Registers a cluster with Consul via the HTTP API
    pub fn register_cluster(&self) -> impl Future<Item = (), Error = ClusterError> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/";
        let client = Client::new();
        let req = self.put_request(&uri, Body::empty());
        client.request(req).map(|_| ()).map_err(|_| ClusterError::FailedRegisteringNode)
    }

    /// Registers a shard with the Consul cluster
    pub fn register_shard<T: Shard + serde::Serialize>(&mut self, shard: &T) -> impl Future<Item = (), Error = ()> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/" + &shard.shard_id().to_hyphenated_ref().to_string() + "/";
        let client = Client::new();
        let json_body = serde_json::to_string(&shard).unwrap();
        let req = self.put_request(&uri, json_body);
        client.request(req).map(|_| ()).map_err(|e| {
            error!("Error registering shard: {:?}", e);
            std::process::exit(1);
        })
    }

    fn base_consul_url(&self) -> String {
        self.scheme.clone() + "://" + &self.address + ":" + &self.port + "/v1/kv/" + CONSUL_PREFIX
    }

    fn put_request<T>(&self, uri: &str, payload: T) -> Request<Body>
    where
        hyper::Body: std::convert::From<T>,
    {
        Request::builder().method("PUT").uri(uri).body(Body::from(payload)).unwrap()
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
            address: "127.0.0.1".into(),
            port: "8500".into(),
            scheme: String::from("http"),
            cluster_name: Some(String::from("kitsune")),
            node_id: Some(String::from("alpha")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body::Payload;

    #[test]
    fn test_create_consul_interface() {
        let consul = ConsulInterface::default();
        assert_eq!(consul.base_consul_url(), "http://127.0.0.1:8500/v1/kv/services/toshi/");
    }

    #[test]
    fn test_consul_cluster_name() {
        let consul = ConsulInterface::default()
            .with_cluster_name("kitsune".to_string())
            .with_address("127.0.0.1".into())
            .with_node_id("alpha".into())
            .with_scheme("http".into())
            .with_port("8500".into());
        assert_eq!(consul.cluster_name(), "kitsune");
    }

    #[test]
    fn test_consul_cluster_put() {
        let consul = ConsulInterface::default();
        let test_req = consul.put_request(&consul.base_consul_url(), Body::empty());
        assert_eq!(test_req.body().content_length().unwrap(), 0);
    }

    #[test]
    fn test_register_node() {
        let mut consul = ConsulInterface::default();
        let _ = consul.register_node();
    }

    #[test]
    fn test_register_cluster() {
        let consul = ConsulInterface::default();
        let _ = consul.register_cluster();
    }
}
