//! Provides an interface to a Consul cluster
use crate::cluster::shard::Shard;
use crate::cluster::ClusterError;
use crate::{Error, Result};

use crate::cluster::shard::PrimaryShard;
use crate::cluster::shard::ReplicaShard;
use futures::stream::Stream;
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper::http::uri::Scheme;
use hyper::rt::Future;
use hyper::Method;
use hyper::{Client, Request, Uri};
use hyper_tls::HttpsConnector;
use serde_derive::{Deserialize, Serialize};
use std::vec::IntoIter;

static CONSUL_PREFIX: &'static str = "/v1/kv/services/toshi/";

#[derive(Serialize, Deserialize)]
pub struct NodeData {
    pub primaries: Vec<PrimaryShard>,
    pub shards: Vec<ReplicaShard>,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize)]
pub struct ConsulKey {
    pub LockIndex: i32,
    pub Key: String,
    pub Flags: i32,
    pub Value: Option<NodeData>,
    pub CreateIndex: i32,
    pub ModifyIndex: i32,
}

#[derive(Serialize, Deserialize)]
pub struct ConsulResponse(Vec<ConsulKey>);

impl ConsulResponse {
    pub fn get(self) -> IntoIter<ConsulKey> {
        self.0.into_iter()
    }
}

/// Stub struct for a connection to Consul
#[derive(Clone, Debug)]
pub struct ConsulInterface {
    address: String,
    scheme: Scheme,
    cluster_name: Option<String>,
    client: Client<HttpsConnector<HttpConnector>>,
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
            .path_and_query(CONSUL_PREFIX)
            .build()
            .map_err(|err| Error::IOError(err.to_string()))
    }

    /// Registers this node with Consul via HTTP API
    pub fn register_node(&mut self) -> impl Future<Item = (), Error = ClusterError> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/" + &self.node_id() + "/";
        self.put_request(&uri, Body::empty())
            .map_err(|err| ClusterError::FailedRegisteringNode(err.to_string()))
    }

    /// Registers a cluster with Consul via the HTTP API
    pub fn register_cluster(&self) -> impl Future<Item = (), Error = ClusterError> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/";
        self.put_request(&uri, Body::empty())
            .map_err(|err| ClusterError::FailedRegisteringCluster(err.to_string()))
    }

    /// Registers a shard with the Consul cluster
    pub fn register_shard<T: Shard + serde::Serialize>(&mut self, shard: &T) -> impl Future<Item = (), Error = ClusterError> {
        let uri = self.base_consul_url() + &self.cluster_name() + "/" + &shard.shard_id().to_hyphenated_ref().to_string() + "/";
        let json_body = serde_json::to_string(&shard).unwrap();
        self.put_request(&uri, json_body)
            .map_err(|err| ClusterError::FailedCreatingPrimaryShard(err.to_string()))
    }

    pub fn get_index(&mut self, index: String, recurse: bool) -> impl Future<Item = ConsulResponse, Error = ClusterError> {
        let uri = format!("{}{}/{}?recurse={}", self.base_consul_url(), &self.cluster_name(), &index, recurse)
            .parse::<Uri>()
            .unwrap();
        self.get_request(uri)
    }

    fn base_consul_url(&self) -> String {
        Uri::builder()
            .scheme(self.scheme.clone())
            .authority(self.address.as_bytes())
            .path_and_query(CONSUL_PREFIX)
            .build()
            .expect("Problem building base Consul URL")
            .to_string()
    }

    fn put_request<T>(&self, uri: &str, payload: T) -> impl Future<Item = (), Error = hyper::Error>
    where
        hyper::Body: std::convert::From<T>,
    {
        let req = Request::builder().method(Method::PUT).uri(uri).body(Body::from(payload)).unwrap();
        self.client.request(req).map(|_| ())
    }

    fn get_request(&self, uri: Uri) -> impl Future<Item = ConsulResponse, Error = ClusterError> {
        self.client
            .get(uri)
            .and_then(|r| r.into_body().concat2())
            .map_err(|err| ClusterError::ErrorInConsulResponse(err.to_string()))
            .and_then(|b| {
                serde_json::from_slice::<ConsulResponse>(&b.to_vec()).map_err(|err| ClusterError::ErrorParsingConsulJSON(err.to_string()))
            })
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
            address: "127.0.0.1:8500".into(),
            scheme: Scheme::HTTP,
            cluster_name: Some(String::from("kitsune")),
            node_id: Some(String::from("alpha")),
            client: {
                let https = HttpsConnector::new(4).expect("Could not create TLS for Hyper");
                Client::builder().build::<_, hyper::Body>(https)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consul_deserialize() {
        let payload = r#"
        [
            {
                "LockIndex": 0,
                "Key": "services/toshi/kitsune/",
                "Flags": 0,
                "Value": null,
                "CreateIndex": 22,
                "ModifyIndex": 22
            },
            {
                "LockIndex": 0,
                "Key": "services/toshi/kitsune/f793f695-c75d-4073-9dcc-0f47b311ee5f/",
                "Flags": 0,
                "Value": null,
                "CreateIndex": 23,
                "ModifyIndex": 23
            }
        ]"#;

        let keys: ConsulResponse = serde_json::from_str(payload).unwrap();

        //        println!("{:?}", keys)
    }

    #[test]
    fn test_create_consul_interface() {
        let consul = ConsulInterface::default();
        assert_eq!(consul.base_consul_url(), "http://127.0.0.1:8500/v1/kv/services/toshi/");
    }

    #[test]
    fn test_consul_cluster_name() {
        let consul = ConsulInterface::default()
            .with_cluster_name("kitsune".into())
            .with_address("127.0.0.1:8500".into())
            .with_node_id("alpha".into())
            .with_scheme(Scheme::HTTP);
        assert_eq!(consul.cluster_name(), "kitsune");
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
