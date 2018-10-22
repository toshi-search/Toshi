/// Provides an interface to a Consul cluster
use std::io::{self, Write};
use hyper::{Client, Request};
use hyper::rt::{self, Future, Stream};
use hyper::body::Body;

static CONSUL_PREFIX: &'static str = "services/toshi/";

/// Stub struct for a connection to Consul
pub struct ConsulInterface {
    address: String,
    port: String,
    scheme: String,
    cluster_name: Option<String>,
    node_id: Option<String>,
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

    /// Registers this node with Consul via HTTP
    pub fn register(&mut self, node_id: &str) {
        if let Some(ref cluster_name) = self.cluster_name {
            let uri = self.scheme.clone() + &self.address + ":" + &self.port + "/v1/kv/" + CONSUL_PREFIX + &cluster_name + "/";
            let value: String = "test".to_string();
            let client = Client::new();
            let body = Body::empty();
            let mut req = Request::new(body);
            *req.uri_mut() = uri.parse().unwrap();
            let _ = client.request(req)
                  .map(|res| {
                      println!("Result was: {:?}", res);
                  })
                  .map(|err| {
                      println!("Error registering was: {:?}", err);
                  });
            
        } else {
            println!("No cluster name found!");
        }
    }

    pub fn register_cluster(&mut self) -> impl Future<Item=(), Error=()> {
        let cluster_name = self.cluster_name.clone();
        let cluster_name = cluster_name.unwrap();
        let uri = self.scheme.clone() + "://" + &self.address + ":" + &self.port + "/v1/kv/" + CONSUL_PREFIX + &cluster_name + "/";
        let client = Client::new();
        let body = Body::empty();
        let mut req = Request::new(body);
        *req.uri_mut() = uri.parse().unwrap();
        client.request(req)
                .map(|_| {
                    println!("Worked!");
                    ()
                })
                .map_err(|_| {
                    println!("Error!");
                    ()
                })
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
