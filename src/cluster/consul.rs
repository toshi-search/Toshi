/// Provides an interface to a Consul cluster
use consul::{Client, Service};

/// Stub struct for a connection to Consul
#[derive(Default)]
pub struct ConsulInterface {
    client: Option<Client>,
    address: String,
    port: String,
    scheme: String,
    cluster_name: Option<String>,
    node_id: Option<String>,
}

impl ConsulInterface {
    /// Sets the address of the consul service
    pub fn with_address(self, address: String) -> Self {
        self.address = address;
        self
    }

    /// Sets the port for the Consul HTTP library
    pub fn with_port(self, port: String) -> Self {
        self.port = port;
        self
    }

    /// Sets the scheme (http or https) for the Consul server
    pub fn with_scheme(self, scheme: String) -> Self {
        self.scheme = scheme;
        self
    }

    /// Sets the *Toshi* cluster name
    pub fn with_cluster_name(self, cluster_name: String) -> Self {
        self.cluster_name = Some(cluster_name);
        self
    }

    /// Sets the ID of this specific node in the Toshi cluster
    pub fn with_node_id(self, node_id: String) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Generates a Consul client to make the actual calls to the cluster
    pub fn with_consul_client(self) -> {
        let addr = self.scheme.clone() + "://"+self.address+":"+self.port;
        self.client = Client::new(addr);
        self
    }
}

impl Default for ConsulInterface {
    fn default() -> ConsulInterface {
        ConsulInterface {
            client: None,
            address: String::from("127.0.0.1"),
            port: String::from("8500"),
            scheme: String::from("http"),
            cluster_name: None,
            node_id: None
        }
    }
}
