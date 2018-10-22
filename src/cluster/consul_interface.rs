/// Provides an interface to a Consul cluster
use hyper;

static CONSUL_PREFIX: &'static str = "services/toshi/";

/// Stub struct for a connection to Consul
pub struct ConsulInterface {
    tokio_handle: Option<Handle>,
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

    pub fn with_handler(mut self, handler: Handle) -> Self {
        self.tokio_handle = Some(handler);
        self
    }

    /// Registers this node with Consul via HTTP
    pub fn register(&mut self, node_id: &str) {
        if let Some(ref cluster_name) = self.cluster_name {
            let keypath = CONSUL_PREFIX.to_string() + &cluster_name + "/";
            let value: String = "test".to_string();
            if let Some(h) = self.tokio_handle.clone() {
                let client = hyper::Client::configure()
                                .keep_alive(true)
                                .build(&h);
            }
        } else {
            println!("No cluster name found!");
        }
    }

    pub fn register_cluster(&mut self) {

    }
}

impl Default for ConsulInterface {
    fn default() -> ConsulInterface {
        ConsulInterface {
            tokio_handle: None,
            address: String::from("127.0.0.1"),
            port: String::from("8500"),
            scheme: String::from("http"),
            cluster_name: None,
            node_id: None
        }
    }
}
