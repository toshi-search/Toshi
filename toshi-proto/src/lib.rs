pub mod placement_proto {

    pub use client::*;
    pub use server::*;

    #[cfg(target_family = "unix")]
    include!(concat!(env!("OUT_DIR"), "/placement.rs"));
    #[cfg(target_family = "windows")]
    include!(concat!(env!("OUT_DIR"), "\\placement.rs"));
}

pub mod cluster_rpc {

    pub use client::*;
    pub use server::*;

    #[cfg(target_family = "unix")]
    include!(concat!(env!("OUT_DIR"), "/cluster_rpc.rs"));
    #[cfg(target_family = "windows")]
    include!(concat!(env!("OUT_DIR"), "\\cluster_rpc.rs"));
}
