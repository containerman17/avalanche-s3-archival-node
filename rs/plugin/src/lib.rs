//! epochdb-rs: a subnet-evm follower VM served over avalanchego's rpcchainvm
//! (protocol 45), verbatim from the proto files under `proto/`.
pub mod genesis;
pub mod ghttp;
pub mod layered;
pub mod log;
pub mod node_engine;
pub mod rpc;
pub mod tree;
pub mod vm;

/// The generated protobuf and gRPC code, one module per proto package.
pub mod pb {
    pub mod vm {
        tonic::include_proto!("vm");
        pub mod runtime {
            tonic::include_proto!("vm.runtime");
        }
    }
    pub mod http {
        tonic::include_proto!("http");
    }
    pub mod validatorstate {
        tonic::include_proto!("validatorstate");
    }
    pub mod appsender {
        tonic::include_proto!("appsender");
    }
    pub mod sharedmemory {
        tonic::include_proto!("sharedmemory");
    }
    pub mod rpcdb {
        tonic::include_proto!("rpcdb");
    }
    pub mod warp {
        tonic::include_proto!("warp");
    }
    pub mod aliasreader {
        tonic::include_proto!("aliasreader");
    }
    pub mod io {
        pub mod prometheus {
            pub mod client {
                tonic::include_proto!("io.prometheus.client");
            }
        }
    }
}

/// RPCChainVM protocol version of the avalanchego the protos were copied from.
pub const RPCCHAINVM_PROTOCOL: u32 = 45;
pub const VERSION: &str = concat!("epochdb-rs/", env!("CARGO_PKG_VERSION"), " [rpcchainvm=45]");
