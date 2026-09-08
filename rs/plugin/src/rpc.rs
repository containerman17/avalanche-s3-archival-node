//! The /rpc handler: rs/rpc's Server over the interim PluginStore.
use crate::node_engine::NodeEngine;

/// One request body in, one response body out (single or batch).
pub fn handle(e: &NodeEngine, body: &[u8]) -> Vec<u8> {
    e.rpc.handle(body)
}
