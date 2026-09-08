//! The Otterscan namespace (rpc/otterscan.go).
use serde_json::Value;

use crate::{RpcResult, Server};

pub fn dispatch(_s: &Server, _method: &str, _params: &[Value]) -> Option<RpcResult> {
    None
}
