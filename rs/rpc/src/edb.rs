//! The edb_ namespace (rpc/edb.go, rpc/tokens.go).
use serde_json::Value;

use crate::{RpcResult, Server};

pub fn dispatch(_s: &Server, _method: &str, _params: &[Value]) -> Option<RpcResult> {
    None
}
