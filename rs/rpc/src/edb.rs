//! THE edb_ NAMESPACE (rpc/edb.go): the posting-list log reads of tokens.rs
//! over JSON-RPC. Every method takes ONE object parameter (the fields are
//! named, and most are optional), and the paged ones answer
//! {logs, more, nextCursor} with the same keyset rule as ots_: cursor is the
//! TxNum to continue from, 0 means the end the walk starts at, newest-first
//! unless ascending.
//!
//!     edb_getLogsByEmitter            {emitter, topic0?, cursor?, limit?, ascending?}
//!     edb_getLogsByTopicValue         {value, topic0?, positions?, cursor?, limit?, ascending?}
//!     edb_getTopicGroups              {value, topic0}
//!     edb_getTokenTransfersByHolder   {address, standard, cursor?, limit?, ascending?}
//!     edb_getTokenTransfersByContract {token, standard, cursor?, limit?, ascending?}
//!     edb_getTokenContracts           {address}
use alloy_primitives::{Address, B256};
use serde_json::Value;

use crate::{invalid, RpcResult, Server};

pub fn dispatch(s: &Server, method: &str, params: &[Value]) -> Option<RpcResult> {
    if !matches!(
        method,
        "edb_getLogsByEmitter" | "edb_getLogsByTopicValue" | "edb_getTopicGroups" | "edb_getTokenTransfersByHolder" | "edb_getTokenTransfersByContract" | "edb_getTokenContracts"
    ) {
        return None;
    }
    if params.len() != 1 {
        return Some(Err(invalid(format!("{method} takes one object parameter"))));
    }
    let p = match Params::parse(&params[0]) {
        Ok(p) => p,
        Err(e) => return Some(Err(invalid(format!("bad parameter: {e}")))),
    };
    let desc = !p.ascending;
    Some(match method {
        "edb_getLogsByEmitter" => s.logs_by_emitter(p.emitter, p.topic0, p.cursor, p.limit, desc),
        "edb_getLogsByTopicValue" => s.logs_by_topic_value(p.value, p.topic0, p.positions, p.cursor, p.limit, desc),
        "edb_getTopicGroups" => match p.topic0 {
            None => Err("topic0 is required".into()),
            Some(t) => s.topic_groups(p.value, t),
        },
        "edb_getTokenTransfersByHolder" => s.token_transfers_by_holder(p.address, &p.standard, p.cursor, p.limit, desc),
        "edb_getTokenTransfersByContract" => s.token_transfers_by_contract(p.token, &p.standard, p.cursor, p.limit, desc),
        "edb_getTokenContracts" => s.token_contracts(p.address),
        _ => unreachable!(),
    })
}

/// The one object parameter, field for field with the Go struct: every field
/// optional, a missing address or value being the zero one.
struct Params {
    emitter: Address,
    token: Address,
    address: Address,
    value: B256,
    topic0: Option<B256>,
    positions: u8,
    standard: String,
    cursor: u64,
    limit: i64,
    ascending: bool,
}

/// encoding/json's words for the value kinds a field can be handed.
fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// One fixed-size hex field, with encoding/json's struct-field decoration on
/// the errors it re-renders as an UnmarshalTypeError.
fn fixed<const N: usize>(v: Option<&Value>, field: &str, ty: &str) -> Result<Option<[u8; N]>, String> {
    let Some(v) = v else { return Ok(None) };
    match crate::tokens::unmarshal_fixed(v, N) {
        Ok(b) => Ok(Some(<[u8; N]>::try_from(&b[..]).unwrap())),
        Err((msg, true)) => Err(format!("json: cannot unmarshal {msg} into Go struct field edbParams.{field} of type {ty}")),
        Err((msg, false)) => Err(msg.replace("TYPE", ty)),
    }
}

impl Params {
    fn parse(v: &Value) -> Result<Params, String> {
        let o = v.as_object().ok_or_else(|| format!("json: cannot unmarshal {} into Go value of type rpc.edbParams", kind(v)))?;
        let num = |k: &str, ty: &str, max: u64| -> Result<u64, String> {
            match o.get(k) {
                None | Some(Value::Null) => Ok(0),
                Some(x) => match x.as_u64() {
                    Some(n) if n <= max => Ok(n),
                    _ => Err(format!("json: cannot unmarshal {} {} into Go struct field edbParams.{k} of type {ty}", kind(x), x)),
                },
            }
        };
        Ok(Params {
            emitter: fixed::<20>(o.get("emitter"), "emitter", "common.Address")?.map(Address::from).unwrap_or_default(),
            token: fixed::<20>(o.get("token"), "token", "common.Address")?.map(Address::from).unwrap_or_default(),
            address: fixed::<20>(o.get("address"), "address", "common.Address")?.map(Address::from).unwrap_or_default(),
            value: fixed::<32>(o.get("value"), "value", "common.Hash")?.map(B256::from).unwrap_or_default(),
            topic0: fixed::<32>(o.get("topic0").filter(|v| !v.is_null()), "topic0", "common.Hash")?.map(B256::from),
            positions: num("positions", "uint8", u8::MAX as u64)? as u8,
            standard: match o.get("standard") {
                None | Some(Value::Null) => String::new(),
                Some(x) => x.as_str().ok_or_else(|| format!("json: cannot unmarshal {} into Go struct field edbParams.standard of type string", kind(x)))?.to_string(),
            },
            cursor: num("cursor", "uint64", u64::MAX)?,
            limit: match o.get("limit") {
                None | Some(Value::Null) => 0,
                Some(x) => x.as_i64().ok_or_else(|| format!("json: cannot unmarshal {} {x} into Go struct field edbParams.limit of type int", kind(x)))?,
            },
            ascending: match o.get("ascending") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(x) => return Err(format!("json: cannot unmarshal {} into Go struct field edbParams.ascending of type bool", kind(x))),
            },
        })
    }
}
