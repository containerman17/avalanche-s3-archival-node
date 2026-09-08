//! The http.proto service behind CreateHandlers: the host proxies every HTTP
//! request to the handler's prefix over gRPC. HandleSimple carries plain
//! requests (headers + body). Handle carries upgrades and HTTP/2 requests
//! the way ghttp's client does: the host runs a responsewriter + reader
//! server for the request; a websocket handler hijacks through it and gets a
//! second server (conn + reader + writer) that streams the client's bytes.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::pb::http::http_server::Http;
use crate::pb::http::responsewriter::writer_client::WriterClient as ResponseWriterClient;
use crate::pb::http::responsewriter::{Header as RwHeader, WriteHeaderRequest, WriteRequest as RwWriteRequest};
use crate::pb::http::{Element, HandleSimpleHttpRequest, HandleSimpleHttpResponse, HttpRequest, HttpResponse};
use crate::pb::io::reader::reader_client::ReaderClient;
use crate::pb::io::reader::ReadRequest;
use crate::pb::net::conn::conn_client::ConnClient;
use crate::pb::net::conn::{ReadRequest as ConnReadRequest, WriteRequest as ConnWriteRequest};

/// One handler: a request body in, a response body out (JSON-RPC).
pub type Handler = Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;
/// Serve one upgraded websocket connection over a byte stream until it closes.
pub type WsServe = Arc<dyn Fn(rpc::ws::BoxStream) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct HttpService {
    pub handler: Handler,
    /// Set on the /ws prefix: every request there is a websocket upgrade.
    pub ws: Option<WsServe>,
}

const READ_CHUNK: i32 = 16 << 10;

fn dial(addr: &str) -> Result<Channel, Status> {
    Ok(Endpoint::from_shared(format!("http://{addr}")).map_err(|e| Status::unknown(e.to_string()))?.tcp_nodelay(true).connect_timeout(Duration::from_secs(5)).connect_lazy())
}

fn header(elems: &[Element], k: &str) -> Option<String> {
    elems.iter().find(|e| e.key.eq_ignore_ascii_case(k)).and_then(|e| e.values.first().cloned())
}

fn rw_headers(hs: &[(&str, &str)]) -> Vec<RwHeader> {
    hs.iter().map(|(k, v)| RwHeader { key: k.to_string(), values: vec![v.to_string()] }).collect()
}

/// A duplex stream over the hijacked connection's Conn service: one task
/// pulls Read(n) answers in, one pushes writes out; EOF on the host side
/// ends the stream, dropping the stream ends the writer.
fn conn_stream(conn: ConnClient<Channel>) -> tokio::io::DuplexStream {
    let (ours, theirs) = tokio::io::duplex(1 << 16);
    let (mut rd, mut wr) = tokio::io::split(theirs);
    let mut c_in = conn.clone();
    tokio::spawn(async move {
        loop {
            let Ok(r) = c_in.read(ConnReadRequest { length: READ_CHUNK }).await else { break };
            let r = r.into_inner();
            if !r.read.is_empty() && wr.write_all(&r.read).await.is_err() {
                break;
            }
            if r.error.is_some() {
                break;
            }
        }
    });
    let mut c_out = conn;
    tokio::spawn(async move {
        let mut buf = vec![0u8; READ_CHUNK as usize];
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if c_out.write(ConnWriteRequest { payload: buf[..n].to_vec() }).await.map(|r| r.into_inner().error.is_some()).unwrap_or(true) {
                        break;
                    }
                }
            }
        }
    });
    ours
}

#[tonic::async_trait]
impl Http for HttpService {
    /// ghttp.Server.Handle: the handler runs against the host's response
    /// writer (WriteHeader / Write / Hijack) and body reader.
    async fn handle(&self, req: Request<HttpRequest>) -> Result<Response<HttpResponse>, Status> {
        let req = req.into_inner();
        let rw = req.response_writer.unwrap_or_default();
        let r = req.request.unwrap_or_default();
        let ch = dial(&rw.server_addr)?;
        let mut w = ResponseWriterClient::new(ch.clone());
        let Some(ws) = &self.ws else {
            // The /rpc handler under an upgrade or HTTP/2 request: rpc.Server.ServeHTTP.
            let raw_query = r.url.as_ref().map(|u| u.raw_query.clone()).unwrap_or_default();
            if r.method == "GET" && r.content_length == 0 && raw_query.is_empty() {
                w.write_header(WriteHeaderRequest { headers: vec![], status_code: 200 }).await?;
                return Ok(Response::new(HttpResponse { header: vec![] }));
            }
            let mut body = Vec::new();
            let mut rd = ReaderClient::new(ch);
            loop {
                let x = rd.read(ReadRequest { length: READ_CHUNK }).await?.into_inner();
                body.extend_from_slice(&x.read);
                if let Some(e) = x.error {
                    if e.error_code != crate::pb::io::reader::ErrorCode::Eof as i32 {
                        return Err(Status::unknown(e.message));
                    }
                    break;
                }
            }
            let handler = self.handler.clone();
            let out = tokio::task::spawn_blocking(move || handler(&body)).await.map_err(|e| Status::internal(e.to_string()))?;
            let headers = rw_headers(&[("Content-Type", "application/json")]);
            w.write(RwWriteRequest { headers: headers.clone(), payload: out }).await?;
            return Ok(Response::new(HttpResponse { header: headers.into_iter().map(|h| Element { key: h.key, values: h.values }).collect() }));
        };
        match rpc::ws::handshake(&r.method, &|k| header(&r.header, k)) {
            Err((code, _reason)) => {
                // gorilla's returnError: http.Error(w, StatusText(code), code).
                let headers = rw_headers(&[("Sec-Websocket-Version", "13"), ("Content-Type", "text/plain; charset=utf-8"), ("X-Content-Type-Options", "nosniff")]);
                w.write_header(WriteHeaderRequest { headers: headers.clone(), status_code: code as i32 }).await?;
                w.write(RwWriteRequest { headers: headers.clone(), payload: format!("{}\n", rpc::ws::status_text(code)).into_bytes() }).await?;
                Ok(Response::new(HttpResponse { header: headers.into_iter().map(|h| Element { key: h.key, values: h.values }).collect() }))
            }
            Ok(resp101) => {
                let hj = w.hijack(()).await?.into_inner();
                let mut conn = ConnClient::new(dial(&hj.server_addr)?);
                conn.write(ConnWriteRequest { payload: resp101.into_bytes() }).await?;
                let stream: rpc::ws::BoxStream = Box::pin(conn_stream(conn.clone()));
                ws(stream).await;
                // Closes the client's socket and stops the host's hijack server.
                let _ = conn.close(()).await;
                Ok(Response::new(HttpResponse { header: vec![] }))
            }
        }
    }

    async fn handle_simple(&self, req: Request<HandleSimpleHttpRequest>) -> Result<Response<HandleSimpleHttpResponse>, Status> {
        let req = req.into_inner();
        if self.ws.is_some() {
            // The host sends non-upgrade requests here: gorilla refuses them.
            let (code, _) = rpc::ws::handshake(&req.method, &|k| header(&req.request_headers, k)).err().unwrap_or((400, ""));
            return Ok(Response::new(HandleSimpleHttpResponse {
                code: code as i32,
                headers: vec![
                    Element { key: "Sec-Websocket-Version".into(), values: vec!["13".into()] },
                    Element { key: "Content-Type".into(), values: vec!["text/plain; charset=utf-8".into()] },
                    Element { key: "X-Content-Type-Options".into(), values: vec!["nosniff".into()] },
                ],
                body: format!("{}\n", rpc::ws::status_text(code)).into_bytes(),
            }));
        }
        let handler = self.handler.clone();
        let body = tokio::task::spawn_blocking(move || handler(&req.body)).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(HandleSimpleHttpResponse {
            code: 200,
            headers: vec![Element { key: "Content-Type".into(), values: vec!["application/json".into()] }],
            body,
        }))
    }
}
