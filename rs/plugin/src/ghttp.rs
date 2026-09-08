//! The http.proto service behind CreateHandlers: the host proxies every HTTP
//! request to the handler's prefix over gRPC. HandleSimple carries plain
//! requests (headers + body); Handle carries upgrades (websockets) through
//! reader/writer streams and is not served here.
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::pb::http::http_server::Http;
use crate::pb::http::{Element, HandleSimpleHttpRequest, HandleSimpleHttpResponse, HttpRequest, HttpResponse};

/// One handler: a request body in, a response body out (JSON-RPC).
pub type Handler = Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

pub struct HttpService {
    pub handler: Handler,
}

#[tonic::async_trait]
impl Http for HttpService {
    async fn handle(&self, _: Request<HttpRequest>) -> Result<Response<HttpResponse>, Status> {
        Err(Status::unimplemented("epochdb-rs: upgrade (websocket) requests are not served"))
    }

    async fn handle_simple(&self, req: Request<HandleSimpleHttpRequest>) -> Result<Response<HandleSimpleHttpResponse>, Status> {
        let req = req.into_inner();
        let handler = self.handler.clone();
        let body = tokio::task::spawn_blocking(move || handler(&req.body)).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(HandleSimpleHttpResponse {
            code: 200,
            headers: vec![Element { key: "Content-Type".into(), values: vec!["application/json".into()] }],
            body,
        }))
    }
}
