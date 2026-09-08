//! The vm.proto service, method for method after avalanchego's
//! vms/rpcchainvm/vm_server.go, for a follower VM: no BuildBlock, no App
//! messages, no state sync, WaitForEvent blocks until Shutdown.
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

use crate::ghttp::{Handler, HttpService};
use crate::pb::http::http_server::HttpServer;
use crate::pb::rpcdb::database_client::DatabaseClient;
use crate::pb::validatorstate::validator_state_client::ValidatorStateClient;
use crate::pb::vm::runtime::runtime_client::RuntimeClient;
use crate::pb::vm::vm_server::{Vm, VmServer};
use crate::pb::vm::*;
use crate::pb::vm::Error as PbError;
use crate::tree::{hex, Engine, Error, Id, Tree};
use crate::{RPCCHAINVM_PROTOCOL, VERSION};

pub const ENGINE_ADDR_ENV: &str = "AVALANCHE_VM_RUNTIME_ENGINE_ADDR";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The host-side services the plugin can call back (validatorstate for warp
/// later; rpcdb only for Health, the VM keeps its state under chain_data_dir).
#[derive(Clone)]
pub struct Host {
    pub db: DatabaseClient<Channel>,
    pub validator_state: ValidatorStateClient<Channel>,
}

/// Everything Initialize hands the VM.
pub struct Init {
    pub network_id: u32,
    pub subnet_id: Id,
    pub chain_id: Id,
    pub node_id: Vec<u8>,
    pub public_key: Vec<u8>,
    pub x_chain_id: Id,
    pub c_chain_id: Id,
    pub avax_asset_id: Id,
    pub chain_data_dir: String,
    pub genesis_bytes: Vec<u8>,
    pub upgrade_bytes: Vec<u8>,
    pub config_bytes: Vec<u8>,
    pub host: Host,
}

pub type Factory<E> = Box<dyn FnOnce(&Init) -> Result<E, Error> + Send>;

pub struct VmService<E: Engine> {
    factory: Mutex<Option<Factory<E>>>,
    tree: OnceLock<Arc<Tree<E>>>,
    host: OnceLock<Host>,
    state: Mutex<State>,
    closed: Notify,
    is_closed: AtomicBool,
    /// The Go server's allowShutdown: signals are ignored until Shutdown.
    pub allow_shutdown: Arc<AtomicBool>,
    handler_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

fn id32(b: &[u8], what: &str) -> Result<Id, Status> {
    b.try_into().map_err(|_| Status::unknown(format!("{what}: expected 32 bytes, got {}", b.len())))
}

fn ts(secs: u64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds: secs as i64, nanos: 0 })
}

fn unknown(e: Error) -> Status {
    Status::unknown(e.to_string())
}

fn channel(addr: &str) -> Result<Channel, Status> {
    Ok(Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| Status::unknown(e.to_string()))?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .connect_lazy())
}

/// Server settings after grpcutils.DefaultServerOptions: unbounded message
/// sizes (set per service), keepalive as the Go server.
fn server() -> Server {
    Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(2 * 3600)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)))
}

/// TCP_NODELAY on every accepted connection (grpc-go sets it on its side;
/// without it Nagle plus the client's delayed ACK cost 40 ms per round trip,
/// 20 blk/s). The builder's tcp_nodelay is ignored by serve_with_incoming.
fn incoming(listener: TcpListener) -> TcpIncoming {
    TcpIncoming::from(listener).with_nodelay(Some(true))
}

impl<E: Engine> VmService<E> {
    pub fn new(factory: Factory<E>) -> VmService<E> {
        VmService {
            factory: Mutex::new(Some(factory)),
            tree: OnceLock::new(),
            host: OnceLock::new(),
            state: Mutex::new(State::Unspecified),
            closed: Notify::new(),
            is_closed: AtomicBool::new(false),
            allow_shutdown: Arc::new(AtomicBool::new(false)),
            handler_tasks: Mutex::new(Vec::new()),
        }
    }

    fn tree(&self) -> Result<Arc<Tree<E>>, Status> {
        self.tree.get().cloned().ok_or_else(|| Status::failed_precondition("vm not initialized"))
    }

    /// The last accepted block as Initialize and SetState report it.
    fn last_response(&self) -> Result<(Vec<u8>, Vec<u8>, u64, Vec<u8>, Option<prost_types::Timestamp>), Status> {
        let t = self.tree()?;
        let b = t.last_accepted();
        let m = t.engine.meta(&b);
        Ok((m.id.to_vec(), m.parent.to_vec(), m.height, t.engine.bytes(&b).to_vec(), ts(m.timestamp)))
    }

    async fn parse_one(&self, bytes: Vec<u8>) -> Result<ParseBlockResponse, Status> {
        let t = self.tree()?;
        let b = tokio::task::spawn_blocking(move || t.parse(Bytes::from(bytes)).map(|b| t.engine.meta(&b)))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(unknown)?;
        Ok(ParseBlockResponse { id: b.id.to_vec(), parent_id: b.parent.to_vec(), height: b.height, timestamp: ts(b.timestamp), verify_with_context: false })
    }
}

#[tonic::async_trait]
impl<E: Engine> Vm for VmService<E> {
    async fn initialize(&self, req: Request<InitializeRequest>) -> Result<Response<InitializeResponse>, Status> {
        let r = req.into_inner();
        let Some(factory) = self.factory.lock().unwrap().take() else {
            return Err(Status::failed_precondition("vm already initialized"));
        };
        let db = channel(&r.db_server_addr)?;
        let srv = channel(&r.server_addr)?;
        let host = Host {
            db: DatabaseClient::new(db).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX),
            validator_state: ValidatorStateClient::new(srv).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX),
        };
        let init = Init {
            network_id: r.network_id,
            subnet_id: id32(&r.subnet_id, "subnet_id")?,
            chain_id: id32(&r.chain_id, "chain_id")?,
            node_id: r.node_id,
            public_key: r.public_key,
            x_chain_id: id32(&r.x_chain_id, "x_chain_id")?,
            c_chain_id: id32(&r.c_chain_id, "c_chain_id")?,
            avax_asset_id: id32(&r.avax_asset_id, "avax_asset_id")?,
            chain_data_dir: r.chain_data_dir,
            genesis_bytes: r.genesis_bytes,
            upgrade_bytes: r.upgrade_bytes,
            config_bytes: r.config_bytes,
            host: host.clone(),
        };
        let engine = tokio::task::spawn_blocking(move || factory(&init)).await.map_err(|e| Status::internal(e.to_string()))?.map_err(unknown)?;
        let _ = self.tree.set(Arc::new(Tree::new(engine)));
        let _ = self.host.set(host);
        let (last_accepted_id, last_accepted_parent_id, height, bytes, timestamp) = self.last_response()?;
        eprintln!("epochdb-rs: initialized, last accepted height={height} id={}", hex(&last_accepted_id.as_slice().try_into().unwrap()));
        Ok(Response::new(InitializeResponse { last_accepted_id, last_accepted_parent_id, height, bytes, timestamp }))
    }

    async fn set_state(&self, req: Request<SetStateRequest>) -> Result<Response<SetStateResponse>, Status> {
        let st = req.into_inner().state();
        *self.state.lock().unwrap() = st;
        eprintln!("epochdb-rs: state {}", st.as_str_name());
        let (last_accepted_id, last_accepted_parent_id, height, bytes, timestamp) = self.last_response()?;
        Ok(Response::new(SetStateResponse { last_accepted_id, last_accepted_parent_id, height, bytes, timestamp }))
    }

    async fn shutdown(&self, _: Request<()>) -> Result<Response<()>, Status> {
        self.allow_shutdown.store(true, Ordering::SeqCst);
        if self.is_closed.swap(true, Ordering::SeqCst) {
            return Ok(Response::new(()));
        }
        if let Some(t) = self.tree.get() {
            let t = t.clone();
            tokio::task::spawn_blocking(move || t.engine.shutdown()).await.map_err(|e| Status::internal(e.to_string()))?;
        }
        self.closed.notify_waiters();
        for h in self.handler_tasks.lock().unwrap().drain(..) {
            h.abort();
        }
        eprintln!("epochdb-rs: shutdown");
        Ok(Response::new(()))
    }

    async fn create_handlers(&self, _: Request<()>) -> Result<Response<CreateHandlersResponse>, Status> {
        let t = self.tree()?;
        let handler: Handler = Arc::new(move |body: &[u8]| t.engine.rpc(body));
        let mut handlers = Vec::new();
        for prefix in ["/rpc"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| Status::internal(e.to_string()))?;
            let addr = listener.local_addr().map_err(|e| Status::internal(e.to_string()))?;
            let svc = HttpServer::new(HttpService { handler: handler.clone() }).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX);
            let task = tokio::spawn(async move {
                if let Err(e) = server().add_service(svc).serve_with_incoming(incoming(listener)).await {
                    eprintln!("epochdb-rs: handler server {addr}: {e}");
                }
            });
            self.handler_tasks.lock().unwrap().push(task);
            handlers.push(crate::pb::vm::Handler { prefix: prefix.into(), server_addr: addr.to_string() });
        }
        Ok(Response::new(CreateHandlersResponse { handlers }))
    }

    async fn new_http_handler(&self, _: Request<()>) -> Result<Response<NewHttpHandlerResponse>, Status> {
        Ok(Response::new(NewHttpHandlerResponse { server_addr: String::new() }))
    }

    /// A follower never has an event: block until Shutdown, then fail the
    /// call the way a cancelled context does in Go.
    async fn wait_for_event(&self, _: Request<()>) -> Result<Response<WaitForEventResponse>, Status> {
        if !self.is_closed.load(Ordering::SeqCst) {
            self.closed.notified().await;
        }
        Err(Status::cancelled("context canceled"))
    }

    async fn connected(&self, _: Request<ConnectedRequest>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn disconnected(&self, _: Request<DisconnectedRequest>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn build_block(&self, _: Request<BuildBlockRequest>) -> Result<Response<BuildBlockResponse>, Status> {
        Err(Status::unknown("epochdb-rs: a follower builds no blocks"))
    }

    async fn parse_block(&self, req: Request<ParseBlockRequest>) -> Result<Response<ParseBlockResponse>, Status> {
        Ok(Response::new(self.parse_one(req.into_inner().bytes).await?))
    }

    async fn get_block(&self, req: Request<GetBlockRequest>) -> Result<Response<GetBlockResponse>, Status> {
        let id = id32(&req.into_inner().id, "id")?;
        let t = self.tree()?;
        let Some(b) = t.get_block(&id) else {
            return Ok(Response::new(GetBlockResponse { err: PbError::NotFound as i32, ..Default::default() }));
        };
        let m = t.engine.meta(&b);
        Ok(Response::new(GetBlockResponse {
            parent_id: m.parent.to_vec(),
            bytes: t.engine.bytes(&b).to_vec(),
            height: m.height,
            timestamp: ts(m.timestamp),
            err: PbError::Unspecified as i32,
            verify_with_context: false,
        }))
    }

    async fn set_preference(&self, _: Request<SetPreferenceRequest>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    /// {"database": <rpcdb health>, "health": <engine health>}, as the Go server reports.
    async fn health(&self, _: Request<()>) -> Result<Response<HealthResponse>, Status> {
        let t = self.tree()?;
        let vm_health = t.engine.health().map_err(unknown)?;
        let mut db = self.host.get().ok_or_else(|| Status::failed_precondition("vm not initialized"))?.db.clone();
        let db_health = db.health_check(()).await?.into_inner().details;
        let db_health: serde_json::Value = serde_json::from_slice(&db_health).unwrap_or(serde_json::Value::Null);
        let report = serde_json::json!({"database": db_health, "health": vm_health});
        Ok(Response::new(HealthResponse { details: report.to_string().into_bytes() }))
    }

    async fn version(&self, _: Request<()>) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(VersionResponse { version: VERSION.into() }))
    }

    async fn app_request(&self, _: Request<AppRequestMsg>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn app_request_failed(&self, _: Request<AppRequestFailedMsg>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn app_response(&self, _: Request<AppResponseMsg>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn app_gossip(&self, _: Request<AppGossipMsg>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn gather(&self, _: Request<()>) -> Result<Response<GatherResponse>, Status> {
        Ok(Response::new(GatherResponse { metric_families: Vec::new() }))
    }

    /// block.GetAncestors' local logic: the block, then its parents, up to
    /// max_blocks_num and max_blocks_size (each counted with a 4-byte length).
    async fn get_ancestors(&self, req: Request<GetAncestorsRequest>) -> Result<Response<GetAncestorsResponse>, Status> {
        let r = req.into_inner();
        let t = self.tree()?;
        let mut id = id32(&r.blk_id, "blk_id")?;
        let Some(mut b) = t.get_block(&id) else {
            return Ok(Response::new(GetAncestorsResponse { blks_bytes: Vec::new() }));
        };
        let deadline = std::time::Instant::now() + Duration::from_nanos(r.max_blocks_retrival_time.max(0) as u64);
        let mut out = vec![t.engine.bytes(&b).to_vec()];
        let mut size = out[0].len() + 4;
        while (out.len() as i32) < r.max_blocks_num && std::time::Instant::now() < deadline {
            id = t.engine.meta(&b).parent;
            let Some(p) = t.get_block(&id) else { break };
            let bytes = t.engine.bytes(&p);
            if size + bytes.len() + 4 > r.max_blocks_size as usize {
                break;
            }
            size += bytes.len() + 4;
            out.push(bytes.to_vec());
            b = p;
        }
        Ok(Response::new(GetAncestorsResponse { blks_bytes: out }))
    }

    async fn batched_parse_block(&self, req: Request<BatchedParseBlockRequest>) -> Result<Response<BatchedParseBlockResponse>, Status> {
        let t = self.tree()?;
        let raws = req.into_inner().request;
        let response = tokio::task::spawn_blocking(move || {
            raws.into_iter()
                .map(|raw| {
                    let b = t.parse(Bytes::from(raw))?;
                    let m = t.engine.meta(&b);
                    Ok(ParseBlockResponse { id: m.id.to_vec(), parent_id: m.parent.to_vec(), height: m.height, timestamp: ts(m.timestamp), verify_with_context: false })
                })
                .collect::<Result<Vec<_>, crate::tree::Error>>()
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(unknown)?;
        Ok(Response::new(BatchedParseBlockResponse { response }))
    }

    async fn get_block_id_at_height(&self, req: Request<GetBlockIdAtHeightRequest>) -> Result<Response<GetBlockIdAtHeightResponse>, Status> {
        let t = self.tree()?;
        Ok(Response::new(match t.block_id_at_height(req.into_inner().height) {
            Some(id) => GetBlockIdAtHeightResponse { blk_id: id.to_vec(), err: PbError::Unspecified as i32 },
            None => GetBlockIdAtHeightResponse { blk_id: [0u8; 32].to_vec(), err: PbError::NotFound as i32 },
        }))
    }

    async fn state_sync_enabled(&self, _: Request<()>) -> Result<Response<StateSyncEnabledResponse>, Status> {
        Ok(Response::new(StateSyncEnabledResponse { enabled: false, err: PbError::Unspecified as i32 }))
    }

    async fn get_ongoing_sync_state_summary(&self, _: Request<()>) -> Result<Response<GetOngoingSyncStateSummaryResponse>, Status> {
        Ok(Response::new(GetOngoingSyncStateSummaryResponse { err: PbError::StateSyncNotImplemented as i32, ..Default::default() }))
    }

    async fn get_last_state_summary(&self, _: Request<()>) -> Result<Response<GetLastStateSummaryResponse>, Status> {
        Ok(Response::new(GetLastStateSummaryResponse { err: PbError::StateSyncNotImplemented as i32, ..Default::default() }))
    }

    async fn parse_state_summary(&self, _: Request<ParseStateSummaryRequest>) -> Result<Response<ParseStateSummaryResponse>, Status> {
        Ok(Response::new(ParseStateSummaryResponse { err: PbError::StateSyncNotImplemented as i32, ..Default::default() }))
    }

    async fn get_state_summary(&self, _: Request<GetStateSummaryRequest>) -> Result<Response<GetStateSummaryResponse>, Status> {
        Ok(Response::new(GetStateSummaryResponse { err: PbError::StateSyncNotImplemented as i32, ..Default::default() }))
    }

    async fn block_verify(&self, req: Request<BlockVerifyRequest>) -> Result<Response<BlockVerifyResponse>, Status> {
        let r = req.into_inner();
        let t = self.tree()?;
        let m = tokio::task::spawn_blocking(move || {
            let b = t.parse(Bytes::from(r.bytes))?;
            t.verify(b, r.p_chain_height)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(unknown)?;
        Ok(Response::new(BlockVerifyResponse { timestamp: ts(m.timestamp) }))
    }

    async fn block_accept(&self, req: Request<BlockAcceptRequest>) -> Result<Response<()>, Status> {
        let id = id32(&req.into_inner().id, "id")?;
        let t = self.tree()?;
        tokio::task::spawn_blocking(move || t.accept(&id)).await.map_err(|e| Status::internal(e.to_string()))?.map_err(unknown)?;
        Ok(Response::new(()))
    }

    async fn block_reject(&self, req: Request<BlockRejectRequest>) -> Result<Response<()>, Status> {
        let id = id32(&req.into_inner().id, "id")?;
        self.tree()?.reject(&id);
        Ok(Response::new(()))
    }

    async fn state_summary_accept(&self, _: Request<StateSummaryAcceptRequest>) -> Result<Response<StateSummaryAcceptResponse>, Status> {
        Ok(Response::new(StateSummaryAcceptResponse {
            mode: state_summary_accept_response::Mode::Skipped as i32,
            err: PbError::StateSyncNotImplemented as i32,
        }))
    }
}

/// Serve is rpcchainvm.Serve: listen on 127.0.0.1:0, tell the runtime engine
/// (env AVALANCHE_VM_RUNTIME_ENGINE_ADDR) our protocol version and address,
/// serve until Shutdown and then a SIGTERM (signals before Shutdown are
/// ignored, as the Go plugin does).
pub async fn serve<E: Engine>(factory: Factory<E>) -> Result<(), Error> {
    let runtime_addr = std::env::var(ENGINE_ADDR_ENV).map_err(|_| format!("required env var missing: {ENGINE_ADDR_ENV:?}"))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;

    let svc = Arc::new(VmService::new(factory));
    let allow_shutdown = svc.allow_shutdown.clone();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).expect("SIGINT handler");
        loop {
            let name = tokio::select! {
                _ = term.recv() => "terminated",
                _ = int.recv() => "interrupt",
            };
            if !allow_shutdown.load(Ordering::SeqCst) || name == "interrupt" {
                eprintln!("runtime engine: ignoring signal: {name}");
                continue;
            }
            eprintln!("runtime engine: received shutdown signal: {name}");
            let _ = stop_tx.send(());
            return;
        }
    });

    let mut runtime = RuntimeClient::new(channel(&runtime_addr)?);
    tokio::time::timeout(HANDSHAKE_TIMEOUT, runtime.initialize(runtime::InitializeRequest { protocol_version: RPCCHAINVM_PROTOCOL, addr: addr.to_string() }))
        .await
        .map_err(|_| "runtime handshake timed out")?
        .map_err(|e| format!("failed to initialize vm runtime: {e}"))?;

    let vm = VmServer::from_arc(svc).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX);
    server()
        .add_service(vm)
        .serve_with_incoming_shutdown(incoming(listener), async {
            let _ = stop_rx.await;
        })
        .await?;
    eprintln!("vm server: graceful termination success");
    Ok(())
}
