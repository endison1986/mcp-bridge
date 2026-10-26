//! Relay: the public HTTP entry point.
//! * `/agent/ws`: agents connect here over WebSocket.
//! * `aggregate.path`: the single endpoint AI clients use. It merges the tools of the local
//!   gateway (mixed mode) and of every connected agent (prefixed with the agent id).

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::{
    config::{ClientAuthType, Config},
    gateway::Gateway,
    jsonrpc::{dispatch, into_result, RpcError, ToolProvider},
    protocol::{ForwardRequest, ForwardResponse},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<ForwardResponse>>>>;

#[derive(Clone)]
struct AgentHandle {
    conn: u64,
    tx: mpsc::UnboundedSender<ForwardRequest>,
    pending: Pending,
}

#[derive(Clone)]
enum ClientAuth {
    Open,
    Bearer(String),
    Path(String),
}

struct Inner {
    auth: ClientAuth,
    agent_token: Option<String>,
    aggregate_path: String,
    separator: String,
    local: Option<Arc<Gateway>>,
    agents: Mutex<HashMap<String, AgentHandle>>,
    next_req: AtomicU64,
    next_conn: AtomicU64,
}

#[derive(Clone)]
pub struct RelayState(Arc<Inner>);

impl RelayState {
    pub fn new(config: &Config, local: Option<Arc<Gateway>>) -> Self {
        let security = &config.security;
        let auth = match (security.enabled, security.client_auth.as_ref()) {
            (true, Some(a)) => match (a.kind, a.token.clone()) {
                (ClientAuthType::Bearer, Some(t)) => ClientAuth::Bearer(t),
                (ClientAuthType::Path, Some(t)) => ClientAuth::Path(t),
                _ => ClientAuth::Open,
            },
            _ => ClientAuth::Open,
        };
        let agent_token = if security.enabled {
            security.agent_token.clone()
        } else {
            None
        };
        Self(Arc::new(Inner {
            auth,
            agent_token,
            aggregate_path: config.aggregate.path.clone(),
            separator: config.aggregate.tool_separator.clone(),
            local,
            agents: Mutex::new(HashMap::new()),
            next_req: AtomicU64::new(1),
            next_conn: AtomicU64::new(1),
        }))
    }

    fn agents_snapshot(&self) -> Vec<(String, AgentHandle)> {
        self.0
            .agents
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Sends a JSON-RPC request to an agent and waits for its response.
    async fn rpc_to_agent(&self, handle: &AgentHandle, method: &str, params: Value) -> Result<Value, RpcError> {
        let req_id = self.0.next_req.fetch_add(1, Ordering::Relaxed);
        let body = json!({ "jsonrpc": "2.0", "id": req_id, "method": method, "params": params }).to_string();
        let (tx, rx) = oneshot::channel();
        handle.pending.lock().unwrap().insert(req_id, tx);
        if handle.tx.send(ForwardRequest { id: req_id, body }).is_err() {
            handle.pending.lock().unwrap().remove(&req_id);
            return Err(RpcError::internal("agent disconnected"));
        }
        let resp = match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => return Err(RpcError::internal("agent dropped the request")),
            Err(_) => {
                handle.pending.lock().unwrap().remove(&req_id);
                return Err(RpcError::internal("agent timed out"));
            }
        };
        let msg: Value = serde_json::from_str(&resp.body)
            .map_err(|e| RpcError::internal(format!("invalid agent response: {e}")))?;
        into_result(&msg)
    }
}

pub fn router(state: RelayState) -> Router {
    let route = match &state.0.auth {
        ClientAuth::Path(_) => format!("{}/:token", state.0.aggregate_path),
        _ => state.0.aggregate_path.clone(),
    };
    Router::new()
        .route("/agent/ws", get(agent_ws))
        .route(&route, any(aggregate_handler))
        .with_state(state)
}

pub async fn serve(listen: SocketAddr, state: RelayState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("relay listening on {listen}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

// ---------- aggregate endpoint ----------

async fn aggregate_handler(
    State(state): State<RelayState>,
    method: Method,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> Response {
    if !client_authorized(&state.0.auth, &headers, &uri) {
        return (StatusCode::UNAUTHORIZED, "invalid client token").into_response();
    }
    if method != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, "use POST with a JSON-RPC body").into_response();
    }
    let text = String::from_utf8_lossy(&body).into_owned();
    match dispatch(&state, &text).await {
        Some(json) => ([(header::CONTENT_TYPE, "application/json")], json).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

fn client_authorized(auth: &ClientAuth, headers: &HeaderMap, uri: &Uri) -> bool {
    match auth {
        ClientAuth::Open => true,
        ClientAuth::Bearer(token) => {
            let expected = format!("Bearer {token}");
            headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) == Some(expected.as_str())
        }
        ClientAuth::Path(token) => uri.path().rsplit('/').next() == Some(token.as_str()),
    }
}

#[async_trait]
impl ToolProvider for RelayState {
    async fn list_tools(&self) -> Result<Vec<Value>, RpcError> {
        let mut tools = Vec::new();
        if let Some(local) = &self.0.local {
            tools.extend(local.list_tools().await?);
        }
        for (agent_id, handle) in self.agents_snapshot() {
            match self.rpc_to_agent(&handle, "tools/list", json!({})).await {
                Ok(result) => {
                    let agent_tools = result
                        .get("tools")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    for mut tool in agent_tools {
                        let original = tool
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        if original.is_empty() {
                            continue;
                        }
                        let name = format!("{agent_id}{}{original}", self.0.separator);
                        if let Some(obj) = tool.as_object_mut() {
                            obj.insert("name".to_string(), Value::String(name));
                        }
                        tools.push(tool);
                    }
                }
                Err(e) => tracing::warn!(agent = %agent_id, error = %e.message, "agent tools/list failed"),
            }
        }
        let mut seen = HashSet::new();
        for tool in &tools {
            let name = tool.get("name").and_then(Value::as_str).unwrap_or_default();
            if !seen.insert(name.to_string()) {
                return Err(RpcError::internal(format!("duplicate tool name '{name}'")));
            }
        }
        Ok(tools)
    }

    async fn call_tool(&self, params: Value) -> Result<Value, RpcError> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if let Some(local) = &self.0.local {
            if local.owns(&name).await? {
                return local.call_tool(params).await;
            }
        }
        for (agent_id, handle) in self.agents_snapshot() {
            let prefix = format!("{agent_id}{}", self.0.separator);
            if let Some(original) = name.strip_prefix(prefix.as_str()) {
                let mut forwarded = params.clone();
                tracing::info!(agent = %agent_id, tool = %original, "forwarding tool call to agent");
                if let Some(obj) = forwarded.as_object_mut() {
                    obj.insert("name".to_string(), Value::String(original.to_string()));
                }
                return self.rpc_to_agent(&handle, "tools/call", forwarded).await;
            }
        }
        Err(RpcError::new(-32602, format!("unknown tool: {name}")))
    }
}

// ---------- agent connections ----------

#[derive(Deserialize)]
struct AgentQuery {
    id: String,
}

async fn agent_ws(
    State(state): State<RelayState>,
    Query(q): Query<AgentQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if let Some(token) = &state.0.agent_token {
        let expected = format!("Bearer {token}");
        if headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) != Some(expected.as_str()) {
            return (StatusCode::UNAUTHORIZED, "invalid agent token").into_response();
        }
    }
    ws.on_upgrade(move |socket| handle_agent(state, q.id, socket))
}

async fn handle_agent(state: RelayState, id: String, socket: WebSocket) {
    let conn = state.0.next_conn.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::unbounded_channel::<ForwardRequest>();
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    state.0.agents.lock().unwrap().insert(
        id.clone(),
        AgentHandle {
            conn,
            tx,
            pending: pending.clone(),
        },
    );
    tracing::info!(agent = %id, "agent connected");

    let (mut sink, mut stream) = socket.split();

    let writer = async {
        while let Some(req) = rx.recv().await {
            let Ok(text) = serde_json::to_string(&req) else {
                continue;
            };
            if sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    };

    let reader = async {
        while let Some(Ok(msg)) = stream.next().await {
            let Message::Text(text) = msg else {
                continue;
            };
            match serde_json::from_str::<ForwardResponse>(&text) {
                Ok(resp) => {
                    let waiter = pending.lock().unwrap().remove(&resp.id);
                    if let Some(w) = waiter {
                        let _ = w.send(resp);
                    }
                }
                Err(e) => tracing::warn!(agent = %id, error = %e, "invalid message from agent"),
            }
        }
    };

    tokio::select! {
        _ = writer => {}
        _ = reader => {}
    }

    // Only remove the entry if it still belongs to this connection
    // (the agent may have already reconnected).
    let mut agents = state.0.agents.lock().unwrap();
    if agents.get(&id).map(|h| h.conn) == Some(conn) {
        agents.remove(&id);
    }
    tracing::info!(agent = %id, "agent disconnected");
}
