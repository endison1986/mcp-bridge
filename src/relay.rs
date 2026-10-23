//! Relay: public HTTP entry point. Agents connect in over WebSocket, and
//! MCP clients reach an agent at `/mcp/{agent_id}`.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};

use crate::protocol::{ForwardRequest, ForwardResponse, FORWARDED_HEADERS};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<ForwardResponse>>>>;

#[derive(Clone)]
struct AgentHandle {
    conn: u64,
    tx: mpsc::UnboundedSender<ForwardRequest>,
    pending: Pending,
}

struct Inner {
    agent_token: String,
    agents: Mutex<HashMap<String, AgentHandle>>,
    next_req: AtomicU64,
    next_conn: AtomicU64,
}

#[derive(Clone)]
pub struct RelayState(Arc<Inner>);

impl RelayState {
    pub fn new(agent_token: String) -> Self {
        Self(Arc::new(Inner {
            agent_token,
            agents: Mutex::new(HashMap::new()),
            next_req: AtomicU64::new(1),
            next_conn: AtomicU64::new(1),
        }))
    }
}

pub fn router(state: RelayState) -> Router {
    Router::new()
        .route("/agent/ws", get(agent_ws))
        .route("/mcp/:id", any(mcp_proxy))
        .with_state(state)
}

pub async fn serve(listen: SocketAddr, state: RelayState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("relay listening on {listen}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

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
    let expected = format!("Bearer {}", state.0.agent_token);
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(expected.as_str()) {
        return (StatusCode::UNAUTHORIZED, "invalid agent token").into_response();
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

async fn mcp_proxy(
    State(state): State<RelayState>,
    Path(id): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Server-initiated SSE streams are not supported in this version.
    if method == Method::GET {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            "GET (SSE stream) is not supported by the relay",
        )
            .into_response();
    }

    let handle = state.0.agents.lock().unwrap().get(&id).cloned();
    let Some(handle) = handle else {
        return (StatusCode::NOT_FOUND, "agent not connected").into_response();
    };

    let req_id = state.0.next_req.fetch_add(1, Ordering::Relaxed);
    let fwd_headers: Vec<(String, String)> = FORWARDED_HEADERS
        .iter()
        .filter_map(|name| {
            headers
                .get(*name)
                .and_then(|v| v.to_str().ok())
                .map(|v| (name.to_string(), v.to_string()))
        })
        .collect();

    let (resp_tx, resp_rx) = oneshot::channel();
    handle.pending.lock().unwrap().insert(req_id, resp_tx);

    let req = ForwardRequest {
        id: req_id,
        method: method.to_string(),
        headers: fwd_headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    };
    if handle.tx.send(req).is_err() {
        handle.pending.lock().unwrap().remove(&req_id);
        return (StatusCode::BAD_GATEWAY, "agent disconnected").into_response();
    }

    match tokio::time::timeout(REQUEST_TIMEOUT, resp_rx).await {
        Ok(Ok(resp)) => build_response(resp),
        Ok(Err(_)) => (StatusCode::BAD_GATEWAY, "agent dropped the request").into_response(),
        Err(_) => {
            handle.pending.lock().unwrap().remove(&req_id);
            (StatusCode::GATEWAY_TIMEOUT, "agent timed out").into_response()
        }
    }
}

fn build_response(resp: ForwardResponse) -> Response {
    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for (k, v) in &resp.headers {
        if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from(resp.body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
