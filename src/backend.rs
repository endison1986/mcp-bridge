//! Connections to individual MCP servers: a child process over stdio, or an HTTP endpoint.

use std::{
    collections::{BTreeMap, HashMap},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{oneshot, Mutex},
};

use crate::{
    config::ServerConfig,
    jsonrpc::{into_result, RpcError, PROTOCOL_VERSION},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>;

pub enum Backend {
    Stdio(StdioBackend),
    Http(HttpBackend),
}

impl Backend {
    pub fn from_config(name: &str, cfg: &ServerConfig) -> Self {
        match (&cfg.command, &cfg.url) {
            (Some(cmd), _) => Backend::Stdio(StdioBackend::new(name, cmd.clone(), cfg.env.clone())),
            (None, Some(url)) => Backend::Http(HttpBackend::new(name, url.clone(), cfg.headers.clone())),
            // Config validation guarantees exactly one of the two for enabled servers.
            (None, None) => unreachable!("server '{name}' has neither command nor url"),
        }
    }

    /// Sends one JSON-RPC request and returns its `result`.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        match self {
            Backend::Stdio(b) => b.request(method, params).await,
            Backend::Http(b) => b.request(method, params).await,
        }
    }
}

// ---------- stdio ----------

pub struct StdioBackend {
    name: String,
    command: Vec<String>,
    env: BTreeMap<String, String>,
    session: Mutex<Option<Arc<StdioSession>>>,
    next_id: AtomicU64,
}

struct StdioSession {
    stdin: Mutex<ChildStdin>,
    pending: Pending,
    alive: Arc<AtomicBool>,
    _child: Child, // kill_on_drop: the process dies with the session
}

impl StdioBackend {
    fn new(name: &str, command: Vec<String>, env: BTreeMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            command,
            env,
            session: Mutex::new(None),
            next_id: AtomicU64::new(1),
        }
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let session = self.session().await?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        send_request(&session, id, method, params).await
    }

    /// Returns the running session, starting (and initializing) the process if needed.
    async fn session(&self) -> Result<Arc<StdioSession>, RpcError> {
        let mut guard = self.session.lock().await;
        if let Some(s) = guard.as_ref() {
            if s.alive.load(Ordering::SeqCst) {
                return Ok(s.clone());
            }
        }
        let session = Arc::new(self.spawn().map_err(|e| RpcError::internal(e))?);
        let init_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        send_request(
            &session,
            init_id,
            "initialize",
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "mcp-bridge", "version": env!("CARGO_PKG_VERSION") }
            }),
        )
        .await?;
        send_notification(&session, "notifications/initialized").await?;
        tracing::info!(server = %self.name, "stdio server started");
        *guard = Some(session.clone());
        Ok(session)
    }

    fn spawn(&self) -> anyhow::Result<StdioSession> {
        let (program, args) = self
            .command
            .split_first()
            .ok_or_else(|| anyhow!("server '{}' has an empty command", self.name))?;
        let mut child = Command::new(program)
            .args(args)
            .envs(&self.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting server '{}'", self.name))?;
        let stdin = child.stdin.take().context("child stdin unavailable")?;
        let stdout = child.stdout.take().context("child stdout unavailable")?;
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        tokio::spawn(read_loop(
            self.name.clone(),
            stdout,
            pending.clone(),
            alive.clone(),
        ));
        Ok(StdioSession {
            stdin: Mutex::new(stdin),
            pending,
            alive,
            _child: child,
        })
    }
}

async fn read_loop(name: String, stdout: ChildStdout, pending: Pending, alive: Arc<AtomicBool>) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let is_response = msg.get("result").is_some() || msg.get("error").is_some();
        if let (true, Some(id)) = (is_response, msg.get("id").and_then(Value::as_u64)) {
            let waiter = pending.lock().await.remove(&id);
            if let Some(tx) = waiter {
                let _ = tx.send(msg);
            }
        }
    }
    alive.store(false, Ordering::SeqCst);
    pending.lock().await.clear();
    tracing::warn!(server = %name, "stdio server exited");
}

async fn send_request(
    session: &StdioSession,
    id: u64,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    let (tx, rx) = oneshot::channel();
    session.pending.lock().await.insert(id, tx);
    let line = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
    if let Err(e) = write_line(session, &line).await {
        session.pending.lock().await.remove(&id);
        return Err(RpcError::internal(e));
    }
    match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
        Ok(Ok(msg)) => into_result(&msg),
        Ok(Err(_)) => Err(RpcError::internal("server exited before responding")),
        Err(_) => {
            session.pending.lock().await.remove(&id);
            Err(RpcError::internal("server request timed out"))
        }
    }
}

async fn send_notification(session: &StdioSession, method: &str) -> Result<(), RpcError> {
    let line = json!({ "jsonrpc": "2.0", "method": method }).to_string();
    write_line(session, &line).await.map_err(|e| RpcError::internal(e))
}

async fn write_line(session: &StdioSession, line: &str) -> std::io::Result<()> {
    let mut stdin = session.stdin.lock().await;
    stdin.write_all(line.as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await
}

// ---------- http ----------

pub struct HttpBackend {
    name: String,
    url: String,
    headers: BTreeMap<String, String>,
    client: reqwest::Client,
    state: Mutex<HttpState>,
    next_id: AtomicU64,
}

#[derive(Default)]
struct HttpState {
    ready: bool,
    session_id: Option<String>,
}

impl HttpBackend {
    fn new(name: &str, url: String, headers: BTreeMap<String, String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            name: name.to_string(),
            url,
            headers,
            client,
            state: Mutex::new(HttpState::default()),
            next_id: AtomicU64::new(1),
        }
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let session_id = self.ensure_ready().await?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let resp = self.post(&body, session_id.as_deref()).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            // The server forgot our session; initialize again on the next call.
            *self.state.lock().await = HttpState::default();
        }
        parse_response(resp, id).await
    }

    async fn ensure_ready(&self) -> Result<Option<String>, RpcError> {
        let mut state = self.state.lock().await;
        if state.ready {
            return Ok(state.session_id.clone());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let init = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "mcp-bridge", "version": env!("CARGO_PKG_VERSION") }
            }
        });
        let resp = self.post(&init, None).await?;
        let session_id = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        parse_response(resp, id).await?;

        let note = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        let resp = self.post(&note, session_id.as_deref()).await?;
        if !resp.status().is_success() {
            return Err(RpcError::internal(format!(
                "{}: initialized notification rejected ({})",
                self.name,
                resp.status()
            )));
        }

        state.ready = true;
        state.session_id = session_id.clone();
        tracing::info!(server = %self.name, "http server initialized");
        Ok(session_id)
    }

    async fn post(&self, body: &Value, session_id: Option<&str>) -> Result<reqwest::Response, RpcError> {
        let mut req = self
            .client
            .post(&self.url)
            .header("accept", "application/json, text/event-stream")
            .json(body);
        for (key, value) in &self.headers {
            req = req.header(key.as_str(), value.as_str());
        }
        if let Some(sid) = session_id {
            req = req.header("mcp-session-id", sid);
        }
        req.send()
            .await
            .map_err(|e| RpcError::internal(format!("{}: {e}", self.name)))
    }
}

async fn parse_response(resp: reqwest::Response, id: u64) -> Result<Value, RpcError> {
    let status = resp.status();
    let is_sse = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"));
    let text = resp.text().await.map_err(|e| RpcError::internal(e))?;
    if !status.is_success() {
        let snippet: String = text.chars().take(200).collect();
        return Err(RpcError::internal(format!("HTTP {status}: {snippet}")));
    }
    let messages: Vec<Value> = if is_sse {
        text.lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .filter_map(|d| serde_json::from_str(d.trim()).ok())
            .collect()
    } else if text.trim().is_empty() {
        Vec::new()
    } else {
        vec![serde_json::from_str(&text)
            .map_err(|e| RpcError::internal(format!("invalid JSON from server: {e}")))?]
    };
    let msg = messages
        .into_iter()
        .find(|m| m.get("id").and_then(Value::as_u64) == Some(id))
        .ok_or_else(|| RpcError::internal("no matching response from server"))?;
    into_result(&msg)
}
