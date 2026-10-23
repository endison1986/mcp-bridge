//! Agent: runs next to the local MCP servers and keeps a WebSocket open to the relay.
//! Every JSON-RPC request from the relay is answered by the local gateway.

use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use crate::{
    gateway::Gateway,
    jsonrpc::dispatch,
    protocol::{ForwardRequest, ForwardResponse},
};

pub struct AgentConfig {
    /// e.g. `wss://relay.example.com/agent/ws`
    pub relay_ws: String,
    pub agent_id: String,
    pub token: Option<String>,
    pub gateway: Arc<Gateway>,
}

/// Connects to the relay and reconnects forever on failure.
pub async fn run(cfg: AgentConfig) -> anyhow::Result<()> {
    loop {
        match connect_once(&cfg).await {
            Ok(()) => tracing::warn!("relay closed the connection"),
            Err(e) => tracing::warn!(error = %e, "relay connection failed"),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn connect_once(cfg: &AgentConfig) -> anyhow::Result<()> {
    let url = format!("{}?id={}", cfg.relay_ws, cfg.agent_id);
    let mut request = url.into_client_request()?;
    if let Some(token) = &cfg.token {
        let auth: HeaderValue = format!("Bearer {token}").parse()?;
        request.headers_mut().insert("authorization", auth);
    }

    let (ws, _) = connect_async(request).await?;
    tracing::info!(relay = %cfg.relay_ws, agent = %cfg.agent_id, "connected to relay");

    let (mut sink, mut stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

    loop {
        tokio::select! {
            incoming = stream.next() => {
                let Some(msg) = incoming else {
                    return Ok(());
                };
                let Message::Text(text) = msg? else {
                    continue;
                };
                let req: ForwardRequest = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(error = %e, "invalid message from relay");
                        continue;
                    }
                };
                let gateway = cfg.gateway.clone();
                let out_tx = out_tx.clone();
                tokio::spawn(async move {
                    let body = dispatch(gateway.as_ref(), &req.body).await.unwrap_or_default();
                    let resp = ForwardResponse { id: req.id, body };
                    if let Ok(text) = serde_json::to_string(&resp) {
                        let _ = out_tx.send(text);
                    }
                });
            }
            Some(text) = out_rx.recv() => {
                sink.send(Message::Text(text)).await?;
            }
        }
    }
}
