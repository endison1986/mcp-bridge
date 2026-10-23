//! Agent: runs next to a local MCP service and keeps a WebSocket open to the relay.
//! Every request the relay sends is replayed against the local MCP endpoint.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use crate::protocol::{ForwardRequest, ForwardResponse, RESPONSE_HEADERS};

pub struct AgentConfig {
    /// e.g. `wss://relay.example.com/agent/ws`
    pub relay_ws: String,
    pub agent_id: String,
    pub token: String,
    /// e.g. `http://127.0.0.1:3000/mcp`
    pub target: String,
}

/// Connects to the relay and reconnects forever on failure.
pub async fn run(cfg: AgentConfig) -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    loop {
        match connect_once(&cfg, &http).await {
            Ok(()) => tracing::warn!("relay closed the connection"),
            Err(e) => tracing::warn!(error = %e, "relay connection failed"),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn connect_once(cfg: &AgentConfig, http: &reqwest::Client) -> anyhow::Result<()> {
    let url = format!("{}?id={}", cfg.relay_ws, cfg.agent_id);
    let mut request = url.into_client_request()?;
    let auth: HeaderValue = format!("Bearer {}", cfg.token).parse()?;
    request.headers_mut().insert("authorization", auth);

    let (ws, _) = connect_async(request).await?;
    tracing::info!(relay = %cfg.relay_ws, target = %cfg.target, "connected to relay");

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
                        tracing::warn!(error = %e, "invalid request from relay");
                        continue;
                    }
                };
                tracing::debug!(id = req.id, method = %req.method, "forwarding request");

                let http = http.clone();
                let target = cfg.target.clone();
                let out_tx = out_tx.clone();
                tokio::spawn(async move {
                    let resp = forward(&http, &target, req).await;
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

pub async fn forward(http: &reqwest::Client, target: &str, req: ForwardRequest) -> ForwardResponse {
    let method = reqwest::Method::from_bytes(req.method.as_bytes()).unwrap_or(reqwest::Method::POST);
    let mut builder = http.request(method, target).body(req.body);
    for (k, v) in &req.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }

    match builder.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let headers: Vec<(String, String)> = RESPONSE_HEADERS
                .iter()
                .filter_map(|name| {
                    resp.headers()
                        .get(*name)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| (name.to_string(), v.to_string()))
                })
                .collect();
            let body = resp.text().await.unwrap_or_default();
            ForwardResponse {
                id: req.id,
                status,
                headers,
                body,
            }
        }
        Err(e) => ForwardResponse {
            id: req.id,
            status: 502,
            headers: vec![],
            body: format!("upstream error: {e}"),
        },
    }
}
