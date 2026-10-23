//! Minimal JSON-RPC 2.0 handling shared by the aggregate endpoint and agents.

use async_trait::async_trait;
use serde_json::{json, Value};

pub const PROTOCOL_VERSION: &str = "2025-06-18";

#[derive(Debug, Clone)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn internal(message: impl std::fmt::Display) -> Self {
        Self::new(-32603, message.to_string())
    }
}

/// Something that can list and call tools: a local gateway, or the relay's aggregate view.
#[async_trait]
pub trait ToolProvider: Send + Sync {
    async fn list_tools(&self) -> Result<Vec<Value>, RpcError>;
    /// `params` is the original `tools/call` params, including `name` and `arguments`.
    async fn call_tool(&self, params: Value) -> Result<Value, RpcError>;
}

/// Handles one JSON-RPC message. Returns `None` for notifications, which get no response.
pub async fn dispatch<P: ToolProvider + ?Sized>(provider: &P, body: &str) -> Option<String> {
    let req: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            return Some(error_body(
                Value::Null,
                RpcError::new(-32700, format!("parse error: {e}")),
            ))
        }
    };
    let id = req.get("id").cloned()?;
    let method = req
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let params = req.get("params").cloned().unwrap_or_else(|| json!({}));

    let result = match method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "mcp-bridge", "version": env!("CARGO_PKG_VERSION") }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => provider
            .list_tools()
            .await
            .map(|tools| json!({ "tools": tools })),
        "tools/call" => provider.call_tool(params).await,
        other => Err(RpcError::new(-32601, format!("method not found: {other}"))),
    };

    Some(match result {
        Ok(r) => json!({ "jsonrpc": "2.0", "id": id, "result": r }).to_string(),
        Err(e) => error_body(id, e),
    })
}

fn error_body(id: Value, e: RpcError) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": e.code, "message": e.message }
    })
    .to_string()
}

/// Extracts `result` from a JSON-RPC response, or converts its `error` into `RpcError`.
pub fn into_result(msg: &Value) -> Result<Value, RpcError> {
    if let Some(err) = msg.get("error") {
        let code = err.get("code").and_then(Value::as_i64).unwrap_or(-32603);
        let message = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error")
            .to_string();
        return Err(RpcError::new(code, message));
    }
    Ok(msg.get("result").cloned().unwrap_or(Value::Null))
}
