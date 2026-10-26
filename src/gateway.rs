//! Local gateway: owns the enabled servers from the config and presents them as one tool list.
//! Tool names are `{server}{separator}{tool}`.

use anyhow::anyhow;
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::{
    backend::Backend,
    config::Config,
    jsonrpc::{RpcError, ToolProvider},
};

struct Server {
    name: String,
    backend: Backend,
    /// Tool list fetched from the server; `None` until a successful fetch.
    cache: Mutex<Option<Vec<Value>>>,
}

struct Exposed {
    name: String,
    server: usize,
    original: String,
    tool: Value,
}

impl Server {
    async fn tools(&self) -> Result<Vec<Value>, RpcError> {
        let mut cache = self.cache.lock().await;
        if let Some(tools) = cache.as_ref() {
            return Ok(tools.clone());
        }
        let result = self.backend.request("tools/list", json!({})).await?;
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        *cache = Some(tools.clone());
        Ok(tools)
    }
}

pub struct Gateway {
    servers: Vec<Server>,
    separator: String,
}

impl Gateway {
    pub fn new(config: &Config) -> Self {
        let servers = config
            .enabled_servers()
            .map(|(name, cfg)| Server {
                name: name.clone(),
                backend: Backend::from_config(name, cfg),
                cache: Mutex::new(None),
            })
            .collect();
        Self {
            servers,
            separator: config.aggregate.tool_separator.clone(),
        }
    }

    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    /// Starts every enabled server and caches its tools. Servers that fail are retried on demand.
    /// Fails if two tools end up with the same exposed name.
    pub async fn preload(&self) -> anyhow::Result<()> {
        for s in &self.servers {
            if let Err(e) = s.tools().await {
                tracing::warn!(server = %s.name, error = %e.message, "server not ready; will retry on demand");
            }
        }
        self.exposed().await.map(|_| ()).map_err(|e| anyhow!(e.message))
    }

    /// Whether this gateway exposes a tool with the given (prefixed) name.
    pub async fn owns(&self, name: &str) -> Result<bool, RpcError> {
        Ok(self.exposed().await?.iter().any(|e| e.name == name))
    }

    async fn exposed(&self) -> Result<Vec<Exposed>, RpcError> {
        let mut out: Vec<Exposed> = Vec::new();
        for (idx, server) in self.servers.iter().enumerate() {
            let tools = match server.tools().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(server = %server.name, error = %e.message, "failed to list tools");
                    continue;
                }
            };
            for mut tool in tools {
                let original = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if original.is_empty() {
                    continue;
                }
                let name = format!("{}{}{}", server.name, self.separator, original);
                if out.iter().any(|e| e.name == name) {
                    return Err(RpcError::internal(format!("duplicate tool name '{name}'")));
                }
                if let Some(obj) = tool.as_object_mut() {
                    obj.insert("name".to_string(), Value::String(name.clone()));
                }
                out.push(Exposed {
                    name,
                    server: idx,
                    original,
                    tool,
                });
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl ToolProvider for Gateway {
    async fn list_tools(&self) -> Result<Vec<Value>, RpcError> {
        Ok(self.exposed().await?.into_iter().map(|e| e.tool).collect())
    }

    async fn call_tool(&self, mut params: Value) -> Result<Value, RpcError> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let hit = self
            .exposed()
            .await?
            .into_iter()
            .find(|e| e.name == name)
            .ok_or_else(|| RpcError::new(-32602, format!("unknown tool: {name}")))?;
        let server = hit.server;
        tracing::info!(server = %self.servers[server].name, tool = %hit.original, "routing tool call to local server");
        if let Some(obj) = params.as_object_mut() {
            obj.insert("name".to_string(), Value::String(hit.original));
        }
        self.servers[server].backend.request("tools/call", params).await
    }
}
