//! JSON configuration file: the only source of settings.

use std::{collections::BTreeMap, path::Path};

use anyhow::{anyhow, bail, Context};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Relay,
    Agent,
    Mixed,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuthType {
    Bearer,
    Path,
    None,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClientAuth {
    #[serde(rename = "type")]
    pub kind: ClientAuthType,
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Security {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub client_auth: Option<ClientAuth>,
    #[serde(default)]
    pub agent_token: Option<String>,
}

impl Default for Security {
    fn default() -> Self {
        Self {
            enabled: true,
            client_auth: None,
            agent_token: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Aggregate {
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_separator")]
    pub tool_separator: String,
}

impl Default for Aggregate {
    fn default() -> Self {
        Self {
            path: default_path(),
            tool_separator: default_separator(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Command line of a stdio MCP server, e.g. ["npx", "-y", "shell-exec-mcp"].
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Endpoint of an MCP server that is already running (Streamable HTTP).
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub mode: Mode,
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub relay: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub security: Security,
    #[serde(default)]
    pub aggregate: Aggregate,
    #[serde(default)]
    pub servers: BTreeMap<String, ServerConfig>,
}

fn default_true() -> bool {
    true
}

fn default_path() -> String {
    "/mcp".to_string()
}

fn default_separator() -> String {
    "__".to_string()
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let expanded = expand_env(&raw)?;
        let config: Config = serde_json::from_str(&expanded)
            .with_context(|| format!("parsing {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Servers that are switched on. Disabled servers are never started or exposed.
    pub fn enabled_servers(&self) -> impl Iterator<Item = (&String, &ServerConfig)> + '_ {
        self.servers.iter().filter(|(_, s)| s.enabled)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if !self.aggregate.path.starts_with('/') {
            bail!("aggregate.path must start with '/'");
        }
        if !self.aggregate.tool_separator.chars().all(is_ident_char) {
            bail!("aggregate.tool_separator may only contain letters, digits, '_' and '-'");
        }

        let security = &self.security;
        if security.enabled {
            check_secret("security.agent_token", security.agent_token.as_deref())?;
            if self.mode != Mode::Agent {
                let auth = security.client_auth.as_ref().ok_or_else(|| {
                    anyhow!("security.client_auth is required when security.enabled is true (type: bearer | path | none)")
                })?;
                if auth.kind != ClientAuthType::None {
                    check_secret("security.client_auth.token", auth.token.as_deref())?;
                }
            }
        }

        match self.mode {
            Mode::Relay => {
                require(self.listen.as_deref(), "listen")?;
                if self.enabled_servers().next().is_some() {
                    bail!("relay mode does not run local servers; remove them or use mixed/agent mode");
                }
            }
            Mode::Mixed => {
                require(self.listen.as_deref(), "listen")?;
            }
            Mode::Agent => {
                require(self.relay.as_deref(), "relay")?;
                let id = require(self.agent_id.as_deref(), "agent_id")?;
                if !is_name(id) {
                    bail!("agent_id may only contain letters, digits and '-'");
                }
            }
        }

        for (name, server) in &self.servers {
            if !is_name(name) {
                bail!("server name '{name}' may only contain letters, digits and '-'");
            }
            if !server.enabled {
                continue;
            }
            match (&server.command, &server.url) {
                (Some(cmd), None) if !cmd.is_empty() => {}
                (None, Some(url)) if url.starts_with("http://") || url.starts_with("https://") => {}
                _ => bail!("server '{name}': set exactly one of 'command' (non-empty array) or 'url' (http/https)"),
            }
        }
        Ok(())
    }
}

/// Replaces ${NAME} with the value of environment variable NAME. Missing variables are an error.
pub fn expand_env(input: &str) -> anyhow::Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .context("unterminated ${...} in configuration")?;
        let name = &after[..end];
        let value = std::env::var(name)
            .with_context(|| format!("environment variable '{name}' referenced in config is not set"))?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn check_secret(field: &str, value: Option<&str>) -> anyhow::Result<()> {
    let v = value.ok_or_else(|| anyhow!("{field} is required"))?;
    if v.len() < 32 {
        bail!("{field} must be at least 32 characters");
    }
    Ok(())
}

fn require<'a>(value: Option<&'a str>, field: &str) -> anyhow::Result<&'a str> {
    match value {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(anyhow!("'{field}' is required for this mode")),
    }
}

fn is_name(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}
