//! Messages exchanged over the agent <-> relay WebSocket.

use serde::{Deserialize, Serialize};

/// Request sent from the relay to an agent (one HTTP request from an MCP client).
#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardRequest {
    pub id: u64,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Response sent from an agent back to the relay.
#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardResponse {
    pub id: u64,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Request headers the relay passes through to the agent.
pub const FORWARDED_HEADERS: &[&str] = &[
    "content-type",
    "accept",
    "mcp-session-id",
    "mcp-protocol-version",
    "authorization",
];

/// Response headers the agent passes back to the MCP client.
pub const RESPONSE_HEADERS: &[&str] = &["content-type", "mcp-session-id", "mcp-protocol-version"];
