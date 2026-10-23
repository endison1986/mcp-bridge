//! Messages exchanged over the agent <-> relay WebSocket.
//! `body` is a JSON-RPC message encoded as text.

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardRequest {
    pub id: u64,
    pub body: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardResponse {
    pub id: u64,
    pub body: String,
}
