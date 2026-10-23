use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use mcp_bridge::{
    agent::{self, AgentConfig},
    relay::{self, RelayState},
};

#[derive(Parser)]
#[command(name = "mcp-bridge", version, about = "Expose local MCP services through a relay")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Run the public relay server
    Relay {
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: SocketAddr,
        /// Shared secret agents must present
        #[arg(long, env = "MCP_BRIDGE_TOKEN")]
        token: String,
    },
    /// Run an agent next to a local MCP service
    Agent {
        /// e.g. wss://relay.example.com/agent/ws
        #[arg(long)]
        relay: String,
        /// Agent id; MCP clients reach it at /mcp/{id}
        #[arg(long)]
        id: String,
        #[arg(long, env = "MCP_BRIDGE_TOKEN")]
        token: String,
        /// Local MCP endpoint, e.g. http://127.0.0.1:3000/mcp
        #[arg(long)]
        target: String,
    },
    /// Run relay and agent in a single process
    Mixed {
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: SocketAddr,
        #[arg(long, env = "MCP_BRIDGE_TOKEN")]
        token: String,
        #[arg(long)]
        id: String,
        #[arg(long)]
        target: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().mode {
        Mode::Relay { listen, token } => relay::serve(listen, RelayState::new(token)).await,
        Mode::Agent {
            relay,
            id,
            token,
            target,
        } => {
            agent::run(AgentConfig {
                relay_ws: relay,
                agent_id: id,
                token,
                target,
            })
            .await
        }
        Mode::Mixed {
            listen,
            token,
            id,
            target,
        } => {
            // The agent in the same process connects to the relay over loopback.
            let agent_cfg = AgentConfig {
                relay_ws: format!("ws://127.0.0.1:{}/agent/ws", listen.port()),
                agent_id: id,
                token: token.clone(),
                target,
            };
            let relay_task = tokio::spawn(relay::serve(listen, RelayState::new(token)));
            tokio::select! {
                r = relay_task => r??,
                r = agent::run(agent_cfg) => r?,
            }
            Ok(())
        }
    }
}
