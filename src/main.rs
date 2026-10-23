use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use mcp_bridge::{
    agent::{self, AgentConfig},
    config::{Config, Mode},
    gateway::Gateway,
    relay::{self, RelayState},
};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "mcp-bridge", version, about = "Aggregate local MCP servers and expose them through a relay")]
struct Cli {
    /// Path to the JSON configuration file
    #[arg(short, long, default_value = "mcp-bridge.json")]
    config: PathBuf,
    /// Validate the configuration and exit
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(false)
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;

    if cli.check {
        println!(
            "{}: OK (mode={:?}, enabled servers={})",
            cli.config.display(),
            config.mode,
            config.enabled_servers().count()
        );
        return Ok(());
    }
    if !config.security.enabled {
        tracing::warn!("security.enabled is false: client and agent connections are NOT authenticated");
    }

    match config.mode {
        Mode::Relay => {
            let addr = listen_addr(&config)?;
            relay::serve(addr, RelayState::new(&config, None)).await
        }
        Mode::Mixed => {
            let addr = listen_addr(&config)?;
            let gateway = Arc::new(start_gateway(&config).await?);
            relay::serve(addr, RelayState::new(&config, Some(gateway))).await
        }
        Mode::Agent => {
            let relay_ws = config.relay.clone().context("'relay' is required in agent mode")?;
            let agent_id = config.agent_id.clone().context("'agent_id' is required in agent mode")?;
            let gateway = Arc::new(start_gateway(&config).await?);
            agent::run(AgentConfig {
                relay_ws,
                agent_id,
                token: agent_token(&config),
                gateway,
            })
            .await
        }
    }
}

fn listen_addr(config: &Config) -> anyhow::Result<SocketAddr> {
    config
        .listen
        .as_deref()
        .context("'listen' is required in relay and mixed mode")?
        .parse::<SocketAddr>()
        .context("invalid 'listen' address")
}

fn agent_token(config: &Config) -> Option<String> {
    if config.security.enabled {
        config.security.agent_token.clone()
    } else {
        None
    }
}

async fn start_gateway(config: &Config) -> anyhow::Result<Gateway> {
    let gateway = Gateway::new(config);
    tracing::info!(servers = gateway.server_count(), "starting local MCP servers");
    gateway.preload().await?;
    Ok(gateway)
}
