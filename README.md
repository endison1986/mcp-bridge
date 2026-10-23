# mcp-bridge

Expose local MCP services to the public internet through a relay, written in Rust.

## Modes

- `relay`: runs on a server with a public IP. Accepts agent connections and serves `/mcp/{id}`.
- `agent`: runs on a private-network machine. Connects out to the relay and forwards requests to a local MCP endpoint.
- `mixed`: runs relay and agent in one process.

## Usage

```bash
export MCP_BRIDGE_TOKEN=change-me

# Public server
mcp-bridge relay --listen 0.0.0.0:8080

# Private machine
mcp-bridge agent --relay wss://relay.example.com/agent/ws --id my-agent --target http://127.0.0.1:3000/mcp

# Single process
mcp-bridge mixed --listen 0.0.0.0:8080 --id my-agent --target http://127.0.0.1:3000/mcp
```

MCP clients then use `https://relay.example.com/mcp/my-agent`.

## Protocol

- Agent connects to `GET /agent/ws?id={id}` with `Authorization: Bearer {token}`.
- Each MCP HTTP request is sent to the agent as a JSON `ForwardRequest` text frame and answered with a `ForwardResponse` frame, matched by `id`.
- Requests time out after 60s.

## Limitations

- `GET` (server-to-client SSE stream) is not supported yet; the relay returns 405.
- Bodies are passed as UTF-8 text.
