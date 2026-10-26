# mcp-bridge

Aggregate local MCP servers and expose them to AI clients through one endpoint, optionally via a public relay.

- One JSON config file (`--config`) holds everything. No other CLI options.
- Only servers listed in the config are started and reachable. Disabled servers are not started.
- Servers can be stdio commands (`command`, started by mcp-bridge) or existing HTTP endpoints (`url`).
- AI clients configure a single URL, e.g. `https://relay.example.com/mcp`, and see the tools of every enabled server.

## Modes

| mode | runs on | what it does |
|---|---|---|
| `relay` | public server | accepts agents over `/agent/ws`, serves the aggregate endpoint for all connected agents |
| `agent` | private machine | runs the configured servers, connects out to the relay |
| `mixed` | public server | runs the configured servers and the relay in one process |

## Usage

```bash
cargo build --release
export MCP_BRIDGE_CLIENT_TOKEN='at-least-32-random-characters-xxxxxxxx'
export MCP_BRIDGE_AGENT_TOKEN='at-least-32-random-characters-yyyyyyyy'

mcp-bridge --config mcp-bridge.json --check   # validate only
mcp-bridge --config mcp-bridge.json           # run
```

See `mcp-bridge.example.json` for a complete example. Design notes: `docs/配置与聚合设计.md`.

## Config reference

Top level:

| field | required | description |
|---|---|---|
| `mode` | yes | `relay`, `agent` or `mixed` |
| `listen` | relay, mixed | HTTP listen address, e.g. `0.0.0.0:8080` |
| `relay` | agent | relay WebSocket URL, e.g. `wss://relay.example.com/agent/ws` |
| `agent_id` | agent | agent name; becomes the tool prefix on the relay |
| `security` | no | see below |
| `aggregate` | no | `path` (default `/mcp`), `tool_separator` (default `__`, empty string disables prefixes) |
| `servers` | no | map of server name to server definition |

Server definition: exactly one of `command` (array, stdio) or `url` (http/https). Optional `env` (stdio only), `headers` (url only, e.g. `{"Authorization": "Bearer ${TERMINAL_API_KEY}"}`) and `enabled` (default `true`).

Security:

| field | description |
|---|---|
| `enabled` | default `true`. When `false`, no token is checked anywhere and a warning is logged |
| `client_auth.type` | `bearer` (header `Authorization: Bearer <token>`), `path` (`/mcp/<token>`), or `none` |
| `client_auth.token` | required for `bearer` and `path`; at least 32 characters |
| `agent_token` | required when enabled; at least 32 characters; agents must present it |

`${NAME}` anywhere in the file is replaced with the environment variable `NAME` at startup. Values are inserted as raw text, so they must not contain `"` or `\`.

## Tool naming

Tools are exposed as `{server}{separator}{tool}`, e.g. `shell__exec`. In relay mode, agent tools are prefixed with the agent id: `{agent_id}{separator}{server}{separator}{tool}`, e.g. `office__shell__exec`. A duplicate name is an error.

## Endpoints

| path | method | purpose |
|---|---|---|
| `aggregate.path` | POST | JSON-RPC: `initialize`, `ping`, `tools/list`, `tools/call`; notifications return 202 |
| `aggregate.path/<token>` | POST | same, when `client_auth.type` is `path` |
| `/agent/ws` | WebSocket | agent connection, requires `agent_token` |

Other methods return 405 on the aggregate endpoint. Unauthenticated requests return 401.

## Logging

Every `tools/call` is logged with the tool name, request id, duration and outcome:

- `tool call started` (info)
- `routing tool call to local server` (info, with the server name) or `forwarding tool call to agent` (info, with the agent id)
- `tool call succeeded` (info), `tool call returned an error result` (warn) or `tool call failed` (warn, with the error code and message)

The level is controlled with `RUST_LOG`, e.g. `RUST_LOG=info` (default) or `RUST_LOG=debug`.

## Limitations

- Streamable HTTP responses only; server-initiated SSE streams are not forwarded.
- No TLS in the process; put a reverse proxy (Caddy, Nginx) in front for HTTPS/WSS.
- Tool lists are fetched once and cached until restart. Failed servers are retried on the next request.
- Relay routing of agent tools relies on the separator; use a non-empty `tool_separator` when more than one agent is connected.
