# MCP Servers

Kasal connects agents to external tools through the Model Context Protocol (MCP). An MCP server exposes a set of tools over HTTP, and Kasal turns those tools into capabilities your agents and tasks can call. Use MCP when you want an agent to reach a capability that is not a built-in Kasal tool: querying Databricks data, running Unity Catalog functions, searching a vector index, or talking to an external service that speaks MCP.

Kasal supports two transports, set per server with `server_type`:

- `sse`: Server-Sent Events
- `streamable`: Streamable HTTP

Authentication is set per server with `auth_type`:

- `api_key`: a bearer token you supply (stored encrypted)
- `databricks_spn`: Databricks service-principal credentials, used for managed Databricks servers

## Databricks managed servers

Teamspace admins can browse and register Databricks-hosted MCP servers from the chat picker. The catalog is served by `GET /mcp/databricks/available` and is admin-only. Each managed server is registered as a Kasal MCP server with `auth_type=databricks_spn` and `server_type=streamable`.

The catalog groups servers into two families:

- `external`: MCP servers registered inside Databricks as Unity Catalog HTTP connections flagged as MCP connections (the workspace UI lists these under AI Gateway, MCPs). Kasal proxies them at `/api/2.0/mcp/external/{connection_name}`. These are listed using the caller's own credentials, so a user sees only the connections their Unity Catalog permissions allow.
- `managed`: the workspace's managed MCP server types.

The managed types are:

- Databricks SQL: execute SQL against the workspace. Registered at `/api/2.0/mcp/sql`.
- Unity Catalog Functions: run the functions in a catalog and schema. The picker offers the catalog and schema from the teamspace's Databricks configuration when configured (`/api/2.0/mcp/functions/{catalog}/{schema}`), plus the built-in `system.ai` functions such as `python_exec` (`/api/2.0/mcp/functions/system/ai`).
- Genie: pick a Genie space. This type is expandable, so it is not enumerated up front (a workspace can have thousands of spaces). Drilling in calls `GET /mcp/databricks/genie-spaces`, which is searchable and paginated; each space registers at `/api/2.0/mcp/genie/{space_id}`.
- Vector Search (AI Search): pick a vector search index. This type is also expandable. Drilling in calls `GET /mcp/databricks/ai-search-indexes`, which lists the workspace's indexes; each registers at `/api/2.0/mcp/ai-search/{catalog}/{schema}/{index}`.

## External and custom servers

Any HTTP(S) MCP server can be registered directly, including third-party or self-hosted ones. Create a server with `POST /mcp/servers`:

```json
{
  "name": "my-server",
  "server_url": "https://example.com/mcp",
  "server_type": "streamable",
  "auth_type": "api_key",
  "api_key": "your-token",
  "enabled": true,
  "timeout_seconds": 30,
  "max_retries": 3,
  "rate_limit": 60
}
```

Notes on registration:

- Only admins can create, update, delete, toggle, or test MCP servers.
- The `server_url` must be a valid `http` or `https` URL with a host. Other schemes are rejected.
- The `api_key` is encrypted before storage and is not returned in list responses.
- When a teamspace (group) context is present, a server created via `POST /mcp/servers` is scoped to that teamspace. Without a teamspace context it is created as a base (global) server.
- Test a connection before saving with `POST /mcp/test-connection`. Kasal opens an MCP client session and reports how many tools the server exposes.

## Global vs per-teamspace

MCP follows a two-tier model, the same pattern Kasal uses for Models: a global (system-admin) registry plus per-teamspace overrides. The key distinction is between two boolean columns on `mcp_servers`:

- `enabled`: controls availability. For a base server (where `group_id` is `NULL`), `enabled` means "available to all teamspaces". For a teamspace-scoped row, `enabled` is that teamspace's own on/off state.
- `global_enabled`: a separate flag meaning "auto-attach this server to every crew" as a baseline, independent of availability.

Do not confuse the two: a server being available to teamspaces is governed by `enabled`, not by `global_enabled`.

How the tiers resolve:

- Base servers (`group_id IS NULL`) form the system-admin catalog. A base server is visible to a teamspace only when its `enabled` flag is true. System admins manage this catalog via the `servers/base` and `servers/global` endpoints and `PATCH /mcp/servers/{id}/global-availability`.
- A teamspace-scoped row (a row with a `group_id`) shadows a base server of the same name for that teamspace only. Other teamspaces keep seeing the base. This is how a teamspace admin can enable or disable a globally-available server for their own teamspace without affecting anyone else (`PATCH /mcp/servers/{id}/workspace-enabled` and `POST /mcp/servers/{id}/enable-for-workspace`). Disabling a base server for one teamspace creates a teamspace-scoped override with `enabled=false`; the base row is never mutated.
- The global tier gates the teamspace tier. A teamspace override is only effective while its base server is `enabled`. When a system admin disables a base server, that server becomes unavailable to **every** teamspace — hidden from the list and not resolvable at execution — even where a teamspace had enabled its own override. Teamspace-only servers (no base row) are unaffected.
- Deleting a base server cascades: it hard-deletes every per-teamspace override row of the same name, so a globally-removed server disappears from all teamspaces with no orphaned rows. Deleting a teamspace-scoped row removes only that row.

Who manages each tier:

- System admins: the global/base catalog and each base server's availability (`enabled`).
- Teamspace admins: teamspace-scoped overrides, plus creating servers scoped to their teamspace.

What different users see from `GET /mcp/servers`:

- Teamspace admins see the full effective list, including disabled servers, so they can manage state in Configuration → MCP.
- Everyone else sees only the servers an admin has enabled (the curated allow-list), so regular users cannot pick servers the teamspace has not sanctioned.
- Globally-disabled servers never appear for anyone (admin or not): the global disable cascades before the per-user filter.

The chat "+" MCP picker is stricter than that management list: it shows only **enabled** servers — disabled ones are omitted entirely (not greyed out), since only an enabled server can be attached to a run. Disabled servers are managed in Configuration → MCP, not the picker.

Servers are deduplicated by name, preferring a teamspace-specific row over the base row.

A separate `mcp_settings` record holds a master `global_enabled` switch for all MCP functionality and an `individual_enabled` flag that allows agent and task level MCP selection. Read it with `GET /mcp/settings`; admins update it with `PUT /mcp/settings`.

## Genie integration

When you pick a Genie managed server, you do not need a separate space-id step. Kasal copies the Genie space id straight out of the server URL.

The crew generator commonly assigns both the custom `GenieTool` and a Genie MCP server to the same task or agent. A managed Genie MCP server is registered at `.../api/2.0/mcp/genie/{space_id}`, so the space id is embedded in its URL, while the `GenieTool` starts with no space id. During task building, `apply_genie_mcp_space_id` (in `src/engines/crewai/kernel/genie_formatting.py`) scans both the task tools and the agent tools, reads the space id from the Genie MCP tool's adapter URL, and writes it into any `GenieTool` instance that lacks one. Without this, the `GenieTool` would fail with "Genie space ID is not configured". This bridge is shared by both the crew path and the flow path, so selecting the Genie MCP server in the picker is enough.
