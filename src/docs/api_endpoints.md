# Kasal API endpoints reference

The routes this platform actually serves, grouped by domain.

**This document is a map, not a contract.** The app exposes **444 routes across
53 domains** — more than a hand-written page can carry without going stale, which
is exactly what happened to the previous version of this file (24 of its 63
documented endpoints did not exist, including a `/crews/{id}/kickoff` pair that
was never how a crew ran). The authoritative, always-current reference is the
OpenAPI schema the app generates from its own routers:

```text
GET /docs          # Swagger UI
GET /openapi.json  # the machine-readable schema
```

When those two disagree with this page, the schema is right. See
[Keeping this page honest](#keeping-this-page-honest) for the check.

---

## Base URL

```text
https://<your-app>.databricksapps.com/api/v1     # Databricks Apps
http://localhost:8000/api/v1                     # local development
```

Every path below is relative to that prefix.

---

## Table of contents

- [Authentication](#authentication)
- [Crews](#crews)
- [Agents](#agents)
- [Tasks](#tasks)
- [Flows](#flows)
- [Executions](#executions)
- [Execution history](#execution-history)
- [Execution traces](#execution-traces)
- [Tools](#tools)
- [Models](#models)
- [API keys](#api-keys)
- [Schedules](#schedules)
- [Engine configuration](#engine-configuration)
- [Other domains](#other-domains)
- [Conventions](#conventions)
- [Rate limiting](#rate-limiting)
- [Keeping this page honest](#keeping-this-page-honest)

---

## Authentication

**There is no login endpoint.** Kasal does not issue its own credentials — no
`/auth/login`, no refresh tokens, no session to establish. Identity arrives on
each request as headers, set by whatever fronts the app.

Under **Databricks Apps**, the platform sets them for you:

| Header | Carries |
| --- | --- |
| `X-Forwarded-Email` | The caller's identity; the group (teamspace) is derived from it |
| `X-Forwarded-Access-Token` | The user's Databricks OAuth token, used for on-behalf-of calls |

Behind an **OAuth2 proxy**, the equivalents are read too: `X-Auth-Request-Email`,
`X-Auth-Request-User`, `X-Auth-Request-Access-Token`.

Two optional headers override the derived context:

| Header | Effect |
| --- | --- |
| `group_id` | Act in a specific teamspace rather than the default one for the email |
| `X-Group-Domain` | Select the group by email domain |

A plain `Authorization: Bearer <token>` is also accepted as a source of the
access token, but it does **not** by itself establish identity — the email
header is what determines the group, and group context is what every
tenant-scoped endpoint filters on.

See `src/backend/src/core/dependencies.py` (`get_group_context`) and
`src/backend/src/utils/user_context.py`.

---

## Crews

A crew is a saved workflow definition. **Running one is not a crew endpoint** —
see [Executions](#executions).

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/crews` | List crews in the teamspace |
| `POST` | `/crews` | Create a crew |
| `GET` | `/crews/{crew_id}` | Get a crew |
| `PUT` | `/crews/{crew_id}` | Update a crew |
| `DELETE` | `/crews/{crew_id}` | Delete a crew |
| `DELETE` | `/crews` | Delete every crew in the teamspace |
| `POST` | `/crews/debug` | Echo a crew payload back with validation detail |
| `GET` | `/crews/{crew_id}/feedback` | Read feedback recorded against a crew |
| `POST` | `/crews/{crew_id}/feedback` | Record feedback |
| `GET` | `/crews/feedback-summary` | Aggregated feedback across crews |
| `GET` | `/crews/{crew_id}/publish` | Where this crew is published |
| `POST` | `/crews/{crew_id}/publish` | Publish (e.g. to chat) |
| `PATCH` | `/crews/{crew_id}/publish` | Change a publication |
| `DELETE` | `/crews/{crew_id}/publish` | Unpublish |

Export lives on its own router: see [Other domains](#other-domains).

---

## Agents

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/agents` | List agents |
| `POST` | `/agents` | Create an agent |
| `GET` | `/agents/{agent_id}` | Get an agent |
| `PUT` | `/agents/{agent_id}` | Update selected fields |
| `PUT` | `/agents/{agent_id}/full` | Replace the whole agent |
| `DELETE` | `/agents/{agent_id}` | Delete an agent |
| `DELETE` | `/agents` | Delete every agent in the teamspace |

**Configuration fields:** `name`, `role`, `goal`, `backstory`, `tools`
(tool ids), `tool_configs`, `llm`, plus execution limits (`max_iter`,
`max_rpm`, `max_execution_time`, `max_retry_limit`) and `memory`.

---

## Tasks

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/tasks` | List tasks |
| `POST` | `/tasks` | Create a task |
| `GET` | `/tasks/{task_id}` | Get a task |
| `PUT` | `/tasks/{task_id}` | Update selected fields |
| `PUT` | `/tasks/{task_id}/full` | Replace the whole task |
| `DELETE` | `/tasks/{task_id}` | Delete a task |
| `DELETE` | `/tasks` | Delete every task in the teamspace |

**Configuration fields:** `name`, `description`, `expected_output`, `agent_id`,
`context` (ids of tasks whose output feeds this one), `tools`, `tool_configs`,
`output_pydantic` / `output_json` for structured output, `guardrail`,
`human_input`, `async_execution`.

---

## Flows

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/flows` | List flows |
| `POST` | `/flows` | Create a flow |
| `GET` | `/flows/{flow_id}` | Get a flow |
| `PUT` | `/flows/{flow_id}` | Update a flow |
| `DELETE` | `/flows/{flow_id}` | Delete a flow |
| `DELETE` | `/flows` | Delete every flow in the teamspace |
| `POST` | `/flows/debug` | Validate a flow payload |
| `GET` | `/flows/{flow_id}/checkpoints` | Checkpoints recorded for this flow |
| `DELETE` | `/flows/{flow_id}/checkpoints/{execution_id}` | Drop one run's checkpoints |
| `GET`/`POST`/`PATCH`/`DELETE` | `/flows/{flow_id}/publish` | Publication, as for crews |

Flow *runs* go through `/executions` like everything else.

---

## Executions

**This is how work is started.** A crew or a flow is executed by POSTing its
configuration — there is no per-crew run endpoint.

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/executions` | Start a run (crew or flow) and return its `execution_id` |
| `GET` | `/executions` | List runs (`limit` 1–100, default 50; `offset`) |
| `GET` | `/executions/{execution_id}` | Full record including the result |
| `GET` | `/executions/{execution_id}/status` | Status only — the polling endpoint |
| `POST` | `/executions/{execution_id}/stop` | Ask a run to stop |
| `POST` | `/executions/{execution_id}/force-stop` | Kill it |
| `POST` | `/executions/{execution_id}/resume` | Resume from a checkpoint |
| `GET` | `/executions/{execution_id}/checkpoints` | Checkpoints for a run |
| `GET` | `/executions/{execution_id}/checkpoints/{unit_key}` | One checkpointed unit |
| `DELETE` | `/executions/{execution_id}/checkpoints` | Drop them |
| `POST` | `/executions/generate-name` | Suggest a run name from the payload |
| `GET` | `/executions/health` | Liveness of the execution subsystem |

**Status values:** `PENDING`, `PREPARING`, `RUNNING`, `COMPLETED`, `FAILED`,
`CANCELLED`, `STOPPED`.

There is **no `/executions/{id}/logs`**. Per-step detail is in the traces
(below); process logs are streamed over SSE.

### Starting a crew run

```bash
curl -X POST https://<your-app>.databricksapps.com/api/v1/executions \
  -H "Content-Type: application/json" \
  -H "X-Forwarded-Email: you@example.com" \
  -d '{
        "agents_yaml": {
          "researcher": {
            "role": "Research Analyst",
            "goal": "Find and summarise the relevant material",
            "backstory": "An analyst who checks sources before answering.",
            "tools": []
          }
        },
        "tasks_yaml": {
          "research": {
            "description": "Research the Swiss fintech market.",
            "expected_output": "A short briefing with sources.",
            "agent": "researcher"
          }
        },
        "inputs": {},
        "model": "databricks-llama-4-maverick",
        "execution_type": "crew"
      }'
```

The response carries `execution_id`; poll
`/executions/{execution_id}/status` until it reaches a terminal status, then
read `/executions/{execution_id}` for the result.

To run a **saved** crew, send its stored `agents_yaml` / `tasks_yaml` (read them
from `GET /crews/{crew_id}`) — or use the UI, which does exactly this.

To run a **flow**, POST the same endpoint with `execution_type: "flow"` and
either `flow_id` or an inline `nodes` / `edges` definition.

---

## Execution history

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/executions/history` | Paged run history for the teamspace |
| `GET` | `/executions/history/{execution_id}` | One historical run |
| `DELETE` | `/executions/history/{execution_id}` | Delete one |
| `DELETE` | `/executions/history` | Delete all history for the teamspace |
| `GET` | `/executions/history/all-groups` | Across teamspaces (admin) |
| `GET` | `/executions/{execution_id}/outputs` | Task outputs recorded for a run |
| `PATCH` | `/executions/{job_id}/result` | Amend a stored result |
| `DELETE` | `/executions/{job_id}` | Delete a run record |

---

## Execution traces

Per-agent, per-task and per-tool events for a run — what the timeline renders.

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/traces/` | List traces |
| `POST` | `/traces/` | Record a trace |
| `DELETE` | `/traces/` | Delete traces |
| `GET` | `/traces/{trace_id}` | One trace row |
| `DELETE` | `/traces/{trace_id}` | Delete one |
| `GET` | `/traces/job/{job_id}` | Every trace for a run (`limit`, `offset`) |
| `DELETE` | `/traces/job/{job_id}` | Delete them |
| `GET` | `/traces/job/{job_id}/task-states` | Per-task status map |
| `GET` | `/traces/job/{job_id}/crew-node-states` | Per-crew-node status map |
| `GET` | `/traces/execution/{run_id}` | By numeric run id |
| `DELETE` | `/traces/execution/{run_id}` | Delete by numeric run id |

Note the trailing slash on the collection routes.

---

## Tools

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/tools` | List tools |
| `GET` | `/tools/enabled` | Only the enabled ones |
| `GET` | `/tools/global` | Globally available tools |
| `POST` | `/tools/` | Create a tool |
| `GET` | `/tools/{tool_id}` | Get a tool |
| `PUT` | `/tools/{tool_id}` | Update a tool |
| `DELETE` | `/tools/{tool_id}` | Delete a tool |
| `PATCH` | `/tools/{tool_id}/toggle-enabled` | Enable / disable — **not** `/enable` or `/disable` |
| `PATCH` | `/tools/{tool_id}/global-availability` | Make available to every teamspace |
| `GET` | `/tools/configurations/all` | Every tool configuration |
| `GET` | `/tools/configurations/{tool_name}` | One tool's configuration |
| `PUT` | `/tools/configurations/{tool_name}` | Update it |

---

## Models

Keyed by **model key** (e.g. `databricks-llama-4-maverick`), not a numeric id.

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/models` | List model configurations |
| `GET` | `/models/enabled` | Only the enabled ones |
| `GET` | `/models/global` | Global model configurations |
| `POST` | `/models` | Create one |
| `GET` | `/models/{model_key}` | Get one |
| `PUT` | `/models/{model_key}` | Update one |
| `DELETE` | `/models/{model_key}` | Delete one |
| `PATCH` | `/models/{model_key}/toggle` | Enable / disable |
| `PATCH` | `/models/global/{model_key}/toggle` | Toggle globally |
| `POST` | `/models/enable-all` | Enable every model |
| `POST` | `/models/disable-all` | Disable every model |

There is no `/models/test`. Provider reachability is checked through
`/databricks/...` and the connection endpoints.

---

## API keys

Keyed by **name**, not an id — the name *is* the identifier.

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/api-keys` | List keys (values never returned) |
| `POST` | `/api-keys` | Create a key |
| `PUT` | `/api-keys/{api_key_name}` | Update a key's value |
| `DELETE` | `/api-keys/{api_key_name}` | Delete a key |

Values are encrypted at rest, never returned in plain text, and scoped by group.
Commonly set: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `SERPER_API_KEY`,
`PERPLEXITY_API_KEY`, `DATABRICKS_TOKEN`, `POWERBI_CLIENT_SECRET`.

---

## Schedules

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/schedules` | List schedules |
| `POST` | `/schedules` | Create a schedule |
| `POST` | `/schedules/from-execution` | Turn a past run into a schedule |
| `GET` | `/schedules/{schedule_id}` | Get one |
| `PUT` | `/schedules/{schedule_id}` | Update one |
| `DELETE` | `/schedules/{schedule_id}` | Delete one |
| `POST` | `/schedules/{schedule_id}/toggle` | Activate / pause |
| `GET`/`POST` | `/schedules/jobs` | Databricks job bindings |
| `PUT` | `/schedules/jobs/{job_id}` | Update a job binding |

A scheduled run names no harness, so it takes the configured default — see
below.

---

## Engine configuration

Platform settings, including which agent runtime runs new jobs.

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/engine-config/harness` | The configured harness and what is available |
| `PUT` | `/engine-config/harness` | Change it — applies to runs started afterwards |

A run's harness is decided once, at creation, and recorded on its row; changing
this setting never re-points a run already under way.

The same router also carries the rest of `engine-config` (16 routes) —
`flow_enabled`, OpenTelemetry switches and similar.

---

## Other domains

The app serves considerably more than the above. These are real, and documented
in the OpenAPI schema rather than repeated here:

| Domain | Routes | What it covers |
| --- | --- | --- |
| `memory-backend` | 22 | Memory backends, Databricks Vector Search, indices |
| `database-management` | 21 | Connections, migrations, maintenance |
| `mcp` | 20 | MCP servers, tools, connection testing |
| `converters` | 18 | Document and format conversion |
| `chat-history` | 16 | Chat sessions and their messages |
| `prompt optimization` | 15 | GEPA prompt optimisation |
| `databricks-secrets` | 14 | Secret scopes and values |
| `powerbi` | 14 | Semantic models, business mappings, field synonyms, query |
| `groups` | 12 | Teamspaces and membership |
| `Human in the Loop` | 10 | Approval gates for tool calls |
| `skills` | 10 | Skill definitions attached to agents |
| `templates` | 9 | Prompt templates |
| `mlflow` | 9 | Experiments, traces, evaluation |
| `databricks` | 9 | Workspace configuration and auth |
| `a2a` / `a2a-agents` | 17 | Agent-to-agent protocol |
| `crews-export` | 8 | Export a crew as a standalone Databricks App |
| `users` | 7 | Users and permissions |
| `Server-Sent Events` | 6 | Live run streaming |
| `schemas` | 6 | Structured-output schema definitions |
| `genie`, `agentbricks`, `databricks_knowledge` | 15 | Databricks-native tooling |
| `health` | 3 | `/health`, `/health/db`, `/health/cache` |

There is no `/health/services` and no `/version`.

---

## Conventions

**Responses are the resource itself.** Endpoints return their Pydantic model
directly — there is no `{"status": ..., "data": ...}` envelope. A list endpoint
returns a JSON array (or an object with the collection plus paging fields, e.g.
`/traces/job/{job_id}` returns `{"traces": [...]}`).

**Errors** are FastAPI's shape:

```json
{ "detail": "Human-readable message" }
```

with the meaning in the HTTP status: `400` invalid input, `403` outside your
group, `404` unknown id, `409` conflict, `422` schema validation, `500` server
error.

**Paging** is `limit` + `offset`, not `page`. Where a cap exists it is stated on
the route — `/executions` allows `limit` 1–100 (default 50). Endpoints without
those parameters return everything for the teamspace.

**Tenancy** is implicit. Every tenant-scoped endpoint filters by the group
derived from your identity headers; you never pass a tenant id in the path.

---

## Rate limiting

Per identity (group, falling back to client IP), applied only to the `/api/`
surface. SSE streams and health checks are exempt so a long-lived stream cannot
exhaust a window.

| Setting | Default |
| --- | --- |
| `RATE_LIMIT_DEFAULT` | `600/minute` |
| `RATE_LIMIT_STORAGE_URI` | in-memory |

Exceeding it returns `429`. If the `limits` package is not installed the
middleware is a no-op — there is no limiting at all, rather than a stricter
fallback.

See `src/backend/src/core/rate_limit.py`.

---

## Keeping this page honest

The previous version of this file drifted to 38% fiction because nothing ever
compared it with the app. To check it:

```bash
cd src/backend
uv run python - <<'PY'
from src.api import api_router
from src.config.settings import settings
for r in sorted(api_router.routes, key=lambda r: getattr(r, "path", "")):
    for m in sorted(getattr(r, "methods", set()) - {"HEAD", "OPTIONS"}):
        print(f"{m:7} {settings.API_V1_STR}{r.path}")
PY
```

Anything in this document that is not in that output does not exist. When they
disagree, fix the document — and prefer sending people to `/docs`, which cannot
drift.
