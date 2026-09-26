# Security

Kasal runs AI agent workflows against your enterprise data, so security spans identity, teamspace isolation, agent guardrails, and the dependency supply chain. This page is a concise overview; each section links to the detailed guide for depth.

- [Identity and request authentication](#identity-and-request-authentication)
- [Authorization boundaries](#authorization-boundaries)
- [Identity and on-behalf-of (OBO)](#identity-and-on-behalf-of-obo)
- [Teamspace isolation](#teamspace-isolation)
- [Secrets and encryption at rest](#secrets-and-encryption-at-rest)
- [Agent guardrails](#agent-guardrails)
- [Data exfiltration controls](#data-exfiltration-controls)
- [Supply chain security](#supply-chain-security)

## Identity and request authentication

Kasal never authenticates a user itself. A reverse proxy does, and forwards the identity as request headers. Which headers Kasal trusts depends on where it runs; the rules live in one place, `src/backend/src/utils/request_identity.py`, and every surface that turns a request into a user (the browser API, the MCP and A2A endpoints, the request-context middleware) goes through it.

The trusted headers per deployment are:

| Deployment | Trusted identity headers | Ignored |
| --- | --- | --- |
| Databricks Apps (`DATABRICKS_APP_NAME` is set) | `X-Forwarded-Email`, `X-Forwarded-User`, `X-Forwarded-Access-Token` | `X-Auth-Request-*`: stripped before any handler runs |
| Anywhere else (oauth2-proxy with `--set-xauthrequest`, or a local run) | `X-Auth-Request-Email` / `-User` / `-Access-Token` first, then `X-Forwarded-*` as the fallback | None |

Inside Databricks Apps, `UntrustedIdentityHeadersMiddleware` (registered last in `src/backend/src/main.py`, so it runs outermost) deletes every `X-Auth-Request-*` header from the request, so the rate limiter, the dependencies and any future router never see a client-supplied identity. The Apps proxy never sets that header family, so inside Apps it can only come from the client. `X-Forwarded-User` is carried but never used as the email.

**Unauthenticated requests are refused.** Every protected API route depends on `get_group_context` (`src/backend/src/dependencies/providers.py`), which fails closed:

| Condition | Status |
| --- | --- |
| No trusted identity header | `401 Authentication required` |
| An identity that resolves to no workspace (for example a name without `@`) | `401` |
| The requested `group_id` is not one of the caller's workspaces, or cannot be resolved | `403` |
| The workspace resolver fails unexpectedly | `503`, retry |

Routes that must stay public (health checks) do not depend on it; `src/backend/tests/unit/architecture/test_api_routes_require_identity.py` keeps that list explicit.

**Local development identity.** Outside production, a developer can opt in to a fallback identity with `LOCAL_DEV_AUTH=true`: `LocalDevAuthMiddleware` in `main.py` then adds `X-Forwarded-Email: <LOCAL_DEV_USER_EMAIL>` (default `dev@localhost`) to any request that carries no identity header, and logs a warning at startup. `run.sh` sets `LOCAL_DEV_AUTH` for a local run. Inside Databricks Apps, or with `ENVIRONMENT=production` (or `prod`), the flag is ignored and an error is logged. Without it, a request with no identity gets `401`, so running `uvicorn` directly needs either the flag or identity headers.

**Server-sent events on loopback.** A browser `EventSource` cannot attach headers, so on a direct local run the SSE routes also accept `_sse_email` and `_sse_group_id` query parameters. They are honoured only when all of these hold: the path contains `/sse/`, the client address is loopback (`127.0.0.1`, `::1`, `localhost`), and the process is neither in Databricks Apps nor `ENVIRONMENT=production`. A header identity always wins over them.

## Authorization boundaries

Beyond the tenant filter, these routes carry their own role or ownership checks. Paths are relative to `/api/v1`.

| Route | Who may call it | Refusal | Source |
| --- | --- | --- | --- |
| `GET /databricks/warehouses`, `/databricks/catalogs`, `/databricks/schemas` with `?host=` | Workspace admins and editors; the host must normalise to the configured Databricks workspace and use `https` | `403` for another role, another host or a non-https scheme | `api/databricks_router.py`, `services/databricks/workspace/host_guard.py` |
| `POST /models`, `PUT` and `DELETE /models/{model_key}`, `POST /models/enable-all`, `POST /models/disable-all`, `PATCH /models/global/{model_key}/toggle` | System admins only: model rows are the global catalog every workspace sees | `403` | `api/models_router.py` |
| `PATCH /models/{model_key}/toggle` | Workspace admins; writes a per-workspace override, not the shared row | `403` | `api/models_router.py` |
| `/api/converters/*` (Power BI conversion history, jobs and saved configurations) | Callers in the owning group. Saved configurations are visible when they are a system template, or in the caller's group and public or their own; update and delete require ownership | `404` across groups, `403` on update or delete of someone else's configuration | `services/powerbi/conversions.py`, `repositories/conversion_repository.py` |
| `DELETE /crews/{crew_id}/deployment/{endpoint_name}` | Workspace admins, and only for an endpoint that serves that crew, in the caller's group | `403` for non-admins, `404` for an endpoint that does not serve this group's crew | `api/crews_export_router.py`, `services/deployment/endpoint_ownership.py` |
| `POST /mcp/databricks/migrate-external-urls` | Workspace admins or system admins. Re-points this workspace's MCP registrations from the legacy external-MCP proxy URL to the Unity Catalog MCP service URL; a system admin's call also migrates the global rows. `GET /mcp/databricks/available` only reports the pending count (`legacy_external_count`) and no longer rewrites anything | `403`; `503` when the Databricks workspace connection cannot be authenticated | `api/mcp_router.py` |

The Power BI converter routes are mounted at `/api/v1/api/converters/...` because the router declares its own `/api` prefix.

## Identity and on-behalf-of (OBO)

Every action runs as the signed-in user. All Databricks resource access (Unity Catalog, Vector Search, Genie) uses on-behalf-of (OBO) user authorization, so agents operate within the scope of the user's own token rather than elevated service principal credentials. An agent can only read what the user is already authorized to view, and Kasal does not grant tools permissions beyond the specific API calls they wrap. Because Kasal relies on OBO, it does not store long-lived Databricks tokens in `.env` files, and API keys are stored encrypted in the database rather than as plain environment variables.

See [security compliance](./README_SECURITY_COMPLIANCE.md) (design items D1 to D4) for details.

## Teamspace isolation

Kasal is multi-tenant and group-aware. Resources and permissions are scoped to the teamspace (group) context so that one teamspace's data, executions, and configuration do not leak into another. All LLM and embedding calls route through Databricks model serving endpoints in the Databricks workspace; the platform does not create or use fine-tuned models that could encode sensitive information.

## Secrets and encryption at rest

Sensitive values — provider API keys, MCP server credentials, Databricks personal access tokens, and encrypted tool configurations — are stored encrypted in the application database (SQLite in local development, Databricks Lakebase in production). Only ciphertext is persisted; plaintext secrets are never written to the database, to `.env` files, or to logs.

**Cipher.** Encryption uses a hybrid scheme (`EncryptionUtils`): each value is encrypted with a freshly generated symmetric (Fernet) key, and that symmetric key is then encrypted with an RSA public key. Reversing it requires the RSA private key, so a value can only be read by an instance that holds the key.

**Key storage.** The RSA key pair is generated on first use and kept on the application's filesystem at `~/.backendcrew/keys/` (private key with owner-only permissions). It is created once and reused for the life of that environment — including across ordinary redeploys — so values encrypted under it stay decryptable from one deployment to the next.

**Rotation and recovery.** The key is regenerated only when the key files are absent — on a first-ever startup, or after a full environment recreation / filesystem reset (not a routine redeploy). Any value encrypted under a previous key cannot be decrypted afterward and must be re-entered. The log line `Error decrypting value with SSH key: Decryption failed` is the signal that a stored secret predates the current key and needs re-saving.

## Agent guardrails

Kasal layers several always-on and opt-in defenses into the crew execution pipeline. The always-on layers are log-only (fail-open) by design: they emit structured `[SECURITY]` audit warnings rather than blocking legitimate execution. Blocking behavior is provided by the opt-in LLM guardrails.

- Prompt hardening and spotlighting: every agent system prompt is prepended with a security preamble that declares the instruction hierarchy and instructs the LLM to treat tool outputs and external data as untrusted. Output from untrusted-input tools is wrapped in `<<` `>>` delimiters at crew assembly.
- Heuristic prompt-injection detection: a regex scanner classifies user input by severity (HIGH, MEDIUM, LOW) before each crew runs, and tool and task outputs are scanned during execution.
- LLM guardrails (opt-in per task): a prompt-injection check classifies task output as SAFE or INJECTION, and a self-reflection check verifies the output against the task goal. A FAIL or INJECTION verdict triggers a CrewAI retry; both fail open on LLM errors.
- Lethal-trifecta detection: at crew assembly, Kasal inspects every tool against a capability manifest and warns when a crew (or a single task) can simultaneously read sensitive internal data, ingest untrusted external content, and communicate externally, which is the highest-risk configuration for indirect prompt-injection exfiltration. A related mixed-task check recommends splitting a task that combines untrusted input with sensitive or destructive tools.
- Excessive agency detection: tools that can trigger irreversible actions are flagged, with a recommendation to enable human-in-the-loop.

For the full guardrail set and how to verify each one, see the [security guardrails test guide](./README_SECURITY_GUARDRAILS_TESTGUIDE.md).

## Data exfiltration controls

Kasal blocks the common exfiltration paths that indirect prompt injection tries to exploit:

- Secret leak detection scans agent and tool output for leaked credentials (Databricks PATs, AWS keys, Slack and GitHub tokens, PEM private keys, GCP service accounts, Azure connection strings, and generic API keys).
- Frontend rendering is hardened: `react-markdown` uses `rehypeSanitize`, disallows `img` / `script` / `iframe`, and blocks `javascript:`, `data:`, and `vbscript:` URL schemes, which prevents image-based GET exfiltration and link-based XSS.
- HTML preview iframes use a tightened sandbox (no forms, popups, or downloads) plus a Content-Security-Policy that sets `connect-src: none`, so an embedded document cannot make outbound network calls.
- HTTP security headers (Content-Security-Policy, `X-Content-Type-Options: nosniff`, `X-Frame-Options: SAMEORIGIN`, and a strict referrer policy) are applied to every response.
- In multi-crew flows, each crew's task output is scanned (log-only) and leaked secrets are redacted before the output is logged, streamed or persisted. There is no blocking scan between crews; enable the LLM injection guardrail on downstream tasks when you need one.

These controls map to areas 5 to 12 in [security compliance](./README_SECURITY_COMPLIANCE.md).

## Supply chain security

The runtime guardrails above defend against prompt injection, not against malicious code in dependencies. Kasal manages Python dependencies with `uv`, pinned by exact version and sha256 hash (sdist and wheel, including transitive dependencies) in `uv.lock`, so a re-uploaded package whose content does not match the lock is rejected. CI runs `uv lock --check` and `pip-audit` on every pull request and weekly, and Dependabot proposes grouped updates. The supply chain doc analyzes the March 2026 litellm compromise (Kasal was not exposed: it pinned a pre-compromise version at the time) and proposes further dependency-layer defenses.

See [supply chain security](./README_SECURITY_SUPPLY_CHAIN.md).

## Compliance and audit

Kasal's runtime controls are mapped against Databricks AI security guidance, with the corresponding implementation and observed runtime log evidence for each recommendation. The always-on scanners emit consistent, structured `[SECURITY]` audit log entries that surface in the Execution Logs panel, giving operators a record of injection, secret-leak, and trifecta findings.

See [security compliance](./README_SECURITY_COMPLIANCE.md).

## Detailed guides

- [Security compliance](./README_SECURITY_COMPLIANCE.md): mapping of Databricks AI security guidance to its Kasal implementation, with runtime log evidence.
- [Security guardrails test guide](./README_SECURITY_GUARDRAILS_TESTGUIDE.md): verify each guardrail manually and with automated tests.
- [Supply chain security](./README_SECURITY_SUPPLY_CHAIN.md): dependency-layer threats and the defenses proposed in response.

Back to the [documentation hub](./README.md).
</content>
</invoke>
