# Security

Kasal runs AI agent workflows against your enterprise data, so security spans identity, workspace isolation, agent guardrails, and the dependency supply chain. This page is a concise overview; each section links to the detailed guide for depth.

## Identity and on-behalf-of (OBO)

Every action runs as the signed-in user. All Databricks resource access (Unity Catalog, Vector Search, Genie) uses on-behalf-of (OBO) user authorization, so agents operate within the scope of the user's own token rather than elevated service principal credentials. An agent can only read what the user is already authorized to view, and Kasal does not grant tools permissions beyond the specific API calls they wrap. Because Kasal relies on OBO, it does not store long-lived Databricks tokens in `.env` files, and API keys are stored encrypted in the database rather than as plain environment variables.

See [security compliance](./README_SECURITY_COMPLIANCE.md) (design items D1 to D4) for details.

## Workspace isolation

Kasal is multi-tenant and group-aware. Resources and permissions are scoped to the workspace (group) context so that one workspace's data, executions, and configuration do not leak into another. All LLM and embedding calls route through Databricks model serving endpoints in the workspace; the platform does not create or use fine-tuned models that could encode sensitive information.

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
- In multi-crew flows, one crew's output is scanned at the trust boundary before it becomes the next crew's input, limiting injection propagation.

These controls map to areas 5 to 12 in [security compliance](./README_SECURITY_COMPLIANCE.md).

## Supply chain security

The runtime guardrails above defend against prompt injection, not against malicious code in dependencies. Kasal manages Python dependencies with `uv`, pinned by exact version and sha256 hash (sdist and wheel, including transitive dependencies) in `uv.lock`, so a re-uploaded package whose content does not match the lock is rejected. The supply chain doc analyzes the March 2026 litellm compromise (Kasal was not exposed because it pins a pre-compromise version) and proposes further dependency-layer defenses.

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
