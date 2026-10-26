# Validation and security for upstream contributions

Run backend checks from `src/backend` with `uv sync --frozen` followed by
`uv run python run_tests.py`. The runner propagates test and quality-check
failures and does not skip missing quality tools. Run frontend checks from
`src/frontend`: `npm ci`, `npm run test:run`, `npm run lint`, and `npm run build`.
Tests must mock paid providers and cloud services; they must not depend on a
contributor's Databricks CLI credentials.

## Typing debt

The strict mypy configuration remains enabled. The readiness review of
`985ecf22` found 4,313 diagnostics across 566 backend files after correcting a
comment that prevented mypy from parsing the source. This is existing debt,
not a clean type-check result.

`check_types.py` compares the complete strict result to `mypy-baseline.json`.
It fails on new diagnostic counts per file/message, tool crashes, or missing
tools. Line numbers are excluded so formatting does not create false changes.
New modules have no allowance. The baseline must shrink as errors are fixed;
do not increase it to pass a contribution. Run `uv run mypy src` for all
remaining errors. Counts cannot distinguish two identical errors moved within
the same file, so code review still matters.

## Dependencies requiring continued review

The locked dependency audit currently reports five advisories in two packages
without published fixed versions. The audit workflow explicitly lists the
exceptions and runs on pull requests and weekly. These are monitored exceptions,
not a claim that the dependency tree is vulnerability-free.

- **ChromaDB 1.1.1** is installed transitively through CrewAI. Four advisories
  affect its server endpoints and tenant authorization. Kasal does not start or
  mount a Chroma server, and its CrewAI adapter forces `Crew(memory=False)`;
  the adapter routes memory through Kasal's group-scoped memory services.
  These server attack paths are therefore not exposed by Kasal's supported
  deployment. Do not enable a Chroma server in that deployment without a new
  review. [Code-injection advisory](https://github.com/advisories/GHSA-f4j7-r4q5-qw2c).
- **DiskCache 5.6.3** can deserialize malicious pickle if an attacker can write
  its cache directory. Kasal no longer enables LiteLLM disk caching: `local` is
  the default and legacy `disk` settings fall back to memory. Redis remains
  available for shared caching. DSPy remains a transitive consumer, but the
  Kasal MemAlign bridge explicitly disables caching on its LLM and embedder.
  Native chat/crew/flow transport calls do not use this LiteLLM cache.
  [DiskCache advisory](https://github.com/advisories/GHSA-w8v5-vhqr-4h9v).

Recheck published fixes and remove exceptions when an upgrade clears them.
Extending a runtime to use either dependency's disabled functionality requires
reviewing this exposure assessment again.

## Outbound connections

Tenant-supplied webhooks and A2A endpoints validate DNS at connection time and
connect to the validated numeric IP, preserving HTTP Host and TLS SNI. HTTPX
clients disable environment proxy mounts and redirects. Aiohttp webhook delivery
uses the same public-address policy in its resolver and closes it after use.
The existing event-trigger private-webhook deployment opt-in is retained;
operators enabling it intentionally permit private destinations.

Regression tests cover rebinding between preflight and connection, IPv6,
TLS hostname preservation, proxy environments, mixed DNS answers, and timeouts.
This does not replace deployment egress controls or a penetration test.
