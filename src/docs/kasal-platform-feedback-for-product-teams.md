# Kasal → Databricks product teams: field-proven asks

**Audience:** Databricks product managers (and their leadership) for Lakebase, Vector
Search, Genie/AI-BI, Model Serving, Unity Catalog, Databricks Apps, MLflow, and Agent
Bricks / Mosaic AI Agent Framework.

**What this is.** Kasal is a Databricks-native agent platform built in the field and
shipped to real customers. It runs *as* a Databricks App and uses the Databricks stack
end to end. To deliver it we had to build a number of agentic-AI capabilities
ourselves. This document lists those capabilities, why a customer needed each one, and
what we would ask the platform to absorb.

**What this is not.** Not a critique, and not a scorecard. Every item here is
something we built *because a customer engagement required it on a deadline* — which
makes each one a piece of validated demand rather than a hypothesis. Where our
implementation is partial or a workaround, we say so explicitly; those notes are the
most useful part of the document.

**Why it should matter to a PM:** Kasal is a working proof that these capabilities
drive consumption of Databricks products. The memory subsystem is why customers turn
on Vector Search and Lakebase. The migration pipeline is why they create Unity Catalog
Metric Views, Lakeview dashboards, and Genie spaces. The demand is already priced in.

---

## 1. One-page summary

| # | Capability we built | Customer need behind it | Platform product it pulls through | Our ask |
|---|---|---|---|---|
| 1 | **Agent memory** (short-term, long-term, entity, document) | Agents that remember across runs, isolated per tenant | **Vector Search**, **Lakebase** | A first-class agent-memory service with tenant scoping |
| 2 | **Human-in-the-loop gates** | Regulated customers cannot let agents act unreviewed | Apps, Jobs | A platform HITL primitive: pause / approve / timeout / resume |
| 3 | **Agent security scanning** | Security review blocked go-live until we could show injection + secret-leak controls | Model Serving, AI Gateway | Platform-side guardrails + a tool capability manifest |
| 4 | **OBO identity through the agent stack** | Every agent action must run as the *end user*, not a service principal | UC, Genie, Vector Search, Lakebase | OBO propagation preserved across async / subprocess boundaries |
| 5 | **Multi-step orchestration** (flows, DAGs, scheduling) | Real work is multi-crew with branching and gates, run on a schedule | Apps, Jobs | Composable multi-agent orchestration, not just single agents |
| 6 | **Per-step observability** (traces, tokens, cost) | "What did the agent do, and what did it cost?" | **MLflow**, UC | Agent-step-level trace + cost attribution out of the box |
| 7 | **Agent-authored UI** (A2UI) | Agent output needs to be a dashboard/document, not a chat blob | Apps, Lakeview | A standard contract for agent-generated UI surfaces |
| 8 | **Export / portability** | Customers will not adopt what they cannot own and run themselves | Apps, DABs | Export-to-owned-asset from managed agent products |
| 9 | **Power BI → Unity Catalog migration** | Migrating BI estates off Power BI is the reason we are in the door | **UC Metric Views**, **Lakeview**, **Genie**, SQL Warehouses | Platform-supported semantic-layer migration |

Products Kasal already integrates: Databricks Apps, Unity Catalog (incl. Metric
Views), Vector Search, Genie/AI-BI, Model Serving + Foundation Model APIs, AI Gateway,
Lakebase, MLflow, SQL Warehouses, Jobs, Volumes, Lakeview, Secrets, Agent Bricks,
Databricks SDK, OBO/OAuth/SPN. (Evidence: `src/utils/telemetry.py` `KasalProduct`
enum; OAuth scopes in `src/deploy.py`.)

---

## 2. The asks, with evidence

### 2.1 Agent memory → pulls through Vector Search + Lakebase
**Need.** Customers expect an agent to remember prior runs, and expect strict
separation between tenants. CrewAI's default memory is local files, which is neither
governed nor multi-tenant.

**What we built.** Four memory types (short-term, long-term, entity, document) behind
one backend interface, with three interchangeable backends: **Databricks Vector
Search**, **Lakebase (Postgres + pgvector)**, and a local fallback. Memory survives
across runs because the crew identity is a deterministic hash of crew structure, and
every record is `group_id`-scoped.
`engines/crewai/memory/{memory_backend_factory,databricks_storage_backend,lakebase_storage_backend,crew_memory_service}.py`

**Why a PM should care.** This is the single biggest driver of Vector Search and
Lakebase usage in our deployments — memory is *why* a customer provisions them.

**Honest caveat.** Entity extraction is unreliable on some Databricks-served models
and falls back to another model. That is a model-behaviour issue, not a storage one.

**Ask.** A managed agent-memory service with tenant scoping and pluggable storage, so
every agent builder is not re-implementing this.

---

### 2.2 Human-in-the-loop → the blocker for regulated adoption
**Need.** In regulated customers, an agent that writes or acts without review cannot
go live. This is a gating requirement, not a nice-to-have.

**What we built.** Approval gates as nodes inside a flow: execution pauses, an
approval record is created with an allowed-approver list and a timeout, webhooks fire
on gate events, and on rejection the flow either fails or retries the prior step.
`models/hitl_approval.py`, `services/{hitl_service,hitl_webhook_service,hitl_timeout_service}.py`

**Ask.** A platform HITL primitive — pause, approve/reject, timeout, resume — usable
from any agent product. We believe this is the most commonly requested missing piece
for agentic AI in the enterprise, and we have the customer evidence behind that claim.

---

### 2.3 Agent security → what security review actually asks for
**Need.** Security review asked concrete questions before go-live: can a prompt
override the agent's instructions; can a secret leak into a trace; which tools can
combine into an exfiltration path.

**What we built.** A scanner pipeline covering prompt-injection heuristics (tiered
severity), secret-leak detection with redaction before persistence, and a **tool
capability manifest** that flags the "lethal trifecta" (reads sensitive data + ingests
untrusted content + can communicate externally) plus destructive-operation tools.
`engines/crewai/security/{scanner_pipeline,prompt_injection_detector,secret_leak_detector,tool_capability_manifest}.py`

**Honest caveat — important.** These are **log-only**. They detect and warn; they do
not block. We deliberately did not put a heuristic in a blocking path. So this is
evidence of the *requirement*, not a claim that we solved enforcement.

**Ask.** Platform-side guardrails on the serving/gateway path, and a standard tool
capability manifest so risk can be assessed declaratively rather than per-tool.

---

### 2.4 OBO identity through the whole agent stack
**Need.** Governance only holds if the agent queries as the *end user*. A service
principal that can see everything defeats Unity Catalog.

**What we built.** An auth chain (OBO user token → OAuth → PAT → env) threaded down to
every Databricks call, including through async offloads and spawned subprocesses,
where the user context is easily lost. `utils/databricks_auth.py`; group isolation in
`models/group.py` and `services/group_service.py`.

**Honest caveat.** Keeping OBO alive across async and subprocess boundaries is subtle
and we have fixed real bugs where the token silently dropped and calls fell back to a
service principal — i.e. a *governance* bug, not just a functional one.

**Ask.** First-class OBO propagation guarantees in agent frameworks and SDKs, so
identity cannot be silently downgraded.

---

### 2.5 Multi-step orchestration
**Need.** Real customer work is not one agent answering once. It is several units of
work with branching, approval gates, and a schedule.

**What we built.** Three execution paths behind one interface — an in-process
single-agent path for chat latency, and subprocess-isolated crew and flow (DAG) paths
— plus cron scheduling. `engines/crewai/paths/{light_agent,crew,flow}/`,
`models/schedule.py`.

**Ask.** Composable orchestration primitives (graph, gate, schedule, isolation) rather
than only a single-agent abstraction.

---

### 2.6 Per-step observability and cost attribution
**Need.** "What did the agent actually do, and what did it cost?" — asked in every
review, by both engineering and finance.

**What we built.** Per-step traces (agent, task, tool) with OTel span structure,
MLflow autologging with secret redaction before export, live streaming to the UI, and
token/usage capture per LLM call — all `group_id`-scoped.
`models/execution_trace.py`, `engines/crewai/infra/mlflow_integration.py`,
`services/{execution_trace_service,trace_broadcast_service}.py`

**Related field note.** We separately built internal consumption tracking and found
that per-product agent cost attribution is only cleanly derivable for token-billed
products; for uptime-billed products it required inference. If agent-level cost
attribution were a platform feature, that guesswork disappears.

**Ask.** Agent-step-level tracing and cost attribution as a default, not an
integration exercise.

---

### 2.7 Agent-authored UI (A2UI)
**Need.** For BI-adjacent work the useful output is a dashboard, document, or
presentation — not a paragraph of chat.

**What we built.** A JSON surface contract (component tree + data bindings) that an
agent emits and a renderer draws, covering conversation, document, presentation,
dashboard, mindmap and other surface kinds, with a component registry (tables, charts,
maps, embeds). Renderer is vendored into every export so surfaces stay portable.
`engines/crewai/exporters/templates/databricks_app/frontend/src/a2ui/`

**Ask.** A standard contract for agent-generated UI, so agent output can render
consistently across Apps and AI/BI instead of each team inventing one.

---

### 2.8 Openness and export — the adoption question
**Need.** The most common enterprise objection we hear is ownership: *"if we build on
this, can we still run it ourselves?"* Customers commit faster when the answer is yes.

**What we built.** An agent built in Kasal exports to a **deployable Databricks App**
(with DABs bundle, config YAML, bundled tools, vendored UI), a **Databricks notebook**,
or a **standalone Python project**. `engines/crewai/exporters/`

**On Agent Bricks specifically — stated carefully.** We integrate Agent Bricks as a
callable tool, so a Databricks-native agent can participate in a Kasal workflow
(`repositories/agentbricks_repository.py`,
`engines/crewai/tools/custom/agentbricks_tool.py`). What we cannot currently do is the
reverse: compose Agent Bricks agents into an external orchestration graph as
first-class citizens, or export one as a customer-owned asset. That asymmetry is a
recurring adoption objection in our deals — customers ask what happens if they want to
move. We raise this as demand signal, not criticism: the integration surface that
exists is what let us adopt Agent Bricks at all.

**Ask.** Two-way openness for managed agent products — callable *and* composable
*and* exportable to an asset the customer owns.

---

### 2.9 Power BI → Unity Catalog migration (the wedge)
**Need.** Migrating a BI estate off Power BI is frequently the reason we are in the
room. The blocker is never the data; it is thousands of DAX measures.

**What we built.** An end-to-end pipeline: extract measures and M-queries from the
Power BI APIs → transpile DAX to SQL (deterministic pattern registry plus a
skill-corpus LLM path) → resolve measure dependencies → emit **Unity Catalog Metric
View** YAML and deploy SQL → optionally generate **Lakeview** dashboards and **Genie**
spaces. `converters/services/powerbi/`, `converters/pipeline.py`,
`engines/crewai/tools/custom/metric_view_utils/`

**Why a PM should care.** This converts a Power BI estate directly into Unity Catalog
Metric Views, Lakeview dashboards, and Genie spaces — the migration is the adoption
event for three products at once.

**Honest caveat, and it is the interesting part.** Not every measure converts. Some
DAX has no metric-view equivalent (slicer-driven scalars, display-only formatting,
disconnected-slicer dispatch). We therefore treat honest reporting as a feature: every
non-converted measure is surfaced with its original DAX and the reason, reviewers can
triage it in the UI, and a re-evaluation sweep re-tests previously-failed measures
whenever transpilation capability improves. **We would rather show a customer a
truthful gap than a silently wrong metric** — and the same principle would serve any
platform migration tooling.

---

## 3. What we are deliberately not claiming

Included because it is what makes the rest credible:

- **Our guardrails do not block.** Detection and logging only.
- **The migration converter is not 100%.** A meaningful share of complex DAX still
  needs a human; we report it rather than hide it.
- **Kasal is not all our own work.** Orchestration builds on CrewAI; LLM routing on
  litellm; tracing on OpenTelemetry. Our contribution is the Databricks-native
  integration, governance, and productisation.
- **We are not claiming these gaps are unknown to you.** Several are likely already on
  roadmaps. The contribution here is evidence of *sequence and priority* from real
  deployments.
- **Kasal is not a product.** It is a field asset. Everything here is offered as input,
  and we would rather hand a capability over than keep owning it.

---

## 4. What we would ask for next

1. **Tell us what is already coming.** Where a roadmap item exists, we will drop our
   implementation and adopt yours — we would prefer to carry less.
2. **Use us as a design partner.** Kasal has real customers exercising these paths
   daily; that is a fast feedback loop for a pre-GA primitive.
3. **Prioritisation input.** If we had to rank by "blocks enterprise adoption today":
   **HITL** first, **agent memory** second, **enforcing guardrails** third, **two-way
   openness/export** fourth.
4. **Take the migration asks.** Semantic-layer migration drives UC Metric View, Genie
   and Lakeview adoption; it deserves platform investment beyond a field asset.

---

*Every capability above is implemented and running; file paths are given so any claim
can be checked in the repository. Where something is partial, the caveat is stated
inline rather than in a footnote.*
