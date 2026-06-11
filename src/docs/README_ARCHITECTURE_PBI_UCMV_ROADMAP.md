# PowerBI / UCMV / Genie / Dashboard Tooling — Architecture Assessment & Next-Release Roadmap

**Date:** 2026-06-11
**Scope:** All CrewAI tools and converter services in the PBI → UCMV → Genie/Dashboard pipelines:
PowerBI analysis/fetcher/DAX/reducer/report-references/relationships/hierarchies/field-parameters
tools, UC Metric View generator/validator/deployer, Genie config generator + space generator,
PBI Visual-UCMV mapper, dashboard creator, mquery conversion pipeline, and the
`converters/services/{mquery,powerbi,uc_metrics}` services.
**Status baseline:** post PR-52 (LLMManager + ToolSessionProvider centralization, contextvars
bridge, group-id policy unification) — all fixes live-validated against real flow executions.

---

## 1. Current state (validated)

### 1.1 Centralized infrastructure — DONE, do not regress

| Concern | Single implementation | Enforcement |
|---|---|---|
| LLM calls | `core/llm_manager.LLMManager.completion()` / `configure_crewai_llm()` | Zero direct `litellm`/`crewai.LLM`/OpenAI-SDK calls in `engines/crewai/tools/**` and `converters/**` |
| DB sessions in tools | `engines/crewai/tools/tool_session_provider.ToolSessionProvider` | No tool builds its own session/engine |
| Sync→async bridging | `engines/crewai/tools/async_bridge.run_async_with_context()` | Zero bare `ThreadPoolExecutor.submit(asyncio.run, …)` offloads — ContextVars (group_id, OBO token) always propagate |
| Tenant identity | `utils/user_context.UserContext`; resolution order: explicit config → trace context → UserContext → fail loud | `agent_helpers`/`task_helpers` raise `ValueError` on missing group_id; no silent `'default'` fallbacks in LLM paths |
| Databricks auth for LLM | LLMManager internal chain (OBO → API-key service) | No tool/converter gates LLM usage on `llm_workspace_url`/`llm_token` config anymore |
| Telemetry | `utils/telemetry.get_user_agent_header(KasalProduct.*)` | Present on all Databricks-bound calls in scope |

**Regression guards:** `tests/unit/engines/crewai/tools/test_async_bridge.py` (context propagation
through the thread bridge, per-tool), LLM test seam is `src.core.llm_manager.LLMManager.completion`
(never `litellm.completion`).

### 1.2 Known-good behaviors to preserve

- Fail-open for *optional* enrichment (semantic enrichment, DAX LLM fallback, LLM guardrails);
  fail-closed for tenant identity.
- Flow checkpoint/resume + HITL gates + per-crew OTel traces.
- Standalone-flow DB fallbacks: Genie config generator and UCMV validator read the latest UCMV
  Generator output from the DB when `ucmv_output` is not flow-injected.

---

## 2. Findings — debt to address next release

### F1 — God-tools: business logic lives in the tool layer (HIGH)

Measured line counts (target per project standards: ≤ 500):

| File | Lines |
|---|---|
| `powerbi_analysis_tool.py` | 3,560 |
| `powerbi_semantic_model_fetcher_tool.py` | 2,534 |
| `powerbi_semantic_model_dax_tool.py` | 2,345 |
| `powerbi_report_references_tool.py` | 2,099 |
| `mquery_conversion_pipeline_tool.py` | 1,520 |
| `powerbi_field_parameters_calculation_groups_tool.py` | 1,463 |
| `powerbi_metadata_reducer_tool.py` | 1,266 |

These tools contain extraction pipelines, caching strategy, retry loops, prompt construction,
SQL generation, and output formatting inside `BaseTool._run`. The project's own layering
(API → Service → Repository, UoW transactions) is bypassed: the tool *is* the service.
Consequence observed in practice: the group-id resolution bug existed in three copies
(config generator, metadata reducer ×2 sites); a copy-pasted retry condition shipped broken
in the validator.

### F2 — Implicit, unversioned inter-tool data contracts (HIGH)

The pipeline communicates over three fragile channels:

1. **Flow injection of raw JSON strings** — `ucmv_output`, `report_references_json`,
   `measures_json`, `genie_config_override` are untyped strings with magic keys, parsed
   ad-hoc in each consumer.
2. **The metadata cache used as IPC** — Fetcher → Reducer → DAX generator hand off via
   `powerbi_semantic_model_cache` rows keyed on `(group_id, dataset_id, workspace_id,
   report_id='reduced')`. The contract is implicit; a group-resolution mismatch produced a
   silent no-op Reducer (found and fixed 2026-06-11, but the contract remains unchecked).
3. **`execution_trace` scraping by display name** — validator and Genie config generator locate
   upstream output via `span_name LIKE 'UC Metric View Generator%run'`. Renaming a tool/crew
   silently breaks downstream consumers.

### F3 — Observability gap: silent degradation is invisible (MEDIUM-HIGH)

Tool module loggers (`logging.getLogger(__name__)`) are not routed to the file handlers in the
flow-execution subprocess — `[GenieConfigGen]`/`[PBIVisualMapper]` runtime lines never reach
`logs/*.log`; behavior had to be reconstructed from the traces DB. Combined with fail-open
design there is **no signal when a fallback activates**: the original C1 bug (LLM path dead,
structural fallback served) was indistinguishable from healthy operation.

### F4 — Residual duplication (MEDIUM)

- 5 tools carry a private `_authenticate()` (identical OBO→PAT→SPN bootstrap in a worker thread).
- 2+ private `_parse_llm_response()` implementations (markdown-fence stripping + JSON parse).
- Structural-fallback mapping patterns copy-pasted between mapper/genie tools.

### F5 — Transaction & persistence seams (MEDIUM)

- `ToolSessionProvider` is a parallel mechanism to `core/unit_of_work.UnitOfWork` with
  inconsistent commit ownership: `conversion_repo()` requires callers to reach through to
  `repo.session.commit()`; `cache_service()` persists only because the repository commits
  internally.
- `metric_view_validator_tool.py` issues raw SQL with `::text` casts (PostgreSQL-only; breaks
  the documented SQLite dev path) and unindexed `LIKE '%…%'` scans against `executionhistory`.

### F6 — Per-call config overhead (LOW)

`LLMManager.completion()` performs a fresh DB session + model-config + API-key lookup per call.
Batch loops (DAX translation, semantic enrichment: dozens of calls per run) pay this every
iteration.

---

## 3. Next-release work plan

Ordered by value/effort. WP1 and WP2 are independent; WP3 depends on WP2; WP4 follows WP3.

### WP1 — Observability: make degradation visible (S effort)

1. **Route tool loggers to files in subprocesses.** In the flow/crew subprocess bootstrap
   (where the `crew` logger is configured), attach the existing file handlers to the
   `src.engines.crewai.tools` logger subtree (or set `logging.getLogger('src')` propagation)
   so `[GenieConfigGen]`-style runtime lines land in `crew.log`.
2. **Fallback-activation metric.** Add a single helper, e.g.
   `core/llm_manager.log_fallback(component: str, reason: str)` (log line with a stable
   `[FALLBACK]` prefix + counter), and call it at every structural-fallback site:
   pbi_visual_ucmv_mapper, ucmv_genie_config_generator, dax_llm_fallback, llm/self-reflection
   guardrails (fail-open branch), mquery `_rule_based_conversion`.
3. **Acceptance:** one grep — `grep '\[FALLBACK\]' logs/crew.log` — answers "did any LLM path
   silently degrade in this run?"

### WP2 — Typed inter-tool contracts (M effort)

1. **Create `schemas/pipeline_contracts.py`** (Pydantic v2) with versioned models:
   - `UCMVOutput` (yaml: dict[str, str], sql, stats, measures_with_dax, deployment_results…)
   - `ReducedModelContext` (tables, measures, relationships, sample_data, default_filters…)
   - `VisualMappings` (visual_id, page_name, visual_type, ucmv_view, dimensions, measures, sql)
   - `GenieSpaceConfig` (text_instructions, sample_questions, example_sqls_json, join_specs…)
   Each with `schema_version: int = 1` and a tolerant `parse_lenient(cls, raw: str | dict)`
   classmethod that logs (not raises) on unknown fields.
2. **Consume at boundaries only:** producers `.model_dump_json()`, consumers `parse_lenient()`.
   Internal logic keeps working on the typed object instead of `dict.get` chains.
3. **Kill `span_name LIKE` scraping.** Add
   `ExecutionTraceRepository.get_latest_output_by_event_type(event_type: str, group_id: str)`
   keyed on the stable `event_type` column (e.g. `'uc metric view generator_run'`) instead of
   the display-name `span_name`, **scoped by group_id** (current queries are cross-tenant).
   Replace the three raw-SQL fetchers in `metric_view_validator_tool` and the Genie config
   generator's DB fallback with this method. This also resolves the `::text`/SQLite issue (F5b).
4. **Acceptance:** renaming a crew/tool display name does not break any downstream tool;
   validator/Genie fallback tests pass on SQLite.

### WP3 — Domain-service extraction from god-tools (L effort, the core of the release)

Target structure (mirrors existing `converters/services/*`):

```
src/services/powerbi/
    semantic_model_service.py      # extraction (TMDL/Scanner/DAX 3-tier), enrichment, caching
    dax_generation_service.py      # NL→DAX prompting, self-correction loop, execution
    metadata_reduction_service.py  # fuzzy/llm/combined selection, dependency resolution
    report_references_service.py
src/services/uc_metrics/
    metric_view_generation_service.py
    metric_view_validation_service.py
    genie_config_service.py
    visual_mapping_service.py
```

Rules for the extraction:

1. **Tools become ≤ ~150-line adapters**: schema definition, config merge (`_default_config` +
   kwargs), one `run_async_with_context(service.execute(request))` call, JSON serialization of
   the typed result. No prompts, no SQL, no caching logic, no retry loops in tools.
2. **Services are async-native** (no thread bridges inside services — bridging happens once,
   in the tool adapter) and take `AsyncSession`/UoW via constructor like every other service.
3. **Unify ToolSessionProvider with UnitOfWork**: provider context managers become thin
   factories over `UnitOfWork` with commit-on-success semantics; remove
   `repo.session.commit()` reach-through (currently used by `powerbi_semantic_model_dax_tool`).
4. **Shared helpers replace duplication (F4):**
   - `engines/crewai/tools/databricks_tool_auth.py` → one `authenticate(host_override)` used by
     the 5 tools with private `_authenticate`.
   - `core/llm_response_parsing.py` → one `parse_json_response(text)` (fence-stripping + lenient
     JSON) replacing private `_parse_llm_response` copies.
5. **Migration order** (one tool per PR, tests moved with the logic):
   reducer (smallest LLM surface) → fetcher → DAX tool → analysis tool (largest, last).
6. **Acceptance:** every file in `engines/crewai/tools/custom/` ≤ 500 lines; unit tests target
   services directly (no `BaseTool._run` mega-tests); behavior parity proven by the existing
   flow suite (`flow_test_3`, `flow_genie_space_gen`, `flow_dashboard_deployer`).

### WP4 — Promote engine-agnostic infrastructure to core (S-M effort, after WP3)

Move (imports only, no behavior change):

- `engines/crewai/tools/async_bridge.py` → `core/async_bridge.py`
- `engines/crewai/tools/tool_session_provider.py` → fold into `core/unit_of_work.py` factories
  (per WP3.3)
- Guardrail LLM-judge + caching primitives and the security scanner pipeline → `core/` (they
  are engine-agnostic; the CrewAI guardrail classes stay as adapters)

`engines/crewai/` afterwards contains only CrewAI adapters: crew/task/flow assembly, callbacks,
tool wrappers. Acceptance: `engines/` never imported from `core/`, `services/`, `converters/`
(one-way dependency, enforceable with import-linter).

### WP5 — Small optimizations (S effort, opportunistic)

- Short-TTL (60s) in-process cache for model-config + API-key resolution in
  `LLMManager.completion()` keyed `(group_id, model)` — batch loops stop paying a DB roundtrip
  per call (F6).
- Replace deprecated `asyncio.get_event_loop()` remnants; remove dead `workspace_url`/`token`
  plumbing still accepted "for backward compatibility" once no caller passes it
  (`MQueryLLMConverter.__init__`, `models.ConverterConfig.llm_*`, tool schemas exposing
  `llm_workspace_url`/`llm_token`).

---

## 4. Explicitly out of scope for next release

- Rewriting the flow engine or replacing CrewAI.
- New pipeline features (more tools/stages) — **freeze growth of the tool layer until WP3 lands**;
  every new god-tool added now doubles the extraction cost.
- Changing the fail-open/fail-closed matrix (it is deliberate; WP1 only makes it observable).

## 5. Risk notes

- WP3 is the only work package with regression risk; mitigate by per-tool PRs, parity runs of
  the three validated flows after each migration, and keeping tool input schemas unchanged
  (agents/flows must not notice the refactor).
- WP2.3 changes tenant scoping of trace lookups from cross-tenant to group-scoped — verify no
  flow depends on reading another group's UCMV output (it should not; that was a latent
  isolation gap, not a feature).

## 6. Outlook: engine pluggability (CrewAI → user-selectable agent provider)

**The port already exists** — `engines/base/BaseEngineService` (ABC), `BaseToolRegistry`, and
`engines/factory.py` dispatching on `engine_type == "crewai"` — but it has exactly one adapter,
and the god-tools bypassed it. WP3/WP4 are what make a second adapter (LangChain/LangGraph, …)
feasible:

- **WP3 is ~80% of engine independence.** Today ~21k lines of PBI/UCMV domain logic live inside
  `crewai.BaseTool` subclasses; a second engine would have to reimplement all of it. After WP3
  the domain logic is engine-neutral (`services/`), and a new engine needs only thin per-tool
  wrappers (e.g. LangChain `StructuredTool.from_function(service.execute)`).
- **WP4's one-way import rule is the guarantee** that `engines/langchain/` can be added without
  touching core/services/converters — and import-linter enforcement is what keeps the port from
  rotting again.
- **Workflow definitions are already engine-neutral** (nodes/edges + agents_yaml/tasks_yaml);
  each engine's `config_adapter` interprets the same stored config.

**Remaining gaps for a true second engine (future WP6, not next release):** explicit ports in
`engines/base/` for memory backends, guardrails, callbacks/eventing, HITL gates, and trace
emission — all currently CrewAI-shaped (`execution_callback`, `TaskOutput`, listener crews).
Each needs a small interface with the CrewAI implementation as the first adapter.

**Decision guidance:** a second engine is a permanent tax (feature parity, 2× test matrix,
behavioral drift on identical flows). Build the *boundary* now (WP3/WP4 — wanted anyway), build
the second *adapter* only on a concrete driver (CrewAI roadmap/licensing risk, customer
requirement, engine-exclusive capability). The architectural value is optionality: when needed,
a new engine is a bounded adapter project, not a rewrite.

### 6.1 Generic tool layer — target design (the WP3 end-state contract)

Goal: any model/provider can be channeled into any tool, and any engine can host any tool.
Tools currently have three hard couplings to remove: they subclass `crewai.BaseTool`
(framework), they resolve their own `llm_model` string (model choice), and they read ambient
state (`UserContext`, session factories). Inverted design:

**a) Engine-neutral contract** (`core/tooling/contract.py`, no crewai imports):

```python
class ToolContext(Protocol):
    llm: LLMPort                # bound model — the tool never selects one
    db: SessionPort             # UoW/session factory
    identity: GroupIdentity     # group_id + user token, explicit (not ambient)
    emit: Callable[[ToolEvent], None]   # progress / [FALLBACK] events (WP1)

class KasalTool(Protocol):
    name: str
    description: str
    input_model: type[BaseModel]    # WP2 contracts reused as tool schemas
    output_model: type[BaseModel]
    async def execute(self, request, ctx: ToolContext) -> BaseModel: ...
```

**b) Model independence via `LLMPort`** — completion-only protocol
(`async complete(messages, *, temperature, max_tokens) -> str`). The *binding layer* decides
the implementation per tool/tenant (LLMManager-backed today; any provider later) — model
choice becomes configuration resolved once at bind time, not string-plumbing inside `_run`.
Deliberately narrow: streaming/tool-calling stays in LLMManager; the port is only the
injection seam, not a provider abstraction.

**c) One tool, N engine adapters** — generic factories written once:
`to_crewai_tool(tool, binder)` (wraps schema as `args_schema`, binds context, bridges via
`run_async_with_context`), `to_langchain_tool(...)`, and MCP exposure for free (the contract —
name + JSON schema + async execute — is already the MCP tool shape). `BaseToolRegistry` becomes
the single registry the factories read; `tool_factory.py` reduces to one generic loop.

**Payoff matrix:** engine-independent (adapter factories) · model-independent per tool
(LLMPort binding) · tenant-safe by construction (identity injected at bind, no contextvars
bridges inside tools) · trivially testable (`execute(req, FakeContext())`, no patching) ·
headless reuse (API/MCP can invoke tools without any agent framework).

**Sequencing constraint:** only meaningful *after* WP3 — wrapping today's god-tools in this
contract would freeze the mess behind a nicer signature. This contract IS the target shape of
the WP3-extracted services; ambient `UserContext` remains at the HTTP layer and is resolved
exactly once in `binder.bind()`.

### 6.2 Architecture diagrams: current vs. target

#### Current state (as-is) — tools welded to the engine

```
                                CLIENT (React UI)
                                       │  REST
                          API LAYER (FastAPI routers)
                                       │
                  APPLICATION SERVICES (Execution/Flow/CRUD)
                                       │
                  ENGINE FACTORY — exists, but only ONE adapter
                                       ▼
┌══════════════════════════════════════════════════════════════════════════════┐
║  engines/crewai/  ← everything lives in here                                  ║
║                                                                                ║
║  orchestration (legitimate adapter code):                                      ║
║    crew_preparation · flow_methods · callbacks · helpers · guardrails · HITL   ║
║                                                                                ║
║  tool_factory.py — per-tool wiring (~1,700 lines)                              ║
║                                                                                ║
║  tools/custom/ — 35 tools, ALL subclass crewai.BaseTool, and the DOMAIN        ║
║  LOGIC (extraction, DAX, prompts, caching, retries, SQL) is INSIDE them:       ║
║    powerbi_analysis 3,560 · fetcher 2,534 · dax 2,345 · references 2,099       ║
║    · mquery 1,520 · reducer 1,266 · UCMV/Genie/mapper/dashboard …              ║
╚═══════════════════════════════════════╪═══════════════════════════════════════╝
                                        │ (post PR-52 the INFRASTRUCTURE is
                                        ▼  shared — but not the domain logic)
        converters/services (partial) · CORE (LLMManager, async_bridge,
        ToolSessionProvider) · REPOSITORIES + UoW → DB

  ⚠ Cost of a second engine TODAY: engines/langchain/ would need its own
    tools/custom/ — the same ~21k lines re-implemented as langchain.BaseTool
    subclasses, then maintained in lockstep forever (every fix applied twice,
    permanent drift risk). Tools and engine are welded together; this is the
    hidden price of F1, beyond code quality.
```

#### Target architecture (post WP3–WP6)

```
                                CLIENT (React UI)
                                       │  REST
┌──────────────────────────────────────▼──────────────────────────────────────┐
│  API LAYER — FastAPI routers                                                 │
│  (auth, group context extraction → UserContext set ONCE here)                │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│  APPLICATION SERVICES — ExecutionService, FlowService, CRUD services         │
│  (transactions via UnitOfWork; persistence via repositories)                 │
└──────────┬──────────────────────────────────────────────┬───────────────────┘
           │ agentic execution                             │ direct / headless
           ▼                                               │ (REST, MCP — no agent)
┌─────────────────────────────┐                            │
│  ENGINE FACTORY             │                            │
│  engine_type → adapter      │                            │
│  (exists: engines/factory)  │                            │
└──────┬──────────────┬───────┘                            │
       ▼              ▼                                    │
┌────────────┐  ┌────────────┐    THIN ADAPTERS ONLY:      │
│ engines/   │  │ engines/   │    config interpretation,   │
│ crewai/    │  │ langgraph/ │    callbacks, memory &      │
│            │  │ (future)   │    guardrail ports, HITL    │
└──────┬─────┘  └─────┬──────┘                             │
       │ to_crewai_   │ to_langchain_                      │
       │ tool(t, b)   │ tool(t, b)     ← binder injects    │
       └───────┬──────┘                  ToolContext HERE  │
               ▼                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  GENERIC TOOL LAYER — KasalTool registry (one registry, N engine adapters)    │
│  each tool: name + input/output schema (WP2 contracts) + execute(req, ctx)    │
│  ToolContext = { llm: LLMPort, db: SessionPort, identity, emit }              │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  DOMAIN SERVICES — engine-free business logic (the WP3 extraction)            │
│  services/powerbi/{semantic_model, dax_generation, metadata_reduction, …}     │
│  services/uc_metrics/{generation, validation, genie_config, visual_mapping}   │
│  converters/services/{mquery, powerbi, uc_metrics}                            │
└──────────┬─────────────────────────────────────────────┬─────────────────────┘
           ▼                                             ▼
┌─────────────────────────┐                 ┌─────────────────────────────────┐
│  REPOSITORIES + UoW     │                 │  CORE                           │
│  (all DB access)        │                 │  LLMManager (the LLMPort impl)  │
│        │                │                 │  async_bridge · security ·      │
│        ▼                │                 │  logging · telemetry            │
│     DATABASE            │                 │        │                        │
└─────────────────────────┘                 │        ▼                        │
                                            │ Databricks / PowerBI / LLM APIs │
                                            └─────────────────────────────────┘

  DEPENDENCY RULE (import-linter, WP4): arrows only point DOWNWARD.
  engines → tools → domain services → repositories/core. Never upward.
```

Reading notes:

- **"N engines → N tools" is a registry × adapter-factory composition, not an N×N router.**
  The engine factory routes the *execution* (which engine runs this flow); the tool
  registry + per-engine adapter factories route the *tools* (any registered `KasalTool`
  wrapped for whichever engine is active). New engine = one adapter factory; new tool = one
  registry entry; they compose automatically. Model/tenant selection happens once, in the
  binder (`ToolContext`), at the seam between the two.
- **Repositories are storage only** — they are not on the path to engines; services reach
  engines via the factory, and repositories are used *by* services/domain services for
  persistence.
- **The headless right-hand path is the payoff of tools living below the engines:** the same
  tool is callable from a plain REST endpoint or MCP without any agent framework.

### 6.3 Known trade-offs of the target architecture (accept deliberately)

1. **Indirection tax (permanent):** features touch 3–4 files (schema, service, tool adapter,
   registration) instead of one; deeper stack traces; harder onboarding. The trade is
   write-time convenience for change-time safety — correct for a multi-tenant product, but a
   real daily cost.
2. **Engine abstraction designed at N=1:** engines are not truly interchangeable (tool
   descriptions, streaming, state, HITL semantics differ). Keep the contract minimal until a
   second engine exists; expect to re-cut the seam then. Do NOT generalize speculatively.
3. **`LLMPort` is a deliberate ceiling:** completion-only. Tools wanting streaming/native
   tool-calling/structured output must extend LLMManager, not the port — defend this rule or
   the port becomes a litellm re-implementation.
4. **`ToolContext` god-object risk:** DI contexts accrete members. Review rule: every new
   context member requires written justification.
5. **Boundary serialization cost:** validating/round-tripping large payloads (UCMV output is
   ~340 KB) on every hop is measurable. Validate at edges only; pass objects, not JSON
   strings, within a process.
6. **Golden-test decay path:** LLM nondeterminism → flaky parity diffs → rubber-stamped
   baseline refreshes → guard becomes theater. Compare with tolerances (set overlap, not
   exact lists); every baseline refresh is a reviewed decision.
7. **The architecture rotted once already:** `engines/base/` + factory existed and the
   god-tools grew around it. The diagram is not the deliverable — the **CI enforcement**
   (import-linter one-way rule, ≤500-line file check) is. Without it, this document gets
   rewritten in two years.

Most of the value (testability, headless/MCP, single-fix-single-place, structural immunity to
context-propagation bugs) is captured even if a second engine never ships; the generality is
a bonus, not the justification.

### 6.4 Why the modifications are still worth it (the counter-case)

Each pro is evidenced by a concrete incident from the 2026-06 validation week:

1. **Bug-class elimination** — the group-id bug existed in 3 copies, the retry-condition bug
   was a copy-paste casualty, 5 tools duplicated `_authenticate`. One resolver/bridge/helper
   makes these bugs structurally impossible. The current architecture re-charges this week's
   multi-day forensics cost on every future incident.
2. **Silent degradation becomes visible** — C1 (LLM path dead, flows green) survived for weeks
   because logic, fallback and invisibility shared one blob. Thin tools + `emit` + golden
   baselines turn the next C1 into a failing test within minutes.
3. **Testing cost collapses** — `execute(request, FakeContext())` replaces patch/mock-session/
   thread-bridge scaffolding; the stale-patch failure mode that masked C1 disappears.
4. **Tenant isolation by construction** — identity injected once in the binder; a tool cannot
   leak across tenants by forgetting contextvars semantics (4 tools had).
5. **Headless/MCP surface is near-term product value** — PBI/UCMV/Genie tools callable from
   REST/MCP/notebooks without an agent crew, at ~zero marginal cost once tools sit below the
   engines (unlike the second engine, this is not speculative).
6. **Velocity compounds instead of decaying** — ≤500-line services reverse the god-file slowdown
   curve; AI-assisted development and code review both work dramatically better on small,
   focused files guarded by golden tests.
7. **Cheap framework insurance** — CrewAI churn (e.g. the 1.14-vs-1.9 Codex handler break,
   2026-06-11) becomes a one-directory concern instead of touching everything.

**Decision logic:** costs are bounded and front-loaded (~6–8 wks + modest indirection tax);
the cost of not acting is unbounded and compounding (every new tool deepens the welding, every
incident repeats the forensics, migration price grows per god-tool added — hence the tool-layer
growth freeze). The target shape is already proven inside this repo: the converters are built
exactly this way and caused the least trouble of the entire pipeline. WP3 promotes the proven
pattern from one slice to the whole.

---

## 7. Implementation sequence & effort estimate

**Phase 0 — Golden outputs ✅ DONE (2026-06-11).** The validated outputs of the four flows
(`flow_test_3`, `flow_genie_space_gen`, `flow_dashboard_deployer`, analyst crew) were extracted
from `execution_trace` and committed as sanitized, version-controlled baselines under
`src/backend/tests/golden/` (4 files), with structural guard tests in
`src/backend/tests/unit/golden/test_golden_baselines.py` (11 tests) and comparison rules in
`tests/golden/README.md`. This is the parity suite for every WP3 migration: after each tool
extraction, re-run the matching flow, sanitize the fresh output the same way, and diff the
structured fields against the baseline.

**Phase 1 — WP1 + WP2 (~1.5–2 wks).** WP1 first — the `[FALLBACK]` metric and routed logs are
the safety net that detects silent degradation *during* the WP3 migration, not just after.
WP2 second — the Pydantic contracts define the service I/O signatures WP3 is written against.
(WP1: 1–2 d · WP2: 3–5 d)

**Phase 2 — WP3 incremental, one tool per PR (~3–5 wks).** Define the §6.1
`KasalTool`/`ToolContext` contract first (1–2 d; fold it into WP3, do not defer to WP6), then
migrate per tool, each PR gated on a parity run vs. golden outputs:

| Order | Tool(s) | Lines | Est. |
|---|---|---|---|
| 1 | Metadata Reducer (pathfinder) | 1,266 | 2 d |
| 2 | Validator + Genie config gen + Visual mapper | ~1,400 | 2–3 d |
| 3 | Semantic Model Fetcher | 2,534 | 3–4 d |
| 4 | DAX Generator | 2,345 | 3–4 d |
| 5 | mquery pipeline + Report References | ~3,600 | 3–4 d |
| 6 | Analysis tool | 3,560 | 4–5 d |

Heavy regression testing is **per-tool and continuous** (parity gate on every PR), not a
big-bang phase at the end. The sequence is interruptible: any prefix ships safely.

**Phase 3 — WP4 + WP5 (~3–4 d).** Mechanical after WP3: import moves + import-linter rule +
model-config TTL cache. Test suite benefit: services tested with a `FakeContext` (no
LLMManager patching, no thread bridges) — expect materially smaller, faster PBI test files.

**WP6** stays parked until a concrete driver exists; post-Phase 3 it is a ~2–3 wk adapter
project.

**Total: ~6–8 engineer-weeks focused work (WP1→WP5).** AI-assisted development compresses
calendar time (~3–5 wks realistic), but parity flow runs and human review of service
boundaries do not compress. Clean release-split line if needed: WP1+WP2+tools 1–3 in release
N, rest in N+1. Largest uncertainty: the analysis tool (assume 2–3 latent surprises).
