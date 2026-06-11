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
