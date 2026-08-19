# Dual harness: Kasal runtime and CrewAI, chosen per run

Branch: `engine/crewai-dual-engine`
Status: **Phases 0–6 done, then renamed.** Both harnesses run all three paths.
The only capability CrewAI does not claim is `EXPORT`, a deliberate boundary
rather than a gap.

**The runtime is called a HARNESS**, and it is chosen in **Configuration →
Engines**, which is the one place. A run MAY still name its own on
`CrewConfig.harness` / `FlowConfig.harness`, and the recorded value is what a
resume reads back — but nothing in the UI sets it: the per-run pickers were
built, tried, and removed, because choosing a runtime is an operator decision
and putting it beside the model asked every chat user to have an opinion about
agent frameworks. The pre-existing `EngineConfig` table
and `engine-config` router keep their names: they are the settings store this
platform has always had (they also hold `flow_enabled` and the otel switches),
and the harness is simply a row in them.
Target CrewAI: **1.15.16** (latest on PyPI as of 2026-08-18)

## What this is

Today the platform runs exactly one agent runtime: Kasal's own, in
`services/execution/runtime/`. This plan reintroduces CrewAI as a *second*
runtime behind an **engine layer**, so an operator picks the engine in
Configuration → Engines and every run afterwards uses it — including runs that
happen in a spawned subprocess, and including a resumed run started under the
other engine.

This is deliberately *not* a revert. The commit that removed CrewAI
(`3bc0ca00`) was a **rename**: `src/engines/crewai/` became `src/engines/kasal/`,
and the tree then moved into `services/execution/` (`d494a9cd`). 670 files
changed in the rename alone and ~600 commits have landed since. There is no old
CrewAI integration to restore — the current adapters *are* it, retargeted.

That is the plan's biggest asset. The whole build layer is already CrewAI-shaped.

## The three facts that make this feasible

1. **Kasal's runtime is an API mirror of CrewAI.** `runtime/types.py` literally
   documents classes as "Engine replacement for crewai.Process",
   "…crewai.tasks.task_output.TaskOutput". `services/tools/base.py` says its
   behaviour "follows crewAI 1.15.5". The event types in `core/events/types.py`
   carry CrewAI's own names (`CrewKickoffStartedEvent`,
   `AgentExecutionStartedEvent`, `LLMCallStartedEvent`, …).
2. **The build layer is engine-agnostic in shape but hard-wired in imports.**
   ~20 modules do `from src.services.execution.runtime import Agent, Task, Crew,
   Process`. Nothing else about them assumes which engine.
3. **CrewAI 1.15 exposes everything we need to plug into**: `crewai.Agent`,
   `Task`, `Crew`, `Process`, `TaskOutput`, `CrewOutput`, `LLMGuardrail`,
   `PlanningConfig`, `Flow`, `CheckpointConfig`, `crewai.tools.BaseTool`,
   `crewai.llms.base_llm.BaseLLM`, and `crewai.events.crewai_event_bus` with
   `BaseEventListener`.

## Dependency reality check (verified, not assumed)

`uv pip compile` was run against the real `pyproject.toml` with
`crewai==1.15.16` added.

**One hard conflict, and it is fixable:**

```
Because crewai==1.15.16 depends on mcp>=1.28.1,<1.29
and kasal-backend depends on mcp>=1.26.0,<1.27.0,
your requirements are unsatisfiable.
```

Bumping `mcp` to `>=1.28.1,<1.29.0` makes the whole set resolve. Kasal's MCP
surface is narrow — `mcp.client.sse.sse_client`,
`mcp.client.streamable_http.streamablehttp_client`, `mcp.shared.exceptions.McpError`,
plus the `mcp_server/` JSON-RPC side — so the bump is a contained risk, but it
gets its own verification step (Phase 0) because MCP tools are a shipped feature.

**No litellm conflict.** CrewAI 1.15 moved litellm to an *extra*
(`crewai[litellm]`, `litellm>=1.84,<2`). We do **not** enable it — see the LLM
bridge below — so the repo's `litellm[caching]==1.74.9` pin is untouched.

**What CrewAI drags in** (45 new transitive packages if `crewai-tools` is
excluded, 53 with it): `chromadb`, `lancedb`, `onnxruntime`, `kubernetes`,
`instructor`, `pdfplumber`, `posthog`, `typer`/`textual`/`uv` (via the mandatory
`crewai-cli` dep). Several hundred MB in the Databricks App bundle.

- **`crewai-tools` is excluded.** Kasal has 38 first-party tools; nothing needs
  `pymupdf`/`pytube`/`youtube-transcript-api`.
- **chromadb/lancedb are unavoidable** with the `crewai` meta-package, but they
  are lazily imported (`crewai/memory/__init__.py` defers `Memory` precisely to
  avoid loading lancedb) and we never enable CrewAI's own memory — see the memory
  bridge. They cost bundle size, not startup time.
- Other version movement (`opentelemetry` 1.34 → 1.42, `pydantic` 2.11 → 2.12,
  `openai` 2.32 → 2.54) is what *latest-everything* resolution produces, not what
  CrewAI forces. The real change will be `uv lock --upgrade-package crewai`,
  which holds the rest of the lock still. Phase 0 measures the actual delta.

**Decided (D1): hard dependency.** `crewai==1.15.16` goes in the main dependency
list, not an extra. "Give users a choice in configuration" only means something
if the choice is always live, and an extra would make the selector's availability
depend on how the app happened to be deployed. Phase 0 still measures the bundle
delta; if it breaks a deploy limit that is a new problem to solve, not a reason
to make the feature conditional.

## Architecture: where the seam goes

### Not this

A second `BaseEngineService` implementation alongside `KasalEngineService`. That
duplicates dispatch, status, cancel, log capture, checkpoint lifecycle, history
and broadcast — the machinery in `services/execution/` that has nothing to do
with which runtime executes a task. It is also what the existing
`engine_factory.py` half-implies, and it would double the surface every future
fix has to land in twice.

### This

**One hub, one set of paths, a swappable runtime binding underneath.**

```
                 ExecutionService  (execution_type: agent | crew | flow)
                          │
                 KasalEngineService                (unchanged: the hub)
                          │
        ┌─────────────────┼─────────────────┐
      chat/          agent_builder/     flow_builder/     (unchanged: the paths)
        └─────────────────┼─────────────────┘
                  execution/kernel/                       (build logic, now engine-parameterised)
                          │
              execution/engines/  ◄── NEW: the engine layer
                          │
            ┌─────────────┴─────────────┐
        engines/kasal/            engines/crewai/
            │                           │
   execution/runtime/              crewai (pypi 1.15.16)
```

`services/execution/engines/` is a **capability package** in the sense
`services/CLAUDE.md` uses the term: it may import the runtime library, it must
not import a path package (`chat/`, `agent_builder/`, `flow_builder/`).

### New package layout

```
services/execution/engines/
├── __init__.py          # resolve(), active_engine(), re-exports the protocol
├── binding.py           # EngineBinding protocol + Capability flags
├── selection.py         # config → EngineChoice; env + payload propagation
├── kasal/
│   ├── __init__.py      # KasalBinding
│   ├── build.py         # kwargs → runtime.Agent / Task / Crew  (today's behaviour, verbatim)
│   └── llm.py           # LLMManager.configure_kasal_llm passthrough
└── crewai/
    ├── __init__.py      # CrewAIBinding
    ├── availability.py  # import guard + version assertion + one clear error
    ├── llm.py           # KasalBackedLLM(crewai BaseLLM) → src.core.llm.transport
    ├── tools.py         # kasal BaseTool → crewai BaseTool wrapper (+ approval/replay hooks)
    ├── events.py        # crewai_event_bus → src.core.events.event_bus bridge
    ├── build.py         # kwargs → crewai.Agent / Task / Crew, with a translation table
    ├── memory.py        # context_providers / output_sinks → crewai callbacks
    ├── guardrails.py    # kasal guardrail callables → crewai Task.guardrail
    ├── checkpoint.py    # crewai CheckpointConfig ↔ kasal checkpoint record   (Phase 4)
    └── flow.py          # crewai.flow.Flow adapter                            (Phase 5)
```

### The contract

```python
class EngineBinding(Protocol):
    name: str                                  # "kasal" | "crewai"
    version: str

    def build_agent(self, **kwargs) -> Any: ...
    def build_task(self, **kwargs) -> Any: ...
    def build_crew(self, **kwargs) -> Any: ...
    async def build_llm(self, model: str, group_id: str,
                        temperature: float | None) -> Any: ...
    def adapt_tools(self, tools: list[Any]) -> list[Any]: ...
    def guardrail(self, description: str, llm: Any) -> Any: ...
    def process(self, name: str) -> Any: ...    # "sequential" | "hierarchical"
    def event_bridge(self) -> AbstractContextManager[None]: ...
    def capabilities(self) -> frozenset[Capability]: ...
```

`build_*` take **the kwargs dict the kernel already assembles** and are
responsible for translating it. That is the important design choice: it keeps
the mapping table in one file per engine instead of scattering
`if engine == "crewai"` across 20 call sites, and it makes "what did CrewAI not
support?" a single readable list rather than an archaeology exercise.

Every kwarg a binding drops is logged once per run at INFO with the reason.
Silent drops are how a run "works" at settings nobody chose — the same failure
mode `configure_kasal_llm`'s temperature bug had.

### Capabilities, not exceptions

```python
class Capability(StrEnum):
    CHECKPOINT_RESUME = "checkpoint_resume"
    TOOL_APPROVAL     = "tool_approval"        # HITL gate
    TOOL_REPLAY       = "tool_replay"
    AGENT_PLAN        = "agent_plan"           # runtime/plan.py
    CONTEXT_PROVIDERS = "context_providers"    # memory recall wiring
    OUTPUT_SINKS      = "output_sinks"         # memory persist wiring
    RUN_DEADLINE      = "run_deadline"         # Crew.run_max_seconds
    HIERARCHICAL      = "hierarchical"
    FLOW              = "flow"
    EXPORT            = "export"
```

The API exposes the active engine's capability set; the frontend greys out what
the engine cannot do instead of letting a user press a button that fails. This
is the mechanism that lets Phases 4 and 5 ship later without the product lying
to anyone in the meantime.

## The five bridges

These are where the work actually is.

### 1. LLM — subclass CrewAI, delegate to Kasal's transport

**This is the highest-leverage decision in the plan.** CrewAI accepts any
`crewai.llms.base_llm.BaseLLM` subclass on `Agent(llm=...)`. So:

```python
class KasalBackedLLM(crewai.BaseLLM):
    """A CrewAI LLM whose calls go through src.core.llm.transport."""
```

One class, and CrewAI runs get, unchanged and for free:

- Databricks OBO → PAT → SPN auth (`utils/databricks_auth`) and User-Agent telemetry
- `DatabricksRetryLLM` / `DatabricksResponsesLLM` / `VLLMFunctionCallingLLM` handlers
- model fallback, RPM control, the output clamp and context-window budget
- `LLMCallStartedEvent` / `Completed` / `Failed` / `LLMStreamChunkEvent` on the
  Kasal bus — meaning **traces, token usage, cost and the live log stream are
  identical between engines with no extra bridging**
- `run_deadline` enforcement, so `Capability.RUN_DEADLINE` holds on CrewAI too

It also sidesteps the litellm version conflict entirely, and means an operator
switching engines does not silently switch LLM provider behaviour — which would
make every "did the engine change the answer?" comparison meaningless.

`LLMManager.configure_crewai_llm(model, group_id, temperature)` is added beside
`configure_kasal_llm`, sharing the model-config lookup, context-window
registration and temperature resolution (extract those into a private helper
first; `manager.py` must not grow).

### 2. Tools — wrap, do not port

`ToolAdapter(crewai.tools.BaseTool)` holds a Kasal `BaseTool` and forwards
`name`, `description`, `args_schema`, and `_run`. The 38 first-party tools are
untouched.

The wrapper's `_run` is also where the CrewAI path picks up the features that
live in the Kasal executor's hook system:

- **tool approval / HITL** — call the same gate `kernel/tool_approval.py`
  installs, before delegating
- **tool replay** — consult `kernel/tool_replay.py` first
- **`ToolUsageStarted/Finished/Error` events** on the Kasal bus, so tool spans
  and the trace timeline match the Kasal engine exactly
- the tool-outcome ledger (`runtime/executor.py`'s `_record_tool_outcome`) that
  feeds `_flag_unavailable_sources` on task output

MCP tools already arrive as Kasal `BaseTool`s via `services/tools/mcp_adapter.py`,
so they need nothing extra.

### 3. Events — one listener, re-emitting onto the Kasal bus

`crewai.events.BaseEventListener` subclass that subscribes to CrewAI's bus and
republishes each event as its Kasal counterpart on `src.core.events.event_bus`.

**Nothing downstream changes.** `OTelEventBridge` stays the only trace writer
(the rule in `execution/CLAUDE.md`), `event_pipe.py` keeps streaming to the UI,
the checkpoint recorder keeps recording, `logs/writer_task.py` keeps writing.

~25 event types to map. The names line up almost 1:1 because Kasal's were
inherited from CrewAI's. The real work is field-level: CrewAI 1.15's payloads
have drifted from the 1.14 shapes Kasal's types were derived from. A generated
mapping table plus a test asserting **every** Kasal event type that
`_EVENT_SPAN_MAP` knows about is either produced by the bridge or explicitly
declared unproducible-on-CrewAI.

Events that have no CrewAI source (`A2UISurfaceEvent`, `PlanUpdatedEvent`,
`ContextCompactionEvent`, `CheckpointUnitSavedEvent`) are emitted by Kasal code
that runs *outside* the engine anyway — the A2UI composer, the plan tool, the
transport's compactor, the shared recorder — so they keep working.

### 4. Memory — keep Kasal's, do not adopt CrewAI's

CrewAI 1.15 ships unified cognitive memory on chromadb/lancedb. We do not use
it: Kasal's memory is Databricks Vector Search + SQLite with group isolation,
deterministic crew IDs and its own recall/persist hooks. Adopting CrewAI's would
fork tenant data across two stores.

Kasal wires memory through `Crew.context_providers` and `Crew.output_sinks`,
which CrewAI does not have. The bridge:

- **recall** → CrewAI `Task` accepts a callable/`before` hook per task; wrap each
  task's description assembly so `memory/hooks.build_memory_preamble` runs and
  its output is prepended, exactly as `_apply_context_providers` does today
- **persist** → CrewAI `Crew(task_callback=...)`; chain
  `memory/hooks._persist_task_output` onto whatever callback the config already
  sets

`Crew(memory=False)` is forced on the CrewAI side so its own memory never
initialises (and lancedb never imports).

### 5. Guardrails

CrewAI `Task.guardrail` accepts `Callable[[TaskOutput], tuple[bool, Any]]` — the
same signature Kasal uses. `GuardrailFactory.create_guardrail` output passes
through unchanged. `LLMGuardrail` maps to `crewai.tasks.llm_guardrail.LLMGuardrail`
but built with `KasalBackedLLM`.

The one gap: Kasal's `guardrail_on_exhausted="degrade"` and the structured
`TaskOutput.degraded` / `degradation_reason` fields have no CrewAI equivalent.
The adapter re-applies the degradation decision after CrewAI returns and stamps
the fields on the Kasal-side output, so "can I trust this run?" stays answerable.

## Selection and stickiness

**Storage.** One new `EngineConfig` row, seeded by migration:

| engine_name | engine_type | config_key | config_value |
|---|---|---|---|
| `execution` | `ai` | `backend` | `kasal` |

**API.** `GET /engine-config/backend` → `{backend, available, capabilities,
unavailable: {crewai: "reason"}}`; `PUT /engine-config/backend` (system-admin
only, matching the existing router's permission checks).

**Resolution happens exactly once per execution**, in `ExecutionService` where
the execution row is created. Then:

1. a new nullable `engine_backend` column on `execution_history` records it
   (Alembic migration; existing rows read as `kasal`);
2. the value is written into the config dict handed to the subprocess
   (`crew_config["_engine_backend"]`) and into `KASAL_ENGINE_BACKEND` in the
   child's environment, for code that runs before config parse;
3. `services/execution/checkpointing/resume.py` reads the column, so a **resumed
   run continues on the engine that started it** even if the setting changed;
4. the Chat path resolves the same way in-process.

This is what "once we switch, it sticks" has to mean in practice: a run never
changes engine mid-flight, and the trace records which engine produced it. The
engine name also becomes a trace attribute and a column in the runs table.

**Never read the config inside the runtime.** `engines/selection.py` is the only
module that touches `EngineConfigService`; bindings receive a decided value.

## Phases

Each phase is independently mergeable and leaves `main` green.

### Phase 0 — dependency (small, must be first)

- `pyproject.toml`: add `crewai==1.15.16`; bump `mcp` to `>=1.28.1,<1.29.0`;
  update the two stale comments that say crewai is gone.
- `uv lock --upgrade-package crewai --upgrade-package mcp`, `uv sync`; record the
  real lock delta.
- Verify the MCP bump: `services/mcp/mcp_client` connect over SSE and
  streamable-HTTP, `services/mcp/mcp_server` JSON-RPC, and the MCP tool path end
  to end.
- Measure the App bundle size delta (`src/deploy.py` ships `pyproject.toml` +
  `uv.lock`); if it breaks a limit, D1 flips to the optional-extra shape.
- Gate: full backend suite green, app boots, one crew run and one flow run pass.

### Phase 1 — the engine layer, Kasal only (the risky refactor, done with no behaviour change)

- Add `services/execution/engines/` with `binding.py`, `selection.py`,
  `kasal/`.
- Convert every `from src.services.execution.runtime import Agent/Task/Crew/Process`
  construction site to go through the binding:
  `kernel/agent_builder.py`, `kernel/task_builder.py`, `config/crew_config_builder.py`,
  `config/manager_config_builder.py`, `agent_builder/{agent_adapter,task_adapter,crew_preparation}.py`,
  `flow_builder/{backend_flow.py,modules/{agent_adapter,task_adapter,flow_methods,flow_builder,flow_processors}.py}`,
  `execution/engine_service.py`, `services/deployment/crew.py`.
- Config row + migration + `engine_backend` column + API + stickiness plumbing.
- Frontend: engine radio in `Configuration/Engines/EnginesConfiguration.tsx`,
  capability-driven disabling, engine badge on the run row.
- `engine_factory.py`: replace the dead `register_engine` stub and the
  `"crewai" is a legacy alias for kasal` branch — that alias must now mean the
  *actual* CrewAI engine, so it is a data-correctness question, not a rename.
  Audit `execution_history.engine_name` for existing `crewai` rows first.
- **Subprocess verification is mandatory here**, per `execution/CLAUDE.md`: a
  real spawned run for both the crew and flow paths, not just in-process tests.
- Gate: zero behaviour change. The parity suite (below) compares Phase 1 output
  against Phase 0 output byte for byte on a fixed seed set.

### Phase 2 — CrewAI binding, Chat path

Smallest surface (one agent, in-process, sub-second), so it proves the LLM,
tool and event bridges before anything harder depends on them.

- `crewai/availability.py`, `llm.py`, `tools.py`, `events.py`, `build.py`
- `chat/service.run_light_agent_execution` builds through the binding
- Gate: a chat turn on CrewAI produces the same trace event *types*, the same
  terminal status, the same A2UI surface behaviour and comparable token
  accounting as the same turn on Kasal.

### Phase 3 — CrewAI binding, Agent Builder (crew) path

- `crewai/build.py` crew/agent/task translation incl. hierarchical + manager
- `crewai/memory.py`, `crewai/guardrails.py`
- subprocess propagation, cancel and status parity
- `Capability.CHECKPOINT_RESUME` **off** for CrewAI; the UI disables resume for
  those runs
- Gate: the standard crew fixtures run to completion on both engines with
  matching task counts, statuses and trace shapes.

### Phase 4 — checkpointing and HITL parity on CrewAI

- Map `crewai.events.types.checkpoint_events.*` into the shared recorder and
  `CheckpointUnitSavedEvent`; implement restore through CrewAI's
  `CheckpointConfig`.
- The storage contract in `checkpointing/record.py` is versioned and migrated on
  read — it does not fork per engine. A checkpoint records which engine wrote
  it and refuses a cross-engine resume with a clear message.
- Tool-approval HITL through the tool wrapper; flow-gate HITL stays on the
  isolated session (`f30addd6`).

### Phase 5 — Flow Builder on CrewAI

`crewai.flow.Flow` vs `flow_builder/runtime/flow.py`. Largest remaining gap
(routing, conditions, checkpoint-per-crew, the conversational flow state). Kept
last and behind `Capability.FLOW` so the rest ships without it.

### Phase 6 — docs, comments, and the parity suite in CI

- Rewrite `services/execution/CLAUDE.md` (it currently asserts CrewAI is gone and
  that `crewai_event_bus` has no alias — both stop being true), plus
  `src/backend/CLAUDE.md`, `services/CLAUDE.md`, and the `pyproject.toml`
  comments about `mcpadapt`/`openpyxl`/template linting.
- `src/docs/` user-facing page: what each engine is, what differs, how to switch.
- Note: the exported Databricks App keeps vendoring the **Kasal** runtime
  (`export/runtime_vendor.py`) regardless of engine. `Capability.EXPORT` is
  Kasal-only, and the export dialog says so. Shipping CrewAI into exported apps
  is a separate project.

## Testing

- **Mirror the tree** — `tests/unit/services/execution/engines/…`, enforced by
  `tests/unit/architecture/test_test_tree_mirrors_source.py`.
- **Parity suite** (new, `tests/integration/engines/`): one fixture set run
  through both bindings, asserting equal terminal status, equal task count, the
  same set of trace `event_type`s, and structurally equal outputs. This is the
  test that makes "switch the engine" a supported operation rather than a hope.
- **Translation-table completeness**: every kwarg the kernel can produce is
  either mapped or explicitly listed as dropped, per engine. Fails when a new
  kwarg appears unclassified.
- **Event-map completeness**: every entry in `_EVENT_SPAN_MAP` is produced by the
  CrewAI bridge or explicitly declared unproducible.
- **Subprocess import test** for every new module, per `execution/CLAUDE.md`.
- **Stickiness test**: start a run, flip the config mid-flight, assert the run
  finishes on its original engine and the resume does too.

## File-size ratchet

Three files this plan touches are already over the 1500-line ceiling:
`agent_builder/process_executor.py` (3067), `execution/service.py` (2702),
`tools/tool_factory.py` (2709); `crew_preparation.py` (1343) is near it. Per
`CLAUDE.md`, none may grow. New code goes in `engines/`, and each of these should
leave a phase smaller than it entered — the obvious seam in `process_executor.py`
is the config-validation and signal-handling preamble in `run_crew_in_process`.

## Risks, ranked

1. **Phase 1's mechanical refactor breaking the subprocess paths.** Module-path
   changes that pass in-process still break a spawned interpreter. Mitigated by
   real subprocess runs as a merge gate, and by keeping public import paths
   stable via re-exports.
2. **The `mcp` 1.26 → 1.28 bump.** Two minor versions on a shipped feature.
   Phase 0 gates on exercising both client transports and the server.
3. **Event payload drift between CrewAI 1.15 and Kasal's 1.14-derived types.**
   Caught by the event-map completeness test rather than by a silently empty
   trace.
4. **Bundle size in the Databricks App deploy.** Measured in Phase 0; D1 is the
   escape hatch.
5. **Two engines to maintain.** Every future runtime feature now has a parity
   question. The capability enum is the honest answer to that: a feature may
   land on one engine, as long as it says so.

## Decisions taken

- **D1 — dependency shape: hard dependency.** See above.
- **D2 — scope: full parity, phases 0 through 6.** Checkpoint/resume, HITL and
  the Flow Builder path all land on CrewAI. The capability enum still ships in
  Phase 1 — it is how the product stays honest *between* phases, and it remains
  the mechanism for anything a future CrewAI release cannot do.
- **D3 — granularity: one global setting.** A single `EngineConfig` row on the
  existing Configuration → Engines page, stamped per execution so runs and
  resumes stick. Not per-group and not per-workflow: both reopen stickiness at a
  second level, and neither has a use case yet.


## Where this stands

**Phase 0 — done.** `crewai==1.15.16` is a dependency, `mcp` is bumped to
`~=1.28.1`, and `uv.lock` regenerated. The real delta is much smaller than the
`uv pip compile` probe suggested: **43 packages added, 10 version changes, none
removed** (242 → 285). The only movement outside crewai's own tree is
`mcp` 1.26→1.28, `opentelemetry` 1.34→1.42 (crewai-core requires ~=1.42),
`pydantic-settings` 2.10→2.15 and `pyjwt` 2.11→2.13. `pydantic` stays 2.11.10,
`litellm` stays 1.74.9, `openai` stays 2.32.0, `fastapi` is untouched.
`import crewai` costs ~2.6s and loads chromadb, which is why the CrewAI binding
is imported lazily and never on a Kasal-only install.

**Phase 1 — the engine layer, backend done.**

- `services/execution/engines/` — `binding.py` (the protocol + `Capability`),
  `selection.py` (DB-free resolution), `kasal/` (a pass-through binding).
- Every construction site now builds through the binding: `kernel/agent_builder`,
  `kernel/task_builder`, `config/{crew,manager}_config_builder`,
  `agent_builder/{agent_adapter,task_adapter,crew_preparation}`,
  `flow_builder/{backend_flow,modules/*}`. Two dead imports of the Kasal `Crew`
  were removed from `engine_service` and `agent_builder/process_executor` —
  both would have loaded the Kasal runtime into a CrewAI subprocess.
- `services/execution/engine_choice.py` — resolve once, stamp on the row, carry
  into the subprocess by payload and environment, adopt process-wide in the
  child, and read the ROW (not the setting) when dispatching or resuming.
- `executionhistory.engine_backend` — model column, startup self-heal (alembic
  does not run at boot here) and migration `20260818_engine_backend`.
- `engineconfig` row `execution`/`backend`, repository + service + `GET`/`PUT
  /engine-config/execution-backend`, and an `ExecutionEngineSelector` on the
  Configuration → Engines page.
- `db/session.py::_heal_engine_config_names` is now SCOPED to the pre-rename
  keys. It rewrote every `engine_name='crewai'` row to `'kasal'` on every
  startup; unscoped, it would have silently un-selected CrewAI at each boot.

**Tests.** ~330 tests patched a construction class the modules no longer name
(`patch("…flow_methods.Crew")`). They now go through
`tests/unit/helpers/engine_double.py` — `patched_engine` / `patch_build` —
which moves the same assertion one layer out, onto the binding. New suites cover
selection, the registry and `engine_choice`.

**Phase 2 — the CrewAI binding, Chat path.** `engines/crewai/` is real, and
`describe_engines()` now reports both engines available with different
capability sets.

- `availability.py` — lazy import guard. Also turns CrewAI's outbound reporting
  OFF (`CREWAI_DISABLE_TELEMETRY` / `_TRACKING` / `TRACING_ENABLED`) before the
  import: it ships telemetry on by default, phoning home from the process that
  just ran a tenant's crew. Deliberately NOT `OTEL_SDK_DISABLED`, which CrewAI
  also honours but which would kill Kasal's own span export.
- `llm.py` — `KasalBackedLLM(crewai.BaseLLM)` forwarding to
  `src.core.llm.transport`. The signatures were already identical, so it adds
  nothing to the request path. The credential stays on the wrapped object.
- `tools.py` — the 38 first-party tools wrapped, `args_schema` passed through
  unchanged so the model sees the same function signature on both engines.
- `events.py` — CrewAI's bus republished onto Kasal's, 12 lifecycle types. LLM,
  tool, memory and guardrail events are NOT bridged: Kasal's own subsystems
  emit them under both engines, and bridging would double every trace row.
- `build.py` — kwargs filtered against the target class's own `model_fields`,
  with `_KNOWN_DROPS` naming every Kasal concept CrewAI lacks and *why*.
  Anything unclassified is dropped with a WARNING.

**Two bugs the first real runs surfaced**, both in the LLM adapter, both now
fixed with regression tests. They share a shape worth naming: CrewAI treats an
LLM as a *capability-bearing collaborator*, not just something with a `call`
method, and both failures came from implementing the call and missing the
conversation around it.

1. **`supports_function_calling` was absent**, and CrewAI probes for it with
   `hasattr`. Finding nothing, it silently fell back to a ReAct PROSE loop where
   the agent writes `Action Input:` as text. Every no-argument tool call then
   failed with "the Action Input is not a valid key, value dictionary", and the
   model retried with an invented `{"dummy": ""}` to satisfy the parser. Same
   tools, same model, fine on Kasal — because the transport has always made
   native tool calls. Now delegated to the transport, along with
   `supports_multimodal` and `supports_native_structured_output`.
2. **The two engines divide tool execution differently, and neither said so.**
   Kasal's transport owns the whole loop: give it `available_functions` and it
   returns final text. CrewAI's executor owns the loop instead — it passes
   `available_functions=None` because it applies reflection prompts, iteration
   limits and its tool-failure policy between rounds. With tools present and no
   functions to call, the transport fell through and returned `""`, which CrewAI
   could only report as `Invalid response from LLM call - None or empty`.
   The transport now takes a declared `delegate_tool_calls` flag (a FIELD, not a
   call kwarg: the handler subclasses do not share one `call` signature, and an
   undeclared constructor kwarg on that class is collected into
   `additional_params` and sent to the endpoint). The adapter enables it and
   translates the calls into CrewAI's shape — as OBJECTS, since
   `is_tool_call_list` accepts a dict but `extract_tool_call_info` reads
   `.function.name` by attribute and would silently skip every dict.

3. **`max_execution_time` reported but did not bind.** CrewAI honours the field
   by running the agent in a `ThreadPoolExecutor` and calling
   `future.result(timeout=...)` — but Python cannot kill a thread and the
   enclosing `with` block joins on exit, so the loop keeps going and the
   `TimeoutError` surfaces only once the agent finishes anyway. Measured: one
   task, a 30s cap, still making LLM calls 145 seconds later.
   The Kasal engine stops the agent because the TRANSPORT enforces a deadline
   inside its round loop — but `resolve_execution_budget` rebuilds that deadline
   on every `call()`, so it only bounds a turn when one call IS the turn. True
   under Kasal; false under CrewAI, where each call is one round.
   `engines/crewai/deadline.py` now stamps `run_deadline` — the one term that
   survives across calls — per agent turn, taking the earlier of the turn cap
   and any run-level ceiling. Verified: **51 rounds / 51.6s against a 2s cap
   before, 2 rounds / 2.0s after.**
4. **The event bridge was only installed on the Chat path.** The crew and flow
   subprocesses never entered it, so under CrewAI a crew run produced LLM, tool
   and memory trace rows but no agent/task/crew lifecycle rows at all — and,
   because the checkpoint recorder subscribes to `TaskCompletedEvent`, **no
   checkpoints**. Phase 4's "writing needed nothing" was right in principle and
   untrue in practice. Both subprocess paths now enter the engine's run scope,
   which is also where turn deadlines install: one lifetime, one install site.

5. **An audit of every configurable agent/task feature** found two more gaps.
   `max_context_window_size` was dropped — CrewAI has no such field, but the
   field is not really CrewAI's business: `transport._effective_context_window`
   reads it off `from_agent`, so it is now carried on the agent and a per-agent
   window override works on both engines. And `guardrail_on_exhausted="degrade"`
   — set automatically by the generated research and deep modes — had no CrewAI
   equivalent, so a crew that degrades on Kasal would ABORT on CrewAI, losing
   everything already produced. `engines/crewai/guardrails.py` applies it by
   wrapping the guardrail callable (public contract only, no CrewAI internals),
   annotating the output in both text and structure.

**Two earlier bugs the work surfaced**, both now fixed and covered:

1. **CrewAI's `emit` is fire-and-forget** — it dispatches handlers on a thread
   pool and returns a Future. Removing the bridge's handlers at teardown
   therefore dropped the tail of every run: exactly the completion events a
   timeline ends on. Teardown now flushes before unregistering.
2. **`tests/unit/services/memory/conftest.py` stubbed `chromadb` and installed a
   permanent `crewai.rag` meta-path finder**, process-wide and never removed.
   Harmless while crewai was absent; with crewai installed it shadowed the real
   library and broke 43 unrelated tests with `AttributeError: __spec__`. Both
   stubs are now conditional on the real package being missing. `require_crewai`
   also refuses to CACHE a module with no `__spec__`, so a test that stubs
   crewai to exercise an "not installed" branch can no longer poison a worker.

**Phase 3 — Agent Builder (crew) path.**

- `engines/crewai/memory.py` — a `crewai.Crew` subclass overriding `_get_context`
  and `_process_task_result`, which are exactly the two points Kasal's
  `context_providers` / `output_sinks` correspond to. The memory subsystem, its
  queries, its events and its group scoping are untouched.
- `Crew(memory=False)` would have taken the Kasal memory OBJECT with it — the
  call sites read the backend back off the crew, so it would have returned
  False and every CrewAI crew would have been silently memory-less. The object
  is carried separately and read through the binding's `crew_memory()`.
- `manager_llm` / `planning_llm` are wrapped alongside the agent's. Missing one
  is a quiet failure: CrewAI accepts the object and only discovers it is not a
  `BaseLLM` at the manager's first call, halfway into a hierarchical run.

**Phase 4 — checkpoint and HITL parity.**

- **Writing needed nothing.** The recorder subscribes to bus events and matches
  tasks by identity; the event bridge already republishes CrewAI's
  `TaskCompletedEvent` carrying the same task object.
- **Reading** is `engines/crewai/checkpoint.py`, consuming the same
  engine-neutral payload `checkpointing/resume.build_crew_payload` produces and
  applying the same longest-matching-prefix rule via the same
  `runtime/identity.py` functions.
- Restored tasks keep their place in the list and have their `execute_sync`
  replaced. Removing them would drop them from CrewAI's `task_outputs` chain,
  so every later task would run with different context than it did originally.
- **HITL and replay** came from one change: the tool adapter now calls
  `runtime/executor.wrap_tool` instead of the tool directly. That is where the
  approval gate, replay, the outcome ledger and all three `ToolUsage*` events
  live, so reusing it makes them true on both engines by construction. CrewAI's
  `before_tool_call` hook supplies the agent and task, without which tool trace
  rows could not be grouped under their task.

**Phase 5 — Flow Builder.** The Flow Builder's ORCHESTRATOR — routing,
conditions, HITL gates, per-crew checkpoints, conversational state — stays
Kasal's own under both engines. The engine setting selects the AGENT RUNTIME,
and every crew a flow composes is built through the binding, so a flow running
under CrewAI executes its agents and crews on CrewAI. Swapping the orchestrator
for `crewai.flow.Flow` was considered and NOT taken: it would re-implement
routing, HITL and checkpointing against a second set of primitives for no
behaviour a user could observe.

**Phase 6 — the parity suite.**
`tests/unit/services/execution/engines/test_parity.py` runs the same input
through both bindings and compares: construction, the LLM path, memory wiring,
the restore prefix, and — the check that keeps the capability enum from becoming
decoration — that a declared capability is one the binding actually delivers.
That last one caught a real over-claim: `TOOL_APPROVAL` and `TOOL_REPLAY` were
declared on CrewAI while its adapter still bypassed the hook pipeline.

**A finding worth recording: task identity is engine-dependent.** CrewAI's
`Task` inherits its agent's tools and Kasal's does not, so the identity hash
differs for what is otherwise the same task — and a checkpoint therefore cannot
cross engines. That is handled rather than papered over: a run's engine is
recorded and its resume reuses it, and a row with NO recorded engine now
resolves to Kasal (the only engine that existed when such rows were written)
rather than to the current setting. Without that, resuming an old run after a
switch would restore nothing while reporting that task 0 had changed.

**Sessions.** `engine_choice.py` no longer opens a session of its own. Both
resolvers take the caller's session; `engine_for_execution` reads the run
through `ExecutionService.get_run_by_job_id` — the execution domain's own
service — rather than constructing `ExecutionHistoryRepository`. The single
remaining acquisition is `dispatch_session()`, used only at the boundary that
dispatches a run and only when the caller genuinely has no session, via
`routed_scoped_session`.

**Pre-existing failures, not caused by this work.** Five PowerBI
`test_converter_service` tests fail on `main` too (verified against a stashed
tree). Four `mlflow/test_mlflow_setup.py::TestConfigureMlflowInSubprocess` tests
reach the developer's real Databricks workspace via `~/.databrickscfg` and
assert `tracing_ready is False`; they fail on any machine that has credentials,
and they are what writes the stray `mlruns/` directory the artifact guard
reports.

**Carried debt.** `services/execution/service.py` (2,7xx lines) and
`agent_builder/process_executor.py` (3,0xx) are over the 1,500 ceiling and each
grew by a few lines here. Per `CLAUDE.md` they owe a compensating extraction the
next time a phase touches them.
