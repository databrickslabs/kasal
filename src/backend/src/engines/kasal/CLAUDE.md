# Kasal Engine CLAUDE.md

Instructions for `src/backend/src/engines/kasal/`. **This layout is post-refactor
(branch `refactor/crewai-engine-structure`).** Older docs/CLAUDE notes that show
`common/`, `helpers/`, `utils/`, `services/`, `mcp/`, or top-level path files are
stale — those packages are gone.

## The three execution paths (this is the core mental model)

The engine has **three distinct answer paths**. Opening a file, first ask "which
path am I in?" The `execution_type` string selects it (see
`services/execution_service.py`):

| `execution_type` | Path | Entry point | Where it runs |
|------------------|------|-------------|---------------|
| `"agent"` | **light** (chat) | `paths/light_agent/light_agent_service.py::run_light_agent_execution` | **in-process**, `Agent.kickoff_async`, sub-second |
| `"crew"` | **crew** | `paths/crew/execution_runner.py::run_crew_in_process` | **subprocess** (`services/process_crew_executor.py`) |
| `"flow"` | **flow** | `paths/flow/flow_execution_runner.py::run_flow_in_process` | **subprocess** |

`kasal_engine_service.py` is the **hub**: it resolves the path and delegates. It
holds no path-specific business logic.

### Light-agent = ChatMode
The **light path is what powers ChatMode / the chat answer mode**. It is a SINGLE
agent (no crew, no tasks/process, no planning/reasoning) run **in-process** for
low latency, and it writes its own terminal status so a fast answer is fetchable
over REST. Its memory wiring, tool/agent tracing, and A2UI surface composition
mirror the crew path but must stay independent — do not merge light and crew
build logic. The full chain is:

```
ChatMode UI → dispatcher/chat routes → ExecutionService (execution_type="agent")
            → KasalExecutionService → KasalEngineService.run_light_agent_execution
            → LightAgentService.run_light_agent_execution (in-process)
```

## Directory map (current)

```
engines/kasal/
├── kasal_engine_service.py   # hub: dispatch crew/flow/light + status/cancel
├── config_adapter.py          # config-shape normalization
├── paths/                     # one package per execution path
│   ├── light_agent/           # the chat/light single-agent path (in-process)
│   ├── crew/                  # crew_preparation, execution_runner, agent/task adapters
│   └── flow/                  # flow_runner_service, backend_flow, modules/
├── kernel/                    # path-AGNOSTIC single-source build logic (was common/)
│                              #   agent_builder, agent_tools, task_builder,
│                              #   agent_security, model_conversion_handler, a2ui_runner...
├── memory/                    # memory_hooks ONLY — recall before a task, persist after
├── infra/                     # logging_config, trace_management, mlflow_integration, crew_logger
├── guardrails/                # GuardrailWrapper ONLY — the engine's guardrail label
├── config/                    # crew/embedder/manager config builders
└── callbacks/                 # the crew's own step/task hooks + the volume writer
```

## Rules

- **Put path-specific code under `paths/<path>/`; put shared build logic in
  `kernel/`.** If crew and flow need the same behavior, it belongs in `kernel/`
  (single source of truth), not copied into both. `paths/crew/agent_adapter.py`
  and `paths/flow/agent_adapter.py` share a basename on purpose — the directory
  names the path, and both delegate to `kernel/`.
- **Do not merge the light and crew paths.** They intentionally have separate
  memory wiring and surface composition; the ~8 lines of glue that differ are not
  duplication to eliminate.
- **Subprocess boundary is the highest-risk area.** Crew and flow build inside a
  spawned interpreter (`services/process_crew_executor.py`). After moving/renaming
  any module the crew or flow path imports, verify with a **real subprocess run**,
  not just in-process unit tests — module-path changes that pass in-process can
  still break the spawned interpreter's import resolution.
- **Lakebase in subprocesses**: the spawned interpreter must re-activate Lakebase
  itself (`db.database_router.activate_lakebase_in_subprocess`); it is not
  inherited from the parent's hot-swap.
- **Guardrails**: build via `GuardrailFactory.create_guardrail` from
  `src.services.guardrails`. Add reusable ones under `services/guardrails/core/`,
  demo/one-off ones under `services/guardrails/demo/`. Only `GuardrailWrapper`
  (the engine's label for a built guardrail) lives here.
- **Memory**: use `src.services.memory` — `MemoryBackendFactory` + `CrewMemoryService`. See
  `src/backend/CLAUDE.md` for the crew-ID determinism and Vector Search schema
  rules (do not hardcode index columns; never call `.value` on enum-valued
  Pydantic fields).
- **Naming caveats to know**: `infra/trace_management.py` is the execution-LOGS
  writer (trace persistence moved to OTel/MLflow); `infra/crew_logger.py`
  (`CrewLogger`) is **live** despite an old audit note — it is used by the engine
  hub and several services.
- **One subscriber owns the event bus: `OTelEventBridge`.** Every trace row a run
  produces comes from it, via `KasalDBSpanExporter`. Do not add a second listener
  class that writes traces — that is how the codebase ended up with three
  generations of dead listeners. If a new event needs to show up in the trace,
  map it in the bridge (`_EVENT_SPAN_MAP` **and** the `_EVENT_CLASSES`
  subscription list in `register()` — a mapping without a subscription writes
  nothing, silently).
- All engine work is async; Databricks calls need User-Agent telemetry
  (`src/backend/CLAUDE.md`).

## What is NOT here any more

Capabilities moved to `src/services/` so they can be used without a crew run —
by crew generation, a chat turn, or an exported app. The engine keeps
orchestration: what runs, in what order, wired to what.

| moved to | what |
|---|---|
| `services/tools/` | every tool + the tool factory (flat — no `custom/`) |
| `services/memory/` | backends, vector store, factory, maintenance, crew_memory |
| `services/guardrails/` | the contract, the registry, and every policy |
| `services/security/` | injection scanning, secret redaction, capability manifest |
| `services/knowledge/` | uploads, embedding, and `KnowledgeSearch` |
| `services/export/` | Databricks App / notebook / python-project export + templates |

The test for which side something belongs on: does it import `kasal_engine`
(a vendored LIBRARY — that is fine in services, like `BaseTool`), or
`src.engines.kasal.*` (this orchestration layer — that is not)?

## Related
- `src/docs/crewai-engine-refactor-proposal.md` — the full refactor record (§7 = final state)
