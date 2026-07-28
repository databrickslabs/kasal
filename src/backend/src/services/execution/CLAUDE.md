# Execution CLAUDE.md

Instructions for `src/backend/src/services/execution/` and the three path
packages beside it. **`src/engines/` no longer exists** — any doc, comment or
memory that mentions `src.engines.kasal.*` is stale.

## The three paths (this is the core mental model)

There are **three distinct answer paths**. Opening a file, first ask "which path
am I in?" The `execution_type` string selects it (`services/execution_service.py`):

| `execution_type` | UI name | Package | Where it runs |
|---|---|---|---|
| `"agent"` | **Chat** (ChatMode) | `services/chat/` | **in-process**, `Agent.kickoff_async`, sub-second |
| `"crew"` | **Agent Builder** | `services/agent_builder/` | **subprocess** (`services/process_crew_executor.py`) |
| `"flow"` | **Flow Builder** | `services/flow_builder/` | **subprocess** (`services/process_flow_executor.py`) |

`execution/engine_service.py` (`KasalEngineService`) is the **hub**: it resolves
the path and delegates. It holds no path-specific business logic.

**The wire values are frozen.** `execution_type` is `"agent"`/`"crew"`/`"flow"`
in `execution_history` rows and in what the frontend sends. The package names
were brought over to the UI's vocabulary; the wire values were not, because that
is a data migration. Do not "finish the rename" in a payload or a DB column
without one.

### Chat = the light agent
A SINGLE agent — no crew, no task graph, no planning/reasoning — run
**in-process** for low latency, writing its own terminal status so a fast answer
is fetchable over REST. Its memory wiring, tool/agent tracing and A2UI surface
composition MIRROR the Agent Builder path but stay independent on purpose; the
~8 lines of glue that differ are not duplication to eliminate.

```
ChatMode UI → dispatcher/chat routes → ExecutionService (execution_type="agent")
            → KasalExecutionService → KasalEngineService.run_light_agent_execution
            → chat.service.run_light_agent_execution (in-process)
```

## Layout

```
services/
├── chat/                    # the Chat path (in-process)
├── agent_builder/           # the Agent Builder path (subprocess)
├── flow_builder/            # the Flow Builder path (subprocess)
└── execution/
    ├── engine_service.py    # the hub: dispatch + status/cancel
    ├── engine_factory.py    # builds one KasalEngineService
    ├── base.py              # the engine interface (one implementation)
    ├── config_adapter.py    # normalizes the SHAPE of frontend config
    ├── config/              # crew / embedder / manager config BUILDERS
    ├── kernel/              # path-AGNOSTIC single-source build logic:
    │                        #   agent_builder, agent_tools, task_builder,
    │                        #   agent_security, model_conversion_handler,
    │                        #   tool_approval, execution_callback, trace_context
    ├── logs/                # execution-log capture → queue → DB
    └── subprocess_bootstrap.py  # what a spawned interpreter calls first
```

## Rules

- **Path-specific code goes in that path's package; shared build logic goes in
  `kernel/`.** If Agent Builder and Flow Builder need the same behavior it
  belongs in `kernel/` (single source of truth), not copied into both.
  `agent_builder/agent_adapter.py` and `flow_builder/modules/agent_adapter.py`
  share a basename on purpose — the package names the path, and both delegate
  to `kernel/`.
- **Do not merge the Chat and Agent Builder paths.** See above.
- **The subprocess boundary is the highest-risk area.** Agent Builder and Flow
  Builder build inside a spawned interpreter. After moving or renaming ANY module
  those paths import, verify with a **real subprocess run** — module-path changes
  that pass in-process can still break the child's import resolution:
  ```
  python -c "import subprocess,sys; print(subprocess.run([sys.executable,'-c',
      'import src.services.agent_builder.process_executor; print(1)'],capture_output=True,text=True))"
  ```
- **Lakebase in subprocesses**: the spawned interpreter must re-activate Lakebase
  itself (`db.database_router.activate_lakebase_in_subprocess`); it is not
  inherited from the parent's hot-swap.
- **One subscriber owns the event bus: `OTelEventBridge`.** Every trace row a run
  produces comes from it, via `KasalDBSpanExporter`. Do not add a second listener
  class that writes traces — that is how the codebase ended up with three
  generations of dead listeners. If a new event needs to show up in the trace,
  map it in the bridge (`_EVENT_SPAN_MAP` **and** the `_EVENT_CLASSES`
  subscription list in `register()` — a mapping without a subscription writes
  nothing, silently).
- **Logging**: `src/core/logger.py` owns loggers; `execution/logs/` owns capture;
  `execution/subprocess_bootstrap.py` sets a child interpreter up. Note
  `logs/writer_task.py` drives the LOGS writer — it has never written a trace.
- **Guardrails**: build via `GuardrailFactory.create_guardrail` from
  `src.services.guardrails`.
- **Memory**: `src.services.memory` — `MemoryBackendFactory` + `CrewMemoryService`,
  with `memory/hooks.py` for recall/persist around a task. See
  `src/backend/CLAUDE.md` for crew-ID determinism and Vector Search schema rules.
- All work here is async; Databricks calls need User-Agent telemetry
  (`src/backend/CLAUDE.md`).

## `kasal_engine` is a LIBRARY, not this layer

`kasal_engine/` at the backend root is the vendored agent library — `Agent`,
`Task`, `Crew`, `Flow`, the LLM transport, the event bus, `BaseTool`, memory
primitives. It is the dependency that replaced crewai. Services import it
freely; it imports nothing from `src`.

Two consequences people get wrong:
- **Never stub `kasal_engine.*` into `sys.modules` in a test.** It is real code
  here. Stubbing it shadows working modules, and anything imported inside that
  window stays cached holding MagicMocks — which breaks unrelated suites sharing
  the xdist worker. (This cost a day; see the path-move commit.)
- A tool subclassing `BaseTool` or a memory backend implementing
  `StorageBackend` is a LIBRARY dependency and belongs in services, not here.

## Related
- `src/docs/crewai-engine-refactor-proposal.md` — the earlier refactor record
