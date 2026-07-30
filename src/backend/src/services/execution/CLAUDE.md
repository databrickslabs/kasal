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
    ├── checkpointing/       # crash-resume, SHARED by crew and flow:
    │                        #   record (storage contract, versioned + migrate
    │                        #   on read), store, recorder, lifecycle, resume,
    │                        #   service. Path adapters live with their path:
    │                        #   {agent,flow}_builder/checkpoint_adapter.py
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
- **Checkpointing**: one contract for both subprocess paths — a unit is a TASK
  for a crew and a CREW for a flow, and that is the only difference. Written by
  an event-bus recorder, never derived from traces: telemetry can be sampled or
  retention-pruned for unrelated reasons, and a resume that silently degrades is
  worse than one that reports "no checkpoint". Resume creates a NEW execution
  linked by `resumed_from_execution_id`; the failed run stays failed. The Chat
  path has none and must not get one. See `src/docs/CHECKPOINTING.md`.
- **Guardrails**: build via `GuardrailFactory.create_guardrail` from
  `src.services.guardrails`.
- **Memory**: `src.services.memory` — `MemoryBackendFactory` + `CrewMemoryService`,
  with `memory/hooks.py` for recall/persist around a task. See
  `src/backend/CLAUDE.md` for crew-ID determinism and Vector Search schema rules.
- All work here is async; Databricks calls need User-Agent telemetry
  (`src/backend/CLAUDE.md`).

## The agent runtime lives here now

There is no `kasal_engine` package. What was a 6,594-line library sitting beside
`src/` is first-party code in the tree that uses it:

| was | is |
|---|---|
| `kasal_engine/core/` | `services/execution/runtime/` — Agent, Task, Crew, the tool-call loop |
| `kasal_engine/events/` | `src/core/events/` — the run event bus |
| `kasal_engine/llm/` | `src/core/llm/transport/` — under the config layer that drives it |
| `kasal_engine/tools/base.py` | `services/tools/base.py` — beside its 38 subclasses |
| `kasal_engine/memory/` | `services/memory/engine/` |
| `kasal_engine/flow/` | `services/flow_builder/runtime/` |

The event bus is in `src/core/`, NOT `services/execution/` — this table said
`services/execution/events/` for a while, a directory that has never existed.
The bus spent a few hours under services during the flattening and that inverted
the layering: `core/llm/transport` emits events, so a service-layer bus made
`core` import `services` at module level. See `src/core/events/__init__.py`.

**The direction of dependency is now a convention, not a fact.** It used to be
structural: that package could not import `src` because it shipped separately.
It can now. It must not. `runtime/` and `core/events/` are called BY the app — the
moment one of them reaches for a repository, a session or a `GroupContext`, the
agent loop stops being runnable from anywhere that is not a full Kasal process,
and every import graph in this directory develops a cycle.

Two more things that outlived the package:
- **`crewai_event_bus` is now `event_bus`** (and `CrewAIEventsBus` is
  `EventsBus`). No alias was left behind.
- **Never stub these modules into `sys.modules` in a test.** That was a habit
  from when `crewai` was an absent third-party dependency; it now shadows working
  first-party code, and anything imported inside the stub window stays cached
  holding MagicMocks, which breaks unrelated suites on the same xdist worker.

## Related
- `src/docs/crewai-engine-refactor-proposal.md` — the earlier refactor record
