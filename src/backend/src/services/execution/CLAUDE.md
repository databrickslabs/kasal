# Execution CLAUDE.md

Instructions for `src/backend/src/services/execution/` and the three path
packages beside it. **`src/engines/` no longer exists** — any doc, comment or
memory that mentions `src.engines.kasal.*` is stale.

## The three paths (this is the core mental model)

There are **three distinct answer paths**. Opening a file, first ask "which path
am I in?" The `execution_type` string selects it (`services/execution/service.py`):

| `execution_type` | UI name | Package | Where it runs |
|---|---|---|---|
| `"agent"` | **Chat** (ChatMode) | `services/chat/` | **in-process**, `Agent.kickoff_async`, sub-second |
| `"crew"` | **Agent Builder** | `services/agent_builder/` | **subprocess** (`agent_builder/process_executor.py`) |
| `"flow"` | **Flow Builder** | `services/flow_builder/` | **subprocess** (`flow_builder/process_executor.py`) |

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
    ├── base.py              # the ENGINE-SERVICE interface (the hub, not a harness)
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
  worse than one that reports "no checkpoint". The trace reconstruction the flow
  path used to fall back on has been DELETED — do not reintroduce it. Resume creates a NEW execution
  linked by `resumed_from_execution_id`; the failed run stays failed. The Chat
  path has none and must not get one. See `src/docs/CHECKPOINTING.md`.
- **Guardrails**: build via `GuardrailFactory.create_guardrail` from
  `src.services.guardrails`.
- **Memory**: `src.services.memory`, organised by lifecycle stage —
  `config/` (what the teamspace configured), `storage/` (the backends and
  `MemoryBackendFactory`), `engine/` (the `Memory` object), `run/`
  (`CrewMemoryService` builds a run's `Memory`; `recall.py` before a task,
  `persist.py` after it), `maintenance/` (between runs). The package
  `__init__` is the map; `src/docs/MEMORY.md` is the full account.
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
- **Kasal's bus is `event_bus`** (and `CrewAIEventsBus` is `EventsBus`). No
  alias was left behind — and now that `crewai` is a real dependency again,
  `crewai_event_bus` means CrewAI's OWN bus, a different object. See the harness
  layer below: exactly one bridge connects them, in one direction.
- **Never stub `crewai` into `sys.modules` in a test.** This used to be about
  shadowing first-party code while crewai was absent. It is now worse: crewai IS
  installed, so a stub shadows a working library, and anything imported inside
  the stub window stays cached holding MagicMocks — breaking unrelated suites on
  the same xdist worker.

## Two engines live here

`crewai` is a dependency again, as a SECOND runtime beside
`services/execution/runtime/`. Which one executes a run is an operator setting.
This did not add a second service — it added a layer under the one that exists.

```
KasalEngineService              the hub, unchanged: dispatch, status, cancel
  chat/ agent_builder/ flow_builder/   the three paths, unchanged
    execution/kernel/            build logic, now harness-parameterised
      execution/harnesses/         <- the harness layer
        kasal/                       services/execution/runtime/
        crewai/                      the crewai package
```

**Rules for this layer:**

- **Never construct a runtime class directly.** No
  `from ...runtime import Agent` followed by `Agent(**kwargs)`. Build through
  `active_harness().build_agent(**kwargs)`. The kwargs dict is harness-neutral;
  each binding owns the translation onto its own runtime.
- **`harnesses/` never reads the database and never imports a path package.**
  The setting is resolved once per execution by `harness_choice.py`, which takes
  a SESSION rather than opening one; bindings receive a decided value.
- **A run's harness is decided once and recorded** on
  `execution_history.harness`, then carried into a subprocess by payload
  and environment. Never re-read the setting mid-run: a switch landing between
  agent-build and task-build would produce a run that is half each. A run may
  NAME its harness on `CrewConfig.harness` / `FlowConfig.harness`; the
  Configuration value is the default for runs that do not, which today is EVERY
  run started from the UI. The per-run pickers were removed deliberately —
  choosing a runtime is an operator decision, not something to put in front of
  someone writing a chat message — so the field is reachable from the API only.
- **One bus still writes traces.** The CrewAI binding bridges
  `crewai_event_bus` onto `event_bus` (`engines/crewai/events.py`) and nothing
  downstream changes. Do NOT bridge events a Kasal subsystem already emits —
  LLM calls, tools, memory and guardrails all reach the bus under both harnesses,
  and bridging them again doubles every trace row.
- **Both engines call models through Kasal's transport.** The CrewAI binding's
  LLM is a `crewai.BaseLLM` subclass that forwards to
  `src.core.llm.transport` (`engines/crewai/llm.py`). Keep it that way: it is
  what makes Databricks auth, retry, the context clamp and token accounting
  identical, and therefore what makes a cross-engine comparison mean anything.
- **What a harness cannot do is DECLARED, not discovered.** `Capability` in
  `engines/binding.py` drives the API and greys out the UI. Today CrewAI claims
  everything except `AGENT_PLAN` — the `todo` tool is written for Kasal's
  executor, one call per round over a conversation that executor owns. A declared
  capability must be one the binding actually delivers; the parity suite checks
  that, and it has already caught an over-claim.
- **An export ships one runtime, chosen at export time.** Both harnesses claim
  `EXPORT` and produce a DIFFERENT bundle: the Kasal one vendors
  `services/execution/runtime/`, the CrewAI one ships `crewai` pinned, on top of
  the same vendored transport. `export/templates/databricks_app/agent_server/runtime_binding.py`
  is the whole seam — `agent.py` is identical either way. Keep it that way; a
  second `if RUNTIME ==` anywhere in the template is the start of two apps.
- **The two harnesses divide tool execution differently — say which you mean.**
  Kasal's transport owns the tool loop (give it `available_functions`, get final
  text). CrewAI's executor owns it instead, and asks the LLM only for the
  decision. `transport.delegate_tool_calls` selects the second; the CrewAI
  binding sets it. Without it a tool-call response reaches CrewAI as `""`.
- **CrewAI probes an LLM for capabilities with `hasattr`.** An omitted
  `supports_function_calling` is read as "cannot", and the agent silently drops
  to a ReAct prose loop — no error, just tool calls that stop parsing. Anything
  added to `engines/crewai/llm.py` should ask the transport rather than assert a
  convenient default.
- **A tool is invoked through `runtime/executor.wrap_tool` on BOTH engines.**
  That function is where the approval gate, replay, the outcome ledger and all
  three `ToolUsage*` events live. Never call `tool.run()` directly from an
  adapter — a HITL gate that applies on one harness is not a gate, it is a
  setting that silently stops applying when someone changes a dropdown.
- **Task identity is engine-dependent, so a checkpoint cannot cross engines.**
  CrewAI's `Task` inherits its agent's tools and Kasal's does not. This is
  handled by stickiness, not by normalising the hash: a run records its engine
  and its resume reuses it, and a row with no recorded engine means Kasal.
- **The Flow Builder's orchestrator is Kasal's under both harnesses.** The harness
  setting selects the agent runtime; a flow's crews are built through the
  binding, so they run on the selected engine while routing, HITL gates and
  per-crew checkpoints stay in `flow_builder/runtime/`.

## Related
- `src/docs/crewai-engine-refactor-proposal.md` — the earlier refactor record
