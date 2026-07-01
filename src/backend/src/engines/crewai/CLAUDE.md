# CrewAI Engine CLAUDE.md

Instructions for `src/backend/src/engines/crewai/`. **This layout is post-refactor
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

`crewai_engine_service.py` is the **hub**: it resolves the path and delegates. It
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
            → CrewAIExecutionService → CrewAIEngineService.run_light_agent_execution
            → LightAgentService.run_light_agent_execution (in-process)
```

## Directory map (current)

```
engines/crewai/
├── crewai_engine_service.py   # hub: dispatch crew/flow/light + status/cancel
├── config_adapter.py          # config-shape normalization
├── paths/                     # one package per execution path
│   ├── light_agent/           # the chat/light single-agent path (in-process)
│   ├── crew/                  # crew_preparation, execution_runner, agent/task adapters
│   └── flow/                  # flow_runner_service, backend_flow, modules/
├── kernel/                    # path-AGNOSTIC single-source build logic (was common/)
│                              #   agent_builder, agent_tools, task_builder,
│                              #   agent_security, model_conversion_handler, a2ui_runner...
├── memory/                    # unified memory backends + crew_memory_service
├── infra/                     # logging_config, trace_management, mlflow_integration, crew_logger
├── guardrails/                # root=framework; core/=reusable; demo/=data_processing family
├── config/                    # crew/embedder/manager config builders
├── callbacks/                 # live callbacks only (streaming, execution, volume)
├── security/                  # scanner pipeline, injection/secret detectors, capability manifest
├── tools/                     # tool_factory + custom/ (see tools/CLAUDE.md)
└── exporters/                 # Databricks App / notebook / python-project export + templates
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
- **Guardrails**: build via `GuardrailFactory.create_guardrail`. Add reusable ones
  under `guardrails/core/`, demo/one-off ones under `guardrails/demo/`. Keep the
  `guardrails/__init__.py` barrel and `guardrail_factory` type registry in sync.
- **Memory**: use the unified `memory_backend_factory` + `CrewMemoryService`. See
  `src/backend/CLAUDE.md` for the crew-ID determinism and Vector Search schema
  rules (do not hardcode index columns; never call `.value` on enum-valued
  Pydantic fields).
- **Naming caveats to know**: `infra/trace_management.py` is the execution-LOGS
  writer (trace persistence moved to OTel/MLflow); `infra/crew_logger.py`
  (`CrewLogger`) is **live** despite an old audit note — it is used by the engine
  hub and several services.
- All engine work is async; Databricks calls need User-Agent telemetry
  (`src/backend/CLAUDE.md`).

## Related
- `tools/CLAUDE.md` — custom tools and the tool factory
- `src/docs/crewai-engine-refactor-proposal.md` — the full refactor record (§7 = final state)
