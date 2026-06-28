# CrewAI engine refactor proposal

Record of the `src/engines/crewai` restructure into path, kernel, and infra packages, including the dead-code audit, target tree, and migration log.

- [Current state](#1-current-state)
- [Problems](#2-problems)
- [Confirmed dead code to delete](#3-confirmed-dead-code-to-delete)
- [Proposed target structure](#4-proposed-target-structure)
- [Migration plan](#5-migration-plan)
- [Open questions and risks](#6-open-questions--risks)
- [Execution log and verification corrections](#7-execution-log--verification-corrections-added-during-implementation)

**Status: implemented.** This document is the active record of the `src/engines/crewai` refactor; the re-tree it proposes has since been carried out (see §7 — Phase 0/1/2 DONE). Sections 1–2 describe the *pre-refactor* layout for context; the new layout is `paths/{light_agent,crew,flow}/`, `kernel/` (was `common/`), `infra/`, `memory/`, `guardrails/{core,demo}/`, with `helpers/`, `utils/`, `services/`, `mcp/` removed. All paths below are relative to `src/backend/src/engines/crewai/` unless otherwise noted.

---

## 1. Current state

The engine has **three execution paths** (light-agent, crew, flow) plus shared build machinery, but the folder layout does not reflect that split. Crew-specific, flow-specific, and path-agnostic code are interleaved at the top level and inside `helpers/`, `common/`, and `utils/`.

Legend: `[light]` `[crew]` `[flow]` `[shared]` `[export]` `[tools]` `[infra]` `[DEAD]`

```text
engines/crewai/
├── crewai_engine_service.py        [shared]  hub: dispatches crew / flow / light + status/cancel
├── crewai_flow_service.py          [flow]    adapter for flow_execution_router
├── light_agent_service.py          [light]   in-process single-agent chat path
├── execution_runner.py             [crew]    run_crew_in_process (live) + run_crew (DEAD) + run_light_agent (back-compat)
├── crew_preparation.py             [crew]    CrewPreparation (runs ONLY in subprocess)
├── config_adapter.py               [shared]  adapt/normalize config + get_execution_logger (logger picker — misfit)
├── trace_management.py             [infra]   execution-LOGS writer task (misnamed: no longer "trace")
├── mlflow_integration.py           [infra]   crewai/litellm autolog glue
├── crew_logger.py                  [infra]   CrewLogger singleton — per-job machinery only used by DEAD run_crew
├── logging_config.py               [infra]   subprocess-side DB log handler (LIVE path)
├── memory_config.py                [infra]   [DEAD] legacy file-based memory, kept alive only by its test
├── __init__.py                     [shared]
│
├── common/                         [shared]  path-agnostic single-source build logic
│   ├── agent_builder.py            [shared]  build_agent (LLM→kwargs→security→Agent)
│   ├── agent_tools.py              [shared]  build_agent_with_tools (used by ALL 3 paths)
│   ├── task_builder.py             [shared]  build_task_args (crew + flow)
│   ├── agent_security.py           [shared]  injection-hardening preamble
│   ├── genie_formatting.py         [shared]
│   └── trace_context.py            [shared]
│
├── helpers/                        [crew + shared + export — MIXED]
│   ├── agent_adapter.py            [crew]    create_agent (crew entry over common)
│   ├── task_adapter.py             [crew]    create_task (crew entry over common)
│   ├── conversion_helpers.py       [crew]    extract_crew_yaml_data (config_adapter only)
│   ├── task_callbacks.py           [crew]    [DEAD] configure_task_callbacks
│   ├── model_conversion_handler.py [shared]  used by common.task_builder (both paths)
│   ├── tool_helpers.py             [shared]  resolve_tool_ids_to_names LIVE; prepare_tools/get_tools_for_agent/get_tool_instances DEAD
│   ├── a2ui_runner.py              [shared]  A2UI compose/wrap (light + crew) — really export/shared, not a crew helper
│   └── ui_document.py              [export]  legacy A2UI doc normalization — misplaced here
│
├── utils/                          [DEAD]    agent_utils.py + __init__.py (orphaned event-name extractor)
│
├── services/                       [shared]  crew_memory_service.py (931 LOC, god-ish; crew+flow+light)
│
├── flow/                           [flow]
│   ├── flow_runner_service.py      [flow]
│   ├── backend_flow.py             [flow]    _configure_agent_and_tools/_configure_task/_ensure_event_listeners_registered DEAD
│   ├── flow_execution_runner.py    [flow]    flow counterpart of execution_runner
│   ├── exceptions.py               [flow]
│   └── modules/
│       ├── agent_adapter.py        [flow]    AgentConfig.configure_agent_and_tools (flow entry over common)
│       ├── task_adapter.py         [flow]    TaskConfig.configure_task; _resolve_tool_override DEAD-dup
│       ├── callback_manager.py     [flow]    flow-only callback orchestration
│       ├── flow_builder.py         [flow]
│       ├── flow_processors.py      [flow]
│       ├── flow_methods.py         [flow]    largest flow module
│       ├── flow_config.py          [flow]
│       └── flow_state.py           [flow]
│
├── callbacks/                      [shared — HALF DEAD]
│   ├── streaming_callbacks.py      [shared]  LIVE (JobOutputCallback/EventStreamingCallback)
│   ├── execution_callback.py       [shared]  LIVE
│   ├── logging_callbacks.py        [shared]  no-op shells (kept for caller compat)
│   ├── base.py                     [shared]  alive only via DatabricksVolumeCallback
│   ├── databricks_volume_callback.py [tools] LIVE (task_adapter)
│   ├── transformation_callbacks.py [DEAD]
│   ├── validation_callbacks.py     [DEAD]    overlaps guardrails/
│   ├── storage_callbacks.py        [DEAD]
│   └── handlers/__init__.py        [DEAD]    empty legacy package
│
├── guardrails/                     [tools — reusable + demo MIXED]
│   ├── guardrail_factory.py        the real registry
│   ├── guardrail_wrapper.py / base_guardrail.py / guardrail_model.py
│   ├── minimum_number / company_count / llm_injection / self_reflection   reusable
│   └── data_processing / data_processing_count / empty_data_processing / company_name_not_null  [demo/one-off, data_processing table]
│
├── config/                         [shared]  crew_config_builder / embedder_config_builder / manager_config_builder
│                                             (parallel to top-level config_adapter.py — name collision)
│
├── memory/                         [crew — TWO GENERATIONS]
│   ├── memory_backend_factory.py   LIVE (unified 1.10 StorageBackend)
│   ├── databricks_storage_backend.py / lakebase_storage_backend.py  LIVE
│   ├── databricks_vector_storage.py  legacy, repurposed by knowledge_search/embedding (misplaced)
│   ├── lakebase_pgvector_storage.py  [DEAD]
│   └── chromadb_databricks_storage.py [DEAD]
│
├── security/                       [shared]  cleanly layered, fully wired
│   ├── scanner_pipeline.py / prompt_injection_detector.py / secret_leak_detector.py
│   └── tool_capability_manifest.py
│
├── tools/                          [tools]
│   ├── tool_factory.py (1994)  mcp_handler.py  mcp_integration.py
│   ├── async_bridge.py  tool_session_provider.py
│   ├── native/__init__.py       [DEAD] empty
│   ├── schemas/__init__.py      GoogleSlidesToolOutput [DEAD]
│   └── custom/  ~32 tools; ~22 + 3 subpackages are the PowerBI→UCMV→Genie pipeline (~11k LOC, flat)
│
├── mcp/                            [DEAD]    empty __init__.py + stale README describing a removed design
│
└── exporters/                      [export]
    ├── base_exporter.py  (_get_tool_imports/_get_llm_model/_format_docstring DEAD)
    ├── yaml_generator.py        shared by all 3 exporters
    ├── code_generator.py        used ONLY by notebook + python_project
    ├── databricks_app_exporter.py   PRIMARY (only UI-exposed path)
    ├── databricks_notebook_exporter.py  UI toggle commented out
    ├── python_project_exporter.py       no UI toggle
    └── templates/               shipped payload (not engine runtime)
```

**The blur, explicit:** A reader cannot tell light vs crew vs flow from the layout. `crew_preparation.py` (crew) and `light_agent_service.py` (light) sit next to each other with no path folders; `helpers/agent_adapter.py` (crew) and `flow/modules/agent_adapter.py` (flow) share a basename and only the parent dir disambiguates; `execution_runner.py` is crew but also hosts a back-compat `run_light_agent`; the shared build kernel (`common/`) is buried beside crew-only `helpers/`.

---

## 2. Problems

### (a) Execution-path entanglement
- No `light/`, `crew/`, `flow/` separation. Crew code (`crew_preparation.py`, `execution_runner.py`), light code (`light_agent_service.py`), and the shared hub (`crewai_engine_service.py`) are all flat at the top level.
- `execution_runner.py` is crew-path but carries `run_light_agent` (back-compat delegator to `LightAgentService`) — cross-path leakage.
- `helpers/` claims to be generic but is mostly crew-path entries (`agent_adapter.create_agent`, `task_adapter.create_task`, `conversion_helpers`) with two export/shared modules (`a2ui_runner`, `ui_document`) mixed in.
- Business logic leaks into thin layers: `crewai_engine_service.run_execution` holds a long inline SAFETY-NET status-fixer (lines ~319-353) and heavy knowledge_sources debug logging; `crewai_flow_service.run_flow` parses flow nodes to generate a run_name (lines ~98-144).

### (b) Duplication — specific pairs
- **Memory wiring (light vs crew):** `light_agent_service._attach_memory` (lines 562-668) reimplements the exact 7-step sequence in `crew_preparation._create_crew` steps 4-8 (CrewMemoryService + CrewConfigBuilder + EmbedderConfigBuilder + fetch_memory_backend_config + generate_crew_id + setup_storage_directory + create_unified_storage + resolve_memory_llm_override + configure_crew_memory_components). Two copies that will drift.
- **A2UI surface composition (3 sites):** `execution_runner.run_crew` (dead) and `run_crew_in_process` both inline the same `wrap_result_with_surface(...)` try/except (~lines 391 and 698); `LightAgentService` uses `compose_surface` with a timeout wrapper. Crew uses `wrap_result_with_surface`, light uses `compose_surface` — two entries, no single helper.
- **Agent/task adapters (crew vs flow):** `helpers/agent_adapter.create_agent` ↔ `flow/modules/agent_adapter.AgentConfig.configure_agent_and_tools`; `helpers/task_adapter.create_task` ↔ `flow/modules/task_adapter.TaskConfig.configure_task`. Same basenames, different shapes (module-functions vs static-method classes). Both correctly delegate to `common/`, but each still contains its **own tool-resolution loop** (crew: tool_service id→name + factory; flow: assign-onto-agent), which also overlaps `common/agent_tools.resolve_agent_tools` (agent path only). Task tool resolution was never unified → duplicated 2–3 ways.
- **`_resolve_tool_override`:** byte-near-identical copy in `flow/modules/task_adapter.py` of the one in `common/agent_tools.py`.
- **Callback wiring (crew vs flow):** flow has a `CallbackManager` coordinator; crew wires the same callback classes inline (`crew_preparation` / `execution_callback`). No shared coordinator — structural asymmetry vs the unified agent/task build.
- **Secret-stripping:** `_SECRET_KEY_HINTS` in `exporters/databricks_app_exporter.py` == `_SECRET_CONFIG_HINTS` in `src/services/crew_export_service.py` (identical tuples).

### (c) Misplacement & unclear naming
- **`common/` vs `helpers/` vs `utils/`:** intent is actually clear post-refactor (common = shared kernel, helpers = crew entries, utils = one dead file) but the *names* signal nothing about path or role, so they blur. `utils/` contains only orphaned code.
- **`config/` vs top-level `config_adapter.py`:** unrelated concerns sharing the word "config". `config/` = per-concern builders (crew/embedder/manager); `config_adapter.py` = older CrewConfig normalization + a `get_execution_logger` logger-picker that belongs near `LoggerManager`, not in a config-shape module.
- **`mcp/` vs `tools/mcp_*`:** `mcp/` is an empty package + stale README for a removed design; all real MCP code is `tools/mcp_handler.py` + `tools/mcp_integration.py`. The empty dir next to its real replacement is actively misleading.
- **`trace_management.py`:** misnomer — only manages the execution-LOGS writer; trace persistence moved to OTel.
- **`crew_logger` overloaded:** both the `CrewLogger` singleton (`crew_logger.py`) and a common local alias for `LoggerManager.crew` in `services/`.
- **`services/` under the engine:** holds only `crew_memory_service.py` while every other service lives at `src/services/`.
- **`memory/`:** mixes unified 1.10 backends (live) with legacy per-type storages (`databricks_vector_storage` repurposed elsewhere; two dead).
- **`guardrails/`:** `__init__.py` exports only the 5 legacy/demo guardrails; the real registry `guardrail_factory.py` also handles `minimum_number`, `prompt_injection_check`, `self_reflection` → stale, incomplete barrel.
- **`tools/custom/` sprawl:** ~22 flat PowerBI/UCMV files + 3 subpackages (~11k LOC) for one vertical feature in the generic plugin dir.

### (d) Dead code
See Section 3 — all high-confidence, independently verified. Roughly **~3,500+ LOC** across modules, plus three whole vestigial packages (`mcp/`, `utils/`, `callbacks/handlers/`, `tools/native/`).

---

## 3. Confirmed dead code to delete

Each item is verified high-confidence. Most are kept alive only by a co-located test, so the test must be removed alongside.

| # | Path | Symbol | Evidence (short) | Test to remove |
|---|------|--------|------------------|----------------|
| 1 | `execution_runner.py` | `run_crew` (thread-based, ~480 LOC) | Imported at `crewai_engine_service.py:44`, never called; prod uses `run_crew_in_process` only | `tests/unit/models/test_execution_runner.py`, `tests/unit/engines/crewai/test_execution_runner_callbacks.py`, `test_execution_runner_new.py` |
| 2 | `crew_logger.py` | `CrewLogger.setup_for_job` / `capture_stdout_stderr` / `cleanup_for_job` / `_patch_printer` | Invoked only inside dead `run_crew`; subprocess logs via `logging_config`. Dead import at `crewai_engine_service.py:144` | tests of CrewLogger machinery |
| 3 | `memory_config.py` | whole module (`MemoryConfig`, `MEMORY_DIR`) | No prod importer in `src/`; live path is `memory_paths.py` + `crew_memory_service` | `tests/unit/engines/crewai/test_memory_config.py` |
| 4 | `crewai_engine_service.py` | `_execute_flow` | Defined `:633`, never called; flows go via `run_flow_in_process` | `test_crewai_engine_service.py:466/479/494/504` |
| 5 | `crew_preparation.py` | `_attach_knowledge_sources` | No-op, zero call sites | — |
| 6 | `crew_preparation.py` | `_apply_spotlighting_wrappers` | Body `pass`; real work in `run_crew_security_checks` | — |
| 7 | `crew_preparation.py` | `_initialize_agent_knowledge` + its call at line ~890 | No-op still called; logs 2 deprecation lines/build (remove method + call site) | — |
| 8 | `utils/agent_utils.py` + `utils/__init__.py` | whole `utils/` package | No prod import; live extractor is `mlflow_exporter._extract_agent_name` | `tests/unit/engines/crewai/test_agent_utils.py` |
| 9 | `helpers/task_callbacks.py` | `configure_task_callbacks` | Only barrel re-export, zero call sites | `tests/.../helpers/test_task_callbacks.py` |
| 10 | `helpers/tool_helpers.py` | `prepare_tools` / `get_tools_for_agent` / `get_tool_instances` | Self-contained cluster, no prod caller (`resolve_tool_ids_to_names` stays) | update `test_tool_helpers.py` |
| 11 | `flow/backend_flow.py` | `_configure_agent_and_tools` / `_configure_task` / `_ensure_event_listeners_registered` | Delegator stubs, never called; real sites call modules directly | `test_backend_flow.py:775-868`, `test_coverage_boost_backend_flow.py:849-882` |
| 12 | `flow/modules/task_adapter.py` | `_resolve_tool_override` | Near-identical dup of `common/agent_tools.resolve_tool_override` | replace with import |
| 13 | `helpers/model_conversion_handler.py` | `GeminiCompatConverter` / `DatabricksCompatConverter` + their two dispatch branches | Early return short-circuits them; inline comment confirms unreachable | `test_model_conversion_handler.py:412/680` |
| 14 | `memory/lakebase_pgvector_storage.py` | `LakebasePgVectorStorage` | Zero refs outside file; superseded by `lakebase_storage_backend.py` | its test |
| 15 | `memory/chromadb_databricks_storage.py` | `ChromaDBDatabricksStorage` | DEFAULT branch returns None; never instantiated (test already excluded in conftest) | `test_chromadb_databricks_storage.py` |
| 16 | `callbacks/transformation_callbacks.py` | `OutputFormatter`/`DataExtractor`/`OutputEnricher`/`OutputSummarizer` | Barrel-only, no prod caller | `test_transformation_callbacks.py` |
| 17 | `callbacks/validation_callbacks.py` | `SchemaValidator`/`ContentValidator`/`TypeValidator` | Barrel-only; output validation is via `guardrails/` | `test_validation_callbacks.py` |
| 18 | `callbacks/storage_callbacks.py` | `DatabaseStorage` | Barrel-only, no prod caller | `test_storage_callbacks.py` |
| 19 | `callbacks/handlers/` | empty package | Only a docstring; OTel bridge replaced it | empty test stub |
| 20 | `mcp/` | `__init__.py` + `README.md` | Empty pkg + stale README for removed design; real MCP in `tools/` | — |
| 21 | `tools/native/__init__.py` | empty package | 0 bytes, never populated, zero refs | — |
| 22 | `tools/schemas/__init__.py` | `GoogleSlidesToolOutput` | No `GoogleSlidesTool` exists | `tests/.../tools/schemas/test_init.py` |
| 23 | `tools/custom/powerbi_connector_tool.py` | `PowerBIConnectorTool` | Registered in factory + manifest but no DB seed title → unreachable. Also strip factory reg (`tool_factory.py:89-92,280-281,1734-1751`) + manifest (`78-79`) | `test_powerbi_connector_tool.py`, `test_tool_factory_extended2.py:253-255` |
| 24 | `src/seeds/tools.py` | `enabled_tool_ids` id 67 | No `(67,…)` tuple; `enabled_tool_ids` list itself is never read | — |
| 25 | `exporters/code_generator.py` | `generate_main_code` `for_notebook=True` branch (~309-420) | No prod call passes `for_notebook=True` | `test_code_generator.py:158/171` |
| 26 | `exporters/code_generator.py` | `_generate_class_based_crew_code` `for_notebook=True` branches (233-253, 271-272) | Sole caller passes default False; notebook routes to `_generate_notebook_crew_code` | — (keep helper `for_notebook` params; their True branches are tested) |
| 27 | `exporters/base_exporter.py` | `_get_tool_imports` / `_get_llm_model` / `_format_docstring` | Tested-but-unused; no concrete exporter calls them | `test_base_exporter.py:189-288` |

**Conditional (decision needed, not auto-delete):**
- `execution_runner.run_light_agent` — back-compat delegator; engine already calls `LightAgentService` directly. Remove if no external import/test references it.
- The 4 `data_processing`-table demo guardrails (`data_processing`, `data_processing_count`, `empty_data_processing`, `company_name_not_null`) — coupled to `DataProcessingRepository` used by nothing else; surfaced in `TaskAdvancedConfig.tsx`. Removing them is a **product/UX decision** (see §6). `data_processing_count_guardrail.py` additionally imports `unittest.mock.MagicMock` in runtime code (smell to fix regardless).
- `exporters/python_project_exporter.py` + `databricks_notebook_exporter.py` and the `code_generator.py` class-based/notebook generators — backend-live but UI-hidden. API-only legacy; retire only if the API path is confirmed unused (see §6).

---

## 4. Proposed target structure

Goal: opening the engine folder should answer "what is light vs crew vs flow?" at a glance. Three thin **path** packages over one **kernel**, with infra and export pulled out.

```text
engines/crewai/
├── engine_service.py              # was crewai_engine_service.py — the shared hub only
├── config_adapter.py              # config-SHAPE normalization only (get_execution_logger moved out)
├── __init__.py
│
├── paths/
│   ├── light_agent/
│   │   └── light_agent_service.py         # from light_agent_service.py
│   ├── crew/
│   │   ├── crew_service.py                # from crewai (crew dispatch slice) — optional thin facade
│   │   ├── crew_preparation.py            # from crew_preparation.py
│   │   ├── crew_runner.py                 # from execution_runner.py (run_crew_in_process + status retry; run_crew DELETED)
│   │   ├── agent_adapter.py               # from helpers/agent_adapter.py (create_agent)
│   │   ├── task_adapter.py                # from helpers/task_adapter.py (create_task)
│   │   └── yaml_helpers.py                # from helpers/conversion_helpers.py (extract_crew_yaml_data)
│   └── flow/
│       ├── flow_service.py                # from crewai_flow_service.py
│       ├── flow_runner_service.py         # from flow/flow_runner_service.py
│       ├── backend_flow.py                # from flow/backend_flow.py (delegator stubs DELETED)
│       ├── flow_runner.py                 # from flow/flow_execution_runner.py
│       ├── exceptions.py                  # from flow/exceptions.py
│       ├── agent_adapter.py               # from flow/modules/agent_adapter.py (AgentConfig)
│       ├── task_adapter.py                # from flow/modules/task_adapter.py (TaskConfig; _resolve_tool_override DELETED→import kernel)
│       ├── callback_manager.py            # from flow/modules/callback_manager.py
│       ├── flow_builder.py / flow_processors.py / flow_methods.py / flow_config.py / flow_state.py
│
├── kernel/                        # path-agnostic single-source build logic (was common/)
│   ├── agent/
│   │   ├── agent_builder.py               # from common/agent_builder.py
│   │   ├── agent_tools.py                 # from common/agent_tools.py (resolve_tool_override now the single copy)
│   │   └── agent_security.py              # from common/agent_security.py
│   ├── task/
│   │   ├── task_builder.py                # from common/task_builder.py
│   │   ├── genie_formatting.py            # from common/genie_formatting.py
│   │   └── model_conversion_handler.py    # from helpers/model_conversion_handler.py (dead converters DELETED)
│   ├── tools_resolution.py                # resolve_tool_ids_to_names from helpers/tool_helpers.py (dead trio DELETED)
│   ├── memory_wiring.py                   # NEW: attach_unified_memory(agents, config) — shared by crew + light (see §2b)
│   ├── surface.py                         # NEW: single A2UI entry; from helpers/a2ui_runner.py compose/wrap consolidated
│   └── trace_context.py                   # from common/trace_context.py
│
├── memory/                        # crew memory backends (unified gen only)
│   ├── memory_backend_factory.py
│   ├── databricks_storage_backend.py
│   ├── lakebase_storage_backend.py
│   └── crew_memory_service.py             # from services/crew_memory_service.py (services/ dir removed)
│   #   databricks_vector_storage.py → MOVE to tools/ or knowledge/ (used by knowledge_search/embedding, not memory)
│   #   lakebase_pgvector_storage.py, chromadb_databricks_storage.py → DELETED (dead)
│
├── config/                        # per-concern builders (unchanged content)
│   ├── crew_config_builder.py
│   ├── embedder_config_builder.py
│   └── manager_config_builder.py
│
├── callbacks/                     # live callbacks only
│   ├── streaming_callbacks.py
│   ├── execution_callback.py
│   ├── logging_callbacks.py
│   ├── base.py
│   └── databricks_volume_callback.py
│   #   transformation_/validation_/storage_callbacks.py + handlers/ → DELETED
│
├── guardrails/                    # split reusable vs demo
│   ├── core/                      # base, factory, wrapper, model, minimum_number, company_count, llm_injection, self_reflection
│   └── demo/                      # data_processing family (OR delete — see §6)
│
├── security/                      # unchanged (cleanly layered already)
│   ├── scanner_pipeline.py / prompt_injection_detector.py / secret_leak_detector.py
│   └── tool_capability_manifest.py
│
├── tools/
│   ├── tool_factory.py
│   ├── mcp_handler.py / mcp_integration.py   # the REAL mcp code stays here; top-level mcp/ DELETED
│   ├── async_bridge.py / tool_session_provider.py
│   └── custom/
│       └── powerbi_ucmv/          # NEW sub-package: the ~22 PowerBI→UCMV→Genie files + 3 util subpackages
│           ├── powerbi_analysis_tool.py … (the pipeline family)
│           ├── metric_view_utils/ / metric_view_validation_utils/ / metadata_reduction/
│       └── (genie_tool, perplexity_tool, agentbricks_tool, gmail_tool, databricks_jobs_tool, knowledge_search… stay flat)
│   #   native/ + schemas/ → DELETED (empty / dead)
│
├── infra/                         # cross-cutting plumbing, no path identity
│   ├── execution_logging.py       # from logging_config.py (subprocess DB handler — live)
│   ├── logs_writer.py             # from trace_management.py (renamed: it's the logs writer, not traces)
│   ├── mlflow_integration.py      # from mlflow_integration.py
│   └── execution_logger.py        # get_execution_logger moved out of config_adapter.py, next to LoggerManager usage
│   #   crew_logger.py → DELETED (singleton machinery dead); keep only if any live stdout capture remains
│
└── export/                        # was exporters/
    ├── base_exporter.py           # dead helpers removed
    ├── yaml_generator.py          # shared by all exporters
    ├── code_generator.py          # dead for_notebook branches removed
    ├── databricks_app_exporter.py # primary
    ├── databricks_notebook_exporter.py / python_project_exporter.py   # OR retire (see §6)
    ├── ui_document.py             # from helpers/ui_document.py (export concern — moved out of crew helpers)
    └── templates/
```

**Key resolutions:**
- `common/` → `kernel/` (with `agent/`, `task/` sub-namespaces) — name now states "shared single-source build logic," not "misc."
- `helpers/` is **dissolved**: crew entries → `paths/crew/`, shared sub-helpers → `kernel/`, export concerns (`a2ui_runner`→`kernel/surface.py`, `ui_document`→`export/`).
- `utils/` **deleted** (dead).
- `services/crew_memory_service.py` → `memory/` (the engine-local `services/` dir disappears).
- `config_adapter.py` keeps only shape-normalization; `get_execution_logger` → `infra/execution_logger.py`.
- `mcp/` deleted; MCP lives unambiguously in `tools/`.
- crew vs flow `agent_adapter.py`/`task_adapter.py` keep the same basename **but now sit under `paths/crew/` vs `paths/flow/`** — the path is in the directory, intentionally, and both import the single `kernel/` builder.

---

## 5. Migration plan

Ordered low→high risk. Steps tagged **[mechanical]** (import/move only, behavior-preserving) or **[behavioral]** (logic consolidation, needs tests).

**Phase 0 — Delete confirmed dead code [mechanical, low risk]**
Delete items 1–27 from §3 plus their co-located tests. No production import graph changes except removing now-unused imports (`crewai_engine_service.py:44` `run_crew`, `:144` `crew_logger`). Do this first to shrink the surface (~3,500+ LOC) before any moves.
- Sub-step 0a: pure orphans (no shim needed): `memory_config.py`, `utils/`, `mcp/`, `tools/native/`, `tools/schemas/`, `callbacks/handlers/`, dead callback toolkits, dead memory storages, `_execute_flow`, the crew_preparation no-ops, dead flow delegators, dead exporter branches/helpers.
- Sub-step 0b: `execution_runner.run_crew` + `CrewLogger` per-job machinery (verify no external test harness depends on them beyond the listed unit tests).
- Sub-step 0c: `powerbi_connector_tool` + its factory/manifest registration + seed `id 67`.

**Phase 1 — Consolidate duplicates [behavioral, medium risk]**
Each needs a regression test before/after.
1. Extract `kernel/memory_wiring.py::attach_unified_memory(agents, config)`; rewrite `light_agent_service._attach_memory` and `crew_preparation._create_crew` steps 4-8 to call it.
2. Extract `kernel/surface.py` with one A2UI entry; route `run_crew_in_process` and `LightAgentService` through it (preserve the crew `wrap_result_with_surface` vs light `compose_surface` distinction as two functions, one timeout policy).
3. Replace `flow/modules/task_adapter._resolve_tool_override` with import of `kernel/agent/agent_tools.resolve_tool_override`.
4. Unify task tool-resolution: factor the crew + flow tool loops into `kernel/tools_resolution.py` (this is the largest behavioral item — flow assigns onto the agent, crew maps id→name; reconcile carefully).
5. De-dup secret hints: single tuple shared by `databricks_app_exporter.py` and `src/services/crew_export_service.py`.

**Phase 2 — Move/rename into target tree [mechanical, but wide import churn]**
Pure relocations. Use Serena symbol moves + project-wide reference updates. Order:
1. `common/` → `kernel/` (+ `agent/`,`task/` split).
2. Dissolve `helpers/`: crew files → `paths/crew/`, shared → `kernel/`, export → `export/`.
3. Top-level path files → `paths/{light_agent,crew,flow}/`; `flow/modules/*` → `paths/flow/`.
4. `services/crew_memory_service.py` → `memory/`; relocate `databricks_vector_storage.py` out of `memory/`.
5. `logging_config.py`/`trace_management.py`/`mlflow_integration.py` → `infra/` (with renames).
6. `exporters/` → `export/`.
7. `tools/custom/` PowerBI family → `tools/custom/powerbi_ucmv/`.
8. `guardrails/` → `core/` + `demo/`.

**Backward-compat shims (needed because these are imported widely):**
- `crewai/__init__.py` currently exports `CrewAIEngineService, CrewAIFlowService, BackendFlow, FlowRunnerService` — keep these names re-exported from new locations.
- `flow/__init__.py` exports `BackendFlow, FlowRunnerService` — preserve.
- `flow/modules/__init__.py` exports `AgentConfig, TaskConfig, CallbackManager` — preserve (lazy in-function importers elsewhere depend on this).
- `helpers/__init__.py` barrel (`create_agent`, `create_task`, `is_data_missing`, `extract_crew_yaml_data`) — keep a thin shim re-exporting from new paths for one release, then remove.
- `common/agent_tools.build_agent_with_tools` is imported by all three paths + `process_crew_executor.py` (subprocess, line 872) — the subprocess importer is the **highest-risk** mover; keep a `common/` shim until `process_crew_executor` is updated and the subprocess re-verified end-to-end.
- `guardrail_factory.py` type-string registry must be updated in lockstep with any `guardrails/` package move; fix the stale `guardrails/__init__.py` barrel at the same time.

**Phase 3 — Cleanup [mechanical]**
Remove shims, delete now-empty `common/`, `helpers/`, `utils/`, `services/`, `mcp/` dirs. Update `mcp/README.md` content into a pointer (or drop). Re-run full backend test suite + one live crew, one light, one flow, one app-export smoke.

---

## 6. Open questions / risks

1. **Demo guardrails (data_processing family + `company_count`):** delete or keep? They are wired into the frontend `TaskAdvancedConfig.tsx` menu (6 legacy types) and coupled to a `data_processing` table nothing else uses. Deleting requires a frontend change. The newer LLM guardrails (`self_reflection`, `prompt_injection_check`) are NOT in the UI. Decision: prune demos + add the real ones to the UI, or keep demos under `guardrails/demo/`? (Independently, fix `MagicMock` import in `data_processing_count_guardrail.py` regardless.)
2. **Notebook + Python-project exporters / `code_generator.py`:** UI exposes only `databricks_app`. Notebook toggle is commented out; python_project has no toggle. Are the API-only paths still contractually supported? If yes, keep `code_generator.py` (1008 LOC serving only these); if no, retiring them removes a large maintenance surface and the bulk of `code_generator.py`.
3. **`execution_runner.run_light_agent` back-compat delegator:** safe to delete only if no external/integration test or downstream caller imports it. Needs a quick confirmation grep across non-engine code.
4. **`process_crew_executor.py` subprocess imports:** it instantiates `CrewPreparation` (line 872) and imports `kernel`/`common` build functions from inside a separate process. Any path move must be validated by an actual subprocess crew run, not just unit imports — module-path changes that pass in-process can still break the spawned interpreter's import resolution.
5. **`databricks_vector_storage.py` relocation target:** it's legacy memory storage repurposed by `knowledge_search`/`embedding` services. Move under `tools/` or a new `knowledge/`? Depends on where those consumers conceptually belong.
6. **`crew_logger.py` total removal vs partial:** §3 confirms the per-job machinery is dead, but verify no residual live stdout/Printer capture is wanted for the subprocess path before deleting the whole module (subprocess currently logs via `logging_config`, so likely yes — confirm).
7. **Scope/timing:** Phases 0–1 deliver most of the value (dead-code removal + duplication fixes) with low risk and no churn for downstream importers. Phase 2 (the big rename) is mechanical but touches many import sites and the subprocess boundary — confirm whether the user wants the full re-tree now or just Phases 0–1 first.

Relevant entry-point files for any reviewer: `src/backend/src/engines/crewai/crewai_engine_service.py` (hub), `src/backend/src/services/process_crew_executor.py` (subprocess crew build), `src/backend/src/engines/crewai/flow/flow_runner_service.py` (flow), `src/backend/src/engines/crewai/light_agent_service.py` (light), `src/frontend/.../ExportCrewDialog.tsx` and `TaskAdvancedConfig.tsx` (UI coupling for §6 decisions).

---

## 7. Execution log & verification corrections (added during implementation)

Branch: `refactor/crewai-engine-structure`. Decisions taken: **all phases**, **exporters kept** (API-supported), **guardrails → split `core/`/`demo/`, keep all + fix stale barrel + `MagicMock` smell**.

### Audit over-claims caught during per-item verification (the "25 confirmed dead" list is NOT safe to bulk-action)
- **`CrewLogger` / `crew_logger` is LIVE**, not dead. Used in `crewai_engine_service.initialize()` (~line 144) + `execution_service.py`, `crewai_execution_service.py`, `process_crew_executor.py`, `databricks_gpt_oss_handler.py`, `core/logger.py`. Item #2 was wrong — **do not remove**.
- **`crew_executor.run_crew` (service method) is LIVE** — called at `src/services/crew_executor.py:372`, heavily tested. Only the module-level `execution_runner.run_crew` was dead.
- **`memory_config.py` grep noise**: many `MemoryConfig`/`MEMORY_DIR` hits belong to `schemas/memory_backend.py` + `utils/memory_paths.py`, not the engine module. The engine `memory_config.py` was genuinely dead (only its own test imported it).
- Lesson: every remaining "dead" item must be re-verified against live + dynamic/string dispatch before deletion.

### Phase 0 — DONE & verified green (engine parses, 6267 tests collect 0 errors, targeted suites pass)
Removed: `mcp/`, `utils/`, dead callbacks (transformation/validation/storage + `handlers/`), 2 dead memory backends, `memory_config.py`, `helpers/task_callbacks.py`, `tools/native/`, `tools/schemas/`, dead funcs in `tool_helpers.py`; `execution_runner.run_crew` (~478 LOC), `_execute_flow`, crew_preparation no-ops, backend_flow delegator stubs, dead model converters, base_exporter dead helpers — all with barrels/conftest/co-located tests fixed.

**Deferred / needs decision:**
- **`powerbi_connector_tool`** — currently unreachable (no DB seed title; stale `COMPLETE_INTEGRATION_SUMMARY.md` claims id 74 but that's "M-Query Conversion Pipeline"). Reads like *intended-but-unwired*, not deprecated. Removal needs surgery on the 1994-LOC `tool_factory.py` + manifest + 3 tests. **Left for owner decision** (delete vs. wire up).
- `code_generator.py` `for_notebook` branches — left intact (exporters kept).

### Phase 1 — DONE (consolidation)
Only 2 of the 5 imagined duplications were real: **ACTIONED** — `_resolve_tool_override` deduped (`paths/flow/modules/task_adapter.py` now imports the single copy from `kernel/agent_tools.py`); shared `exporters/secret_hints.py` constant (both `databricks_app_exporter.py` and `crew_export_service.py` import it). **SKIPPED as non-duplication** — `memory_wiring` (the building blocks are already shared via `CrewMemoryService`/`*ConfigBuilder` service methods; only ~8 lines of path-specific glue differ — intentionally decoupled) and `surface` (already single-sourced once Phase 0 removed the dead inline copy). Unified tool-resolution skipped (crew id→name vs flow assign-onto-agent are genuinely different semantics).

### Phase 2 — DONE (the re-tree)
Executed via a deterministic migration script (`git mv`/`mv` + `perl` import-path rewrites across 327 referencing files). **New structure:**
```text
engines/crewai/
├── crewai_engine_service.py   config_adapter.py   __init__.py      # hub + root
├── paths/
│   ├── light_agent/light_agent_service.py
│   ├── crew/    crew_preparation.py  execution_runner.py  agent_adapter.py  task_adapter.py  conversion_helpers.py
│   └── flow/    crewai_flow_service.py  flow_runner_service.py  backend_flow.py  flow_execution_runner.py  exceptions.py  modules/
├── kernel/      (was common/) agent_builder  agent_tools  task_builder  agent_security  genie_formatting  trace_context  model_conversion_handler  tool_helpers  a2ui_runner
├── memory/      (+ crew_memory_service.py from the old engine-local services/)
├── infra/       logging_config  trace_management  mlflow_integration  crew_logger
├── guardrails/  base/factory/wrapper/model at root + core/ (minimum_number, self_reflection, llm_injection) + demo/ (data_processing family)
├── config/   callbacks/   security/   tools/   exporters/   (unchanged)
```
`helpers/`, `utils/`, `services/`, `mcp/` are gone. Leaf filenames kept (no `engine_service`/`crew_runner` renames) to minimize churn. **Guardrails:** `core`/`demo` split done, stale barrel rewritten to reflect it, `MagicMock`-in-runtime smell removed from `demo/data_processing_count_guardrail.py` (+ its obsolete hack-test rewritten to assert real behavior).

**Verification:** engine suite **6234 passed / 32 skipped / 1 failed**; the 1 failure (`config/test_embedder_config_builder…error_path`) is **pre-existing & unrelated** — its source+test are untouched by this branch and it can't even collect standalone (CrewAI-library import artifact). Broad collection across engine+services+api+models: **13482 tests, 0 errors**. Subprocess boundary (`process_crew_executor` + executors): **407 passed**. One test had a hardcoded old file path (`test_tool_config_override.py` isolated loader) — fixed.

**Deferred (optional, not blocking):** `tools/custom/powerbi_ucmv/` grouping (tangential to the light/crew/flow complaint, higher-risk against the 1994-LOC `tool_factory.py`); leaf renames; `exporters/`→`export/` (kept — already clear, template path-strings make it risky); `config_adapter.get_execution_logger`→`infra/`; `powerbi_connector_tool` delete-vs-wire decision (still parked from Phase 0).

---

## Related
- [Code structure guide](./CODE_STRUCTURE_GUIDE.md)
- [Architecture guide](./ARCHITECTURE_GUIDE.md)
- [Developer guide](./DEVELOPER_GUIDE.md)
- [Crew export and deployment guide](./crew-export-deployment.md)

Back to the [documentation hub](./README.md).