# Code structure

Use this map to find the owner of a change. Backend domain packages and the
frontend's existing component folders remain in place; frontend features are
being extracted incrementally.

## Repository and build ownership

| Path | Responsibility |
| --- | --- |
| `src/backend/` | Backend environment, dependency manifest, tests and migrations |
| `src/backend/src/` | FastAPI application; Python imports use the `src` package |
| `src/backend/scripts/maintenance/`, `scripts/diagnostics/` | Operator-run maintenance and diagnostic scripts; outside the shipped application |
| `src/frontend/` | React and TypeScript application built with Vite |
| `src/docs/` | Canonical in-app documentation, images and downloadable examples |
| `src/scripts/build-tasks.cjs` | Stage public assets/docs and publish Vite output |
| `src/package.json` | Build lifecycle shared by deployment and wheel preparation |
| `src/build.py` | Python entry point for that npm lifecycle |
| `src/deploy.py` | Build and stage a Databricks App deployment |
| `src/package_pip.py` | Prepare frontend assets and build the pip distribution |
| `pyproject.toml`, `hatch_build.py`, `packaging/kasal/` | Wheel packaging and CLI; dependencies come from the backend manifest |

Vite combines `frontend/public/` with `src/docs/` in the ignored
`frontend/.generated/public/` directory. It copies nested Markdown, images,
JSON examples and CSS, preserving `/docs/...` URLs. Edit `src/docs/`, then
restart Vite to refresh the staged documentation during development.

The root npm lifecycle publishes `frontend/dist/` as `src/frontend_static/`.
Deployment uploads that snapshot; the wheel includes it under
`kasal/_app/frontend_static`. Generated assets are not source files.

## Backend ownership

Paths in this section are relative to `src/backend/src/`.

| Path | Responsibility |
| --- | --- |
| `main.py`, `api/__init__.py` | Application lifecycle, middleware and router composition |
| `api/` | HTTP routing, request validation and response translation |
| `services/` | Domain operations and orchestration, grouped by domain |
| `repositories/` | Database and external storage access |
| `models/`, `schemas/` | SQLAlchemy entities and Pydantic contracts |
| `db/session.py`, `db/all_models.py` | Sessions and model registration |
| `db/self_heal/` | Deployed startup schema compatibility repairs |
| `config/` | Runtime settings and logging configuration |
| `core/` | Shared infrastructure, LLM transport, events and runtime paths |
| `utils/` | Cross-domain helpers and documented lazy authentication lookups |
| `seeds/`, `seeds/skills_data/` | Shipped seed and skill content |

Use the existing domain package for a service. Tool adapters under
`services/tools/` are intentionally flat; substantial reusable logic belongs
with its domain rather than being copied into each adapter.

Selected execution and integration entry points:

- `services/execution/service.py`: execution lifecycle facade.
- `services/execution/engine_service.py`: engine dispatch.
- `services/execution/finalization.py`: terminal outcomes and persistence retries.
- `services/execution/runtime/`: Kasal agent runtime.
- `services/execution/harnesses/`: runtime bindings, including CrewAI.
- `services/execution/kernel/`: shared agent/task construction and tooling.
- `services/agent_builder/`: crew preparation, execution and subprocess entry point.
- `services/flow_builder/`: flow assembly, execution, checkpoints and subprocess entry point.
- `services/execution/history.py`, `services/execution/logs/`: history and execution logs.
- `services/trace/`: trace ingestion, persistence and broadcasting.
- `services/llm/manager.py`: provider/model selection and streaming.
- `core/llm/robust_json.py`: tolerant JSON recovery for generated responses.
- `services/catalog/templates.py`: database-backed prompt-template lookup.
- `services/powerbi/pipeline_config.py`: shared Power BI extraction and configuration derivation.
  `services/tools/generate_config.py` retains the standalone CLI and compatibility imports.
- `services/scheduling/scheduler.py`: scheduled execution.
- `services/knowledge/documentation_embedding.py`: documentation embeddings.
- `services/databricks/`, `services/mlflow/`: integration services.

## Boundaries and transactions

Follow routers → services → repositories → models. Keep request assembly at
the API boundary, orchestration in services and I/O in repositories. Existing
HTTP-coupled services still need gradual cleanup; translate domain errors in
routers when establishing new reusable service boundaries.

Services own transaction scope and use the injected session inside a request.
Background work uses `routed_scoped_session()`. Repositories neither acquire
sessions nor commit them. Operations spanning multiple writes, such as memory
configuration replacement, must retain one transaction.

`src/backend/pyproject.toml` contains the import-linter contracts and documented
lazy authentication exceptions. Preserve the service-level tenant checks in
those paths. A folder move must not bypass authorization.

Terminal execution state belongs to the existing finalization module. Keep
its failure/cancel/reject outcomes and retry behavior when splitting executors;
do not introduce another pending-outcome registry.

## Schema changes and shipped resources

Alembic configuration and revision history live in `src/backend/alembic.ini`
and `src/backend/migrations/`. They serve offline migration workflows.
Databricks deployment excludes that directory; startup repairs under
`db/self_heal/` have a separate role. Preserve applied revision identifiers.

Runtime/tool changes can affect export allowlists and generated imports under
`services/export/`. Update those consumers alongside source moves. The A2UI
frontend copy in the exported app template is intentional: deployed apps do
not contain the main frontend source. Its synchronization tests protect that
copy.

Keep backend package roots stable. The wheel embeds the application under
`kasal/_app/src`, and its CLI sets up the import path before loading `src.main`.

## Frontend ownership

Paths in this section are relative to `src/frontend/src/`.

| Path | Responsibility |
| --- | --- |
| `components/` | Existing views, forms and UI composition |
| `components/ChatMode/` | Chat workspace, state, persistence, hooks and views |
| `features/executions/trace/lib/` | UI-independent trace processing, indexing and batching |
| `features/executions/trace/hooks/` | Trace fetching and React view state |
| `features/executions/trace/components/` | Trace event icons |
| `features/workflow/canvas/lib/` | Canvas layout calculations and graph indexing |
| `shared/lib/collections.ts` | Generic first-occurrence indexing and deduplication |
| `types/ui/layout.ts` | Application-wide workspace panel/layout state contract |
| `api/` | API clients grouped by domain |
| `types/` | Component-independent contracts grouped by domain |
| `store/`, `hooks/` | Existing shared state and hooks |
| `shared/` | Shared libraries and A2UI rendering |
| `utils/`, `theme/` | Existing utilities and app theme |

Preview parsing belongs to `components/ChatMode/utils/preview.ts`, with its
contracts in `components/ChatMode/types/preview.ts`. Skill selection lookup is
independent of state; reconciliation actions live under the chat store folder.
API clients, contracts and pure processing libraries must not import UI views.
The flat ESLint configuration enforces the boundaries already established.

`config/api/ApiConfig.ts` reads `VITE_API_URL`, defaulting to
`http://localhost:8000/api/v1` in development and `/api/v1` in production.
State storage keys, event names, endpoint URLs and CSS scopes are stable
contracts even when their source files move.

## Tests and checks

Frontend unit/component tests live beside their source. Run Vitest from
`src/frontend/`; `vitest.config.ts` owns coverage thresholds, including exact
paths that must move with covered files. `eslint.config.js` is the ESLint
configuration. `npm run build` checks TypeScript and produces the Vite bundle.

Backend tests live under `src/backend/tests/`, with unit suites following
source ownership. `src/backend/pyproject.toml` owns pytest discovery, asyncio
mode and markers. Run `.venv/bin/python -m pytest` from `src/backend/`, adding
the relevant paths and explicit coverage options when needed. There is no
second configuration under `tests/`. Fixtures and artifact isolation live in
`tests/conftest.py`; architecture tests guard layering and test placement.

When moving code, update imports, mocks, coverage paths, export mappings and
resource locators together. Preserve lazy entry points and subprocess imports.
Test the affected behavior, and check built artifacts when their inputs move.

## Related guides

- [Architecture guide](./ARCHITECTURE_GUIDE.md)
- [Developer guide](./DEVELOPER_GUIDE.md)
- [API endpoints](./api_endpoints.md)
- [Execution harnesses](./harnesses.md)
- [Pip packaging](./PIP_PACKAGE.md)
- [Documentation hub](./README.md)
