# CLAUDE.md

Project-wide instructions for Claude Code (claude.ai/code) when working with the Kasal codebase.

## Context Layering

Claude reads context from multiple CLAUDE.md files. The closest one to the file
you are editing wins on specifics:
- **This file**: Project-wide patterns and rules
- **src/backend/CLAUDE.md**: Backend-specific instructions
- **src/frontend/CLAUDE.md**: Frontend-specific instructions
- **Per-layer files** (added for precision):
  - `src/backend/src/api/CLAUDE.md` — FastAPI routers
  - `src/backend/src/services/CLAUDE.md` — business logic
  - `src/backend/src/repositories/CLAUDE.md` — data access
  - `src/backend/src/models/CLAUDE.md` — SQLAlchemy models + migrations
  - `src/backend/src/engines/kasal/CLAUDE.md` — the Kasal native engine (post-refactor)
  - `src/backend/src/engines/kasal/tools/CLAUDE.md` — custom tools
  - `src/frontend/src/api/CLAUDE.md` — frontend service layer
  - `src/frontend/src/components/CLAUDE.md` — React components
  - `src/frontend/src/shared/a2ui/CLAUDE.md` — the A2UI generative-UI system
    (renderer + composer + catalog + UIConfigurator + exported-app vendoring).
    **Read before adding/editing an A2UI component** — the touchpoints span
    frontend, backend and the export template.

## Important Project Rules

### Dependencies
- **Python dependencies are managed with `uv`** — declared in `src/backend/pyproject.toml` and pinned in `src/backend/uv.lock` (both committed). There is no `requirements.txt`.
- Install dependencies: `cd src/backend && uv sync`
- To change a dependency: edit `pyproject.toml`, run `uv lock` (regenerates `uv.lock`), then `uv sync`. Never hand-edit `uv.lock`.
- The Databricks App deploy (`src/deploy.py`) ships `pyproject.toml` + `uv.lock` at the bundle root so the build runs `uv sync` (a `requirements.txt` at the root would take precedence and bypass uv).
- Key dependencies include: psutil (for process management), litellm, databricks-sdk (the agent engine is the vendored `src/backend/kasal_engine` package)

### Documentation Location
- **ALWAYS create documentation in `src/docs/` directory**
- Do not create docs in the root `docs/` folder
- Frontend copies from `src/docs/` to `public/docs/` for display
- Follow existing documentation patterns and naming conventions

### Test Files Location
- **ALWAYS create test scripts and temporary files in `/tmp` folder**
- Do not create test files in the project directory
- Use paths like `/tmp/test_script.py` for testing
- This keeps the project directory clean

### Service Management
- **DO NOT restart backend or frontend services** - They are managed externally
- Backend uses `--reload` flag and auto-detects code changes
- Frontend uses hot module replacement (HMR) and auto-updates in browser
- Check service status: `ps aux | grep uvicorn` (backend) or `ps aux | grep "npm start"` (frontend)

### Code Quality Standards
- **CRITICAL: All operations must be async and non-blocking**
- **CRITICAL: Never include real URLs, endpoints, or addresses in code**
- Always use placeholder values like "https://example.com" or environment variables
- Follow clean architecture principles
- **Keep files small** — see File Size Limits below (≤400 lines target, 800 hard ceiling)
- Never commit without running linting tools

### File Size Limits (keep files small)

**Target ≤ 400 lines per source file. 800 is the hard ceiling.** ~75% of the
codebase is already under 400 lines; the outliers are the exception, not the
norm, and they are where bugs hide. A 2,000-line module cannot be read in one
sitting, cannot be reviewed properly, and blows out the context of every future
change to it.

Applies to `.py`, `.ts`, and `.tsx` under `src/backend/` and `src/frontend/src/`
(tests included — a 2,800-line test file is as unreviewable as the code it covers).

**The ratchet — this is the operative rule:**
- **Never create a new file over 400 lines.** If what you are writing will land
  above that, it is at least two modules; design it that way from the start.
- **A file already over the ceiling must not grow.** When adding to one, extract
  the new code (and the cohesive block it belongs to) into a sibling module
  instead of appending. "It was already 2,000 lines" is not a licence to make it
  2,100.
- **Touch it, shrink it.** Any non-trivial edit to a file over 800 lines should
  leave it smaller than you found it — pull out one coherent seam, not a token
  gesture.
- **Do not mass-refactor files you were not asked to touch.** Splitting modules
  the crew/flow **subprocess** imports is high-risk (see
  `src/backend/src/engines/kasal/CLAUDE.md`); a drive-by split that passes
  in-process tests can still break the spawned interpreter. Shrink what the task
  puts you in, and flag the rest.

**Split along real seams, never at an arbitrary line number.** A good split is
one another engineer can name:
- **Services** — split by strategy/concern into a package
  (`foo_service/` with one module per strategy), not `foo_service_part2.py`.
- **Custom tools** — the heavy PowerBI/metric-view tools already show the
  pattern: a thin tool class plus a `*_utils/` package of pure helpers
  (parsing, emitting, validating). Follow it.
- **React components** — extract sub-components, `hooks/`, and pure helpers;
  a component file should be JSX plus wiring, not business logic.
- **Keep the public import path stable.** Re-export from the package
  `__init__.py` / `index.ts` so call sites do not churn.

**Known offenders** (do not add to these; shrink when you are in them):
`powerbi_analysis_tool.py`, `process_crew_executor.py`,
`prompt_optimization_service.py`, `flow_methods.py`, `dispatcher_service.py`,
`execution_service.py`, `tool_factory.py`, `ChatWorkspace.tsx`,
`a2ui/components.tsx`, `WorkflowDesigner.tsx`, `TaskForm.tsx`.

Check before you commit: `wc -l <files you touched>`.

### Build and Deploy
- **Build frontend static assets**: `python src/build.py`
- **Deploy application**: `python src/deploy.py`

## Architecture Overview

Kasal is an AI agent workflow orchestration platform with a **clean architecture pattern**:

**Frontend (React + TypeScript)** → **API (FastAPI)** → **Services** → **Repositories** → **Database**

### Technology Stack
- **Backend**: FastAPI + SQLAlchemy 2.0 (async) + Alembic (Python 3.11, pinned `>=3.11,<3.12`)
- **Frontend**: React 18 + TypeScript + Material-UI + ReactFlow, built with **Vite**
- **AI Engine**: Kasal native engine (vendored `kasal_engine` package) for agent orchestration
- **Database**: SQLite (dev) / PostgreSQL / Databricks Lakebase (prod)
- **Authentication**: Databricks OAuth (OBO / SPN); JWT for app sessions

### Project Structure
```
src/
├── backend/                  # FastAPI backend (see backend/CLAUDE.md)
│   ├── src/                 # Core application code
│   ├── tests/               # Unit and integration tests
│   └── migrations/          # Database migrations
├── frontend/                # React frontend (see frontend/CLAUDE.md)
│   └── src/                 # React application
└── frontend_static/         # Built frontend assets
```

## Development Workflow

### Quick Start
1. **Backend**: `cd src/backend && ./run.sh` (auto-reloads on changes)
2. **Frontend**: `cd src/frontend && npm start` (hot module replacement)
3. **Tests**: See respective CLAUDE.md files for testing commands

### Key Principles
- **Clean Architecture**: Separation of concerns across layers
- **Async-First**: All I/O operations must be async
- **Type Safety**: Strong typing in both backend (mypy) and frontend (TypeScript)
- **Test Coverage**: Minimum 80% for backend, comprehensive frontend testing

## Special Considerations

### Memory and Persistence
- Crews generate deterministic IDs for memory persistence
- Group isolation ensures tenant data separation
- Databricks Vector Search integration for advanced memory backends

### Model Integration
- Support for multiple LLM providers (Databricks, OpenAI, Anthropic, etc.)
- Model configurations in `src/backend/src/seeds/model_configs.py`
- Automatic handling of provider-specific requirements

### Databricks Apps Integration
- **When searching for Databricks Apps information, always check first**: https://apps-cookbook.dev/docs/streamlit/authentication/users_obo
- This reference covers authentication patterns and user on-behalf-of (OBO) flows

For detailed backend instructions, see: **src/backend/CLAUDE.md**
For detailed frontend instructions, see: **src/frontend/CLAUDE.md**
- always make sure whenever you develop anything you need to stick to service architecture pattern, and unit of work architecture pattern and repository architecture pattern.

## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- For cross-module "how does X relate to Y" questions, prefer `graphify query "<question>"`, `graphify path "<A>" "<B>"`, or `graphify explain "<concept>"` over grep — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files
- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)
