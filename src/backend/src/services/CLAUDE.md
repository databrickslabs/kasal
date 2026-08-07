# Services Layer CLAUDE.md

Instructions for the business-logic layer in `src/backend/src/services/`.

## Role of this layer

Services own **business logic and orchestration**. They validate rules,
coordinate one or more repositories, enforce group isolation, and
encrypt/decrypt sensitive data. They are called by routers and by the engine,
never the reverse.

## Every service is a package

`services/` is a directory of DOMAINS, never of loose modules. A new service goes
in `services/<domain>/<module>.py` — not `services/<domain>_service.py` beside
it. A loose file leaves the next thing that domain needs with nowhere to go, so
it lands as `<domain>_service_helpers.py` alongside, and the boundary that was a
directory becomes a naming convention nobody enforces.

`tests/unit/architecture/test_services_are_packages.py` fails on a loose file,
and on a package with no `__init__.py` (an implicit namespace package imports
fine until a stale `__pycache__` shadows it — a failure that reads as "the module
vanished"). It has NO allowlist, so an exemption has to be argued for in review.

**Protocols split by direction.** `a2a/` and `mcp/` each hold `*_server/`
(Kasal ANSWERING someone else) and `*_client/` (Kasal CALLING someone else). The
two directions have opposite trust models — inbound, the caller is untrusted and
Kasal decides what to expose; outbound, Kasal makes requests to an address a
tenant supplied — and which side a file is on should be legible from its path,
not from reading it. `services/mcp_server/` used to sit at the top level, one
word away from `services/mcp/`; that is what this layout fixes.

## Two shapes live here

Not everything in this directory is a `*_service.py` CRUD class, and that is
deliberate. **Capability packages** — `tools/`, `memory/`, `guardrails/`,
`security/`, `knowledge/`, `export/`, `a2ui/`, `trace/`, `task_output/`,
`converters/` — are things an agent run needs that do not require a crew to be
running. They were moved out of the old `src/engines/` tree so a chat turn, crew
generation, or an exported app can use them without importing an orchestrator.

`converters/` came from a different place: it was `src/converters/`, a top-level
package sitting beside `services/` with no contract covering it. It is the
KPI/KBI conversion library the PowerBI and metric-view tools are built on
(YAML → DAX / SQL / UC Metrics / M-Query). Its layout is
`base/` (the `BaseConverter` + `ConversionFormat` contract), `common/`
(transformers and translators shared by every target) and `formats/`
(one package per target: `powerbi/`, `sql/`, `uc_metrics/`, `mquery/`). That last
one was itself called `services/` — which, once the package moved under
`src/services/`, would have read `services/converters/services/powerbi`.
`formats/` matches the `ConversionFormat` enum the base contract already uses.

`src/engines/` is gone entirely: the three paths are `chat/`, `agent_builder/`
and `flow_builder/`, over shared machinery in `execution/`. See
`execution/CLAUDE.md`.

The rule that keeps a capability a capability: it may import `kasal_engine` (the
vendored LIBRARY — `BaseTool`, `event_bus`, `MemoryRecord`), but it must
not import a PATH package. A guardrail that imports `flow_builder` has stopped
being usable from a chat turn, which is the whole reason these moved.

## Layering, and how it is enforced

    api  →  services  →  repositories  →  models
                ↓
              core, utils, schemas   (below everything; never import upward)

`import-linter` checks this on every `run_tests.py` run — contracts live in
`[tool.importlinter]` in `pyproject.toml`, or run `lint-imports` directly. It
traces TRANSITIVE chains, which is the point: the violations it found were
mostly `repositories → utils → services`, invisible to a grep for
`from src.services` in `repositories/`.

Each contract carries an `ignore_imports` list of KNOWN violations, each with the
reason it is still there. **That list is meant to shrink.** Adding to it needs a
reason in review; the whole value of the check is that it fails on new ones.

Two rules the contracts encode:

- **A repository never imports a service.** No exceptions — clean.
- **`core/` and `utils/` never import services.** `core` is clean. Five
  exceptions remain in `utils/`, all lazy imports, all reviewed and ACCEPTED
  rather than pending: `user_context` (group membership), `databricks_auth`
  (the OBO → PAT → SPN chain), `prompt_utils` (a DB-backed template).

  The fix would be a `CredentialProvider` port in `core`, implemented in
  services. It was deliberately not taken: this is the auth path for every
  Databricks call and it runs inside the crew and flow SUBPROCESSES, so a global
  provider registry introduces a failure mode a lazy import cannot have — miss
  the registration in one entry point and PAT lookup silently degrades to
  environment variables with no error.

  **Do not "fix" these by pointing them at the repositories.**
  `ApiKeysService.find_by_name` RAISES when `group_id` is None; that is the
  multi-tenant isolation guarantee, and reaching past it into `ApiKeyRepository`
  would silently drop the check the contract exists to protect.

Two more rules that `import-linter` cannot express (constructing a query or
opening a session is about what you DO with an object, not what you import), so
each has an AST check in `tests/unit/architecture/`:

- **A service should not build queries, and should not persist rows.**
  Repositories own both; a service holds a session only for transaction control
  (`commit`/`rollback`). `test_service_query_construction.py` checks two things:
  - *queries* (`session.execute`, `select(...)`) — **zero, no baseline.** This was
    a ratchet; it is now a hard ban. Four offenders moved their SQL into a
    repository, and the last two — the DEAD post-subprocess log writers — were
    DELETED rather than refactored, since neither was reachable from `src/`.
    Keep `_BASELINE` empty.
  - *ORM writes* (`session.add/add_all/delete/merge`) — **zero, no baseline.** This
    is the half a query check cannot see: ten services were persisting rows with no
    SQL in sight while the query ratchet reported six offenders. It skips modules
    using `aiohttp.ClientSession`, whose `.delete()` is an HTTP call.

  The exemption for both is DDL — `databricks/lakebase/{migration,schema,permission,management}`
  execute raw SQL because there is no repository for `CREATE SCHEMA`.
- **A service should not open a session.** See the next section;
  `test_sessions_go_through_the_router.py` enforces it with no baseline.

## Another domain's data goes through that domain's SERVICE

Repositories are not a shared data-access pool. Each belongs to a domain, and that
domain's service is where its invariants live — group scoping, encryption, cascade
order, status transitions. Construct another domain's repository and you get the
table without the rules.

`tests/unit/architecture/test_service_repository_ownership.py` enforces this as a
ratchet: `_OWNED` says which repositories each domain may use freely, and `_BASELINE`
records what is left. It started at **42** pairs and is down to **6** — all of them
the `repositories` dict the flow runner injects into `BackendFlow` inside the flow
SUBPROCESS, which cannot be converted safely from in-process tests. Convert that and
this becomes a hard ban.

Closing the other 36 surfaced four real bugs, every one swallowed by an `except`:
GEPA wrote agent/task rows past `get_with_group_check`;
`TemplateService(template_repository)` passed a repository where a session was
expected, so `generate_connections` never loaded its prompt; `tool_factory` called
`get_databricks_config(group_id=...)` on a no-argument method and reported "no config"
for every workspace; and `delete_all_traces_for_group` called a repository method that
does not exist, so bulk trace deletion always failed silently. That is the argument
for the rule — the bypass is where this class of bug hides.

**When the owning service does not expose what you need, add it there.** That is not
a platitude — it is what the scheduler needed. `SchedulerService` built
`ExecutionHistoryRepository` itself because `ExecutionService.get_execution()`
returns a flow-shaped dict and applies NO tenant filter, while the scheduler needs
the ORM row AND `group_ids` (without the filter, anyone could schedule from another
tenant's run and read its config and prompts). The fix was
`ExecutionService.get_execution_record(execution_id, group_ids=...)` — the guarantee
moved into the owning service instead of the caller reaching past it.

The exemptions are `databricks/lakebase/` (it migrates, backs up and truncates every
table — routing through 20 services would be absurd) and `export/templates/`
(shipped into exported apps, which have no service layer).

Note what this does NOT forbid: a service using its own domain's repositories. That
is the normal chain.

## Conventions (match `agent_service.py`)

- File named `<resource>_service.py`. Extend `BaseService[Model, CreateSchema]`
  from `src.core.base_service` when doing standard CRUD.
- Constructor takes `session` first and builds its repository from it:
  ```python
  def __init__(self, session: AsyncSession,
               repository_class: Type[AgentRepository] = AgentRepository,
               model_class: Type[Agent] = Agent):
      super().__init__(session)
      self.repository_class = repository_class
      self.model_class = model_class
      self.repository = repository_class(session)
  ```
  Injecting `repository_class`/`model_class` with defaults keeps the service unit
  testable (pass a fake repository).
- Accept the `session` you are given — see the next section.

## Sessions: one place decides the database

**A service NEVER opens a database session or builds an engine.** Not for a
background task, not "just for logging", not because the request session is
closed, not behind a comment saying it is a special case. If you are typing
`async_session_factory(`, `create_async_engine(`, `async_sessionmaker(` or
`sessionmaker(` inside `services/`, the change is wrong — use
`routed_scoped_session()` (outside a request) or the injected session (inside
one). `tests/unit/architecture/test_sessions_go_through_the_router.py` fails the
build on it, and the allowlist there is a record of past exceptions, not an
invitation to add one.

A service receives a session, hands it to its repositories, and controls the
transaction. The flow is one-directional:

    entry point (router / task / tool)  ->  decides the database
      service                           ->  transaction control
        repository                       ->  queries only

Repositories NEVER acquire or commit a session — they take one in the constructor
(`BaseRepository(model, session)`). Three of them once opened their own; the
comments recording the removal are still there (`flow_repository.py`,
`task_repository.py`, `execution_history_repository.py`). Don't reintroduce it.

### Which helper to use

| Situation | Use |
|---|---|
| Inside an HTTP request | the injected `SessionDep` — do not open anything |
| Background task, subprocess, tool — anything outside a request | `routed_scoped_session()` |
| A commit spanning a slow LLM call on SQLite | `get_isolated_db_session()` (a private connection; the shared StaticPool one can have another request's rollback discard your committed row) |
| Code that runs on its OWN event loop (`new_event_loop`, a sync bridge) | `get_isolated_db_session()` — see below |

**The loop trap.** On SQLite the router hands back the shared StaticPool
connection, which is bound to whichever event loop first opened it. Code that
creates a *new* loop per call — `kasal_flow_persistence`, which bridges CrewAI's
sync `FlowPersistence` hooks — then fails on the second call with "Future attached
to a different loop" / "Event loop is closed". `get_isolated_db_session()` is the
answer there and loses nothing: it checks `is_lakebase_enabled()` itself, so it
still reaches Lakebase, and does so by the same signal the reads use rather than
relying on this process having hot-swapped the global factory.

`routed_scoped_session()` is the default and covers most cases: it reuses the
request's session when there is one and otherwise goes through the database ROUTER,
which re-reads `is_lakebase_enabled()` on every call — so you do not have to know
which situation you are in. The two `get_isolated_db_session()` rows are the only
exceptions, and both are about CONNECTION isolation on SQLite, not about which
database to use. Neither is licence to reach for the raw factory.

**There is exactly one helper, and that is deliberate.**
`request_scoped_session` used to sit beside `routed_scoped_session` and has been
DELETED. It was the trap: the name read as the safe, request-aware option, and
inside a request it *was* identical — but outside one its fallback was literally
`async with async_session_factory()`, the same snapshot and the same silent split.
That indirection is why it survived several audits. 37 call sites looked
request-scoped; 33 were plain snapshot reads.

`routed_scoped_session` absorbed the one case that genuinely needed the raw
factory: a read made while auth is ALREADY resolving. The router needs an API key
to reach Lakebase, so routing the key lookup closes the loop
(`get_smart_db_session` → `get_auth_context` → `ApiKeysService` →
`get_smart_db_session`) — which logged 1,287 "maximum recursion depth exceeded"
and killed every crew and flow subprocess. That is now a `_RESOLVING_AUTH`
ContextVar check *inside* the helper rather than a second helper you had to know
to pick, and it is strictly better for credential reads: an API-key lookup made
away from the auth path now ROUTES, where before it always snapshotted — which is
how a configured Perplexity key read as absent.

So: **outside a request, `routed_scoped_session` is the only answer.** There is no
longer a wrong helper to choose, and the architecture test fails if the deleted one
is reintroduced.

### Providers: when the same wiring repeats

Outside a request there is no router to construct your service, so the caller does
it by hand — acquire a session, build the service, call it. That is legitimate DI,
done manually. It becomes a problem when the SAME three lines appear in unrelated
files, because each copy is a chance to get it wrong:

    async with routed_scoped_session() as session:
        config = await DatabricksService(session, group_id=gid).get_databricks_config()

That block existed verbatim in **seven** files, and two copies were broken. One
called `get_databricks_config(group_id=...)` — the method takes no arguments — so it
raised `TypeError`, an enclosing `try` swallowed it, and the auth check reported "no
Databricks config found" for every workspace. The same copy built the service
without a `group_id`, which would have read another tenant's row once the TypeError
was fixed. Nobody noticed because the failure mode was a soft fallback.

When you see that shape three or more times, add a **provider**: one place that owns
the acquisition and the construction, with the parameters that matter as real
arguments rather than things you may forget.

- `services/databricks/workspace/config_provider.py` — `DatabricksConfigProvider.get(group_id=...)`
- `services/tools/tool_session_provider.py` — sessions and pre-built services for tools

A provider is **not a new layer**. It is still service → repository → session; it
just does the wiring a FastAPI DI provider would have done. Do not put logic in one.

Use the session directly (not a provider) when the block needs the SAME session for
a second repository — `mlflow_setup` reads `MLflowRepository` alongside the config,
and both must see one transaction.

### Why the raw factory is almost always wrong

`async_session_factory` is a global mutable singleton that gets hot-swapped to
Lakebase — but only in the `main.py` lifespan (i.e. Lakebase was already on at
BOOT) or inside a spawned subprocess. A runtime `/lakebase/enable` never swaps it.
So the process runs SPLIT: routed reads go to Lakebase while every raw-factory
holder keeps reading the local database.

The failures share a shape that makes them expensive to find: **the query succeeds
and returns nothing.** No exception, no log — the caller concludes "no rows" and
carries on. That has now cost five separate incidents:

- a GEPA optimizer scored a completed crew 0.0 for 14 minutes
- a configured Perplexity API key read as absent
- an MCP server enabled for a workspace gave 1 tool in Agent Builder and 0 in Chat
- `workflow_recipes.embedding` was never created, so recipe lookup failed
- chat read a different `databricksconfig` than the crew subprocess

`tests/unit/architecture/test_sessions_go_through_the_router.py` enforces this. It
checks CALLS, not imports (importing the factory to inspect `is_lakebase` is
fine), and it follows `as` aliases — `async_session_factory as _plan_factory` hid
the last unrouted write in this codebase from every grep. Its allowlist carries
the reason each entry cannot route; the ones that are genuinely impossible rather
than merely entrenched:

- **`db/`** — it IS the session layer; it builds the engines everyone receives
- **`utils/databricks_auth`** — REENTRANT. The router needs a credential to reach
  Lakebase, so routing auth's own read closes the loop; that logged 1,287
  "maximum recursion depth exceeded" in production and killed every crew and flow
  subprocess. `_auth_scoped_session` routes the outermost entry only
- **`api/healthcheck_router`** — deliberately probes the FACTORY to report whether
  the swap happened; routing it would make the check pass by construction
- **`seeds/`**, **`main.py`**, **`scripts/`** — entry points; choosing the
  database is their job
- **`services/databricks/lakebase/`** — connects TO Lakebase to test and migrate
  it, so it cannot ask the router for a session to the thing it is setting up

The **post-subprocess log flush** (`execution/logs/db_handler`,
`flow_builder/process_executor._write_logs_postgres_async`) used to be on that
list, justified as running after the subprocess event loop is gone. It is not
there any more, because neither was reachable: the handler was never instantiated
in `src/` and the writer had no caller outside tests, while both built an engine
against the LOCAL database with no Lakebase awareness — i.e. each was a latent
split of exactly the kind this check exists to catch, not an exception to it.
Both have been DELETED. The live logs path is queue -> writer, and `writer.py`
routes through `get_smart_db_session`.

Adding to that list needs a reason in review. The whole value of the check is that
it fails on a new one.

## How a service gets its repositories

The service instantiates them on the session it was given — the DI session inside
a request (committed by the router at the end of it), or the one
`routed_scoped_session()` yielded outside a request. That is the whole pattern.

**`UnitOfWork` (`src.core.unit_of_work`) is on the way out — do not add callers.**
It exists for multi-repository atomic work, and nothing here does that: of its 4
async call sites, 3 (`tools/databricks_jobs_tool.py`) open a UoW and then use no
repository at all, and the 4th (`agent_builder/task_adapter.py`) uses exactly one.
A single repository needs no unit of work — the session already is one. What the
ceremony does buy is a second way to acquire a session, which is the bug class
this document is mostly about. `SyncUnitOfWork` (a singleton, for the sync
guardrail callbacks) goes with it.

## Group isolation (required)

- Accept `group_context: GroupContext` (from `src.utils.user_context`) on any
  method that reads or writes tenant data, and pass its `group_id` down to
  repository filters. A service method that ignores group scoping is a data-leak
  bug.
- Stamp `group_id` and `created_by_email` on create.

## Security

- Encrypt sensitive fields before persisting and decrypt after reading, using the
  helpers in `src.utils.sensitive_data_utils` (see `_encrypt_tool_configs_in_data`
  / `_decrypt_agent_tool_configs`). Decrypted values are in-memory only — never
  write them back to the DB or into logs.
- For Databricks calls made from a service, add User-Agent telemetry
  (see `src/backend/CLAUDE.md`).

## Async

- All methods are `async`. Never block the event loop (no sync DB drivers, no
  `requests`, no `time.sleep`).
- Do not `commit()` inside service CRUD helpers that run within a request — the
  session lifecycle owns the transaction. Commit explicitly only when you own the
  session (UoW / background task).

## Crew generation (`crew_generation_service.py`) — learned the hard way

Two mistakes here shipped a "crew generation always produces 1 agent + 1 task"
regression (vs v1.3.0). Do not reintroduce them:

- **Never gate generation behavior on `chat_mode_type` alone.** The field
  DEFAULTS to `"chat"` in the schema, and the AgentBuilder canvas chat (which
  builds real multi-agent crews as nodes) sends `auto_execute=False` with that
  default. The light-agent 1-agent/1-task constraint belongs ONLY to the ChatMode
  ANSWER run — i.e. `chat_mode_type == "chat" AND auto_execute` (that path
  normally short-circuits into `_run_chat_fast_path` anyway). A generate-only
  request must plan the full crew like research/deep.
- **Caps passed to `_generate_crew_plan` are UPPER BOUNDS, not predictions.**
  Never derive them from keyword heuristics: a hardcoded ACTION_VERBS lexicon
  capped "list data products, understand the contracts, …" to ONE task because
  "list"/"understand" weren't in the list. Verb-to-task mapping is the PLAN
  LLM's job (the `generate_crew_plan` template + few-shots own it; "use the
  minimum agents needed" keeps simple prompts small). Only an EXPLICIT numeric
  request ("4 agents", "8 tasks") changes the caps (hard cap 10/10); otherwise
  they stay at the template limits (6 tasks / 3 agents).
- Prompt templates are DB-backed (`TemplateService.get_effective_template_content`)
  but the seeder **overwrites** existing rows from `src/seeds/prompt_templates.py`
  on every startup — so edit templates in the seed file, and remember a running
  backend applies them only after a restart/reseed.
