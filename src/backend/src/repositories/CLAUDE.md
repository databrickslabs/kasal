# Repositories Layer CLAUDE.md

Instructions for the data-access layer in `src/backend/src/repositories/`.

## Role of this layer

A repository owns **query construction** for one model, and nothing else. It is
the only place a `select()` / `update()` / `delete()` should be written. It has no
business rules, no HTTP concepts, no LLM calls, and no opinion about which
database it is talking to.

    api  →  services  →  repositories  →  models

A repository never imports a service. That one is CLEAN — `import-linter`
enforces it with no exemptions, and it is worth keeping that way.

## A repository RECEIVES its session. Always.

```python
class AgentRepository(BaseRepository[Agent]):
    def __init__(self, session: AsyncSession):
        super().__init__(Agent, session)
```

Constructor takes `session`, stores it, uses it. It does **not** open one, and it
does not decide which database that session points at — the entry point decided
that before the service was ever constructed:

    entry point (router / background task / tool)  →  decides the database
      service                                     →  transaction boundary
        repository                                →  queries only

Right now **zero** repositories acquire a session, and a test keeps it that way.
Three of them used to (`flow_repository.py`, `task_repository.py`,
`execution_history_repository.py` — the removal comments are still in place). Do
not reintroduce it, and do not add a module-level repository singleton: a
repository outlives its session only by holding a closed one.

The reason is not tidiness. A repository that opens its own session picks a
database, and the raw `async_session_factory` is a per-process SNAPSHOT that a
runtime `/lakebase/enable` never swaps — so it silently picks the WRONG one and
the query succeeds returning nothing. See "Why the raw factory is almost always
wrong" in `../services/CLAUDE.md` for the five incidents that came from this.

`tests/unit/architecture/test_sessions_go_through_the_router.py` fails on a
repository that imports any session factory or engine. It has **no allowlist for
repositories** — there is no legitimate case.

## Naming the write methods

**These are a CONVENTION, not an inherited contract.** `BaseRepository` gives you
`get`, `list`, `create`, `add`, `update`, `delete` — and nothing below. The four
names here are hand-written per repository, currently in **10 of 52**, and you do
not get them by subclassing. Check the repository before calling one.

When you add one, use these names and these semantics rather than inventing a
third spelling of "write this row":

| Method | Does |
|---|---|
| `insert(obj)` | `add` + `flush`, returns the object (so its `id` is available) |
| `remove(obj)` | `delete` + `flush` |
| `save()` | `flush` only — for attribute changes on an already-tracked instance |
| `reload(obj)` | `refresh` — re-read after a commit, for server-side defaults |

`flush`, not `commit`, is the default: the caller is usually mid-unit-of-work (a
skill create also writes its files; publishing also claims a name), and committing
inside the repository would take that choice away. Where a caller genuinely owns
its session and needs durability before handing off — the scheduler, which spawns a
run that must see the row — the method takes an explicit `commit: bool`.

**The table above is the intent; the tree has drifted from it.** Do not assume a
signature — the current exceptions are:

- `execution_history_repository.insert(run, commit: bool = False)` and
  `.remove(run, commit: bool = True)` — note the defaults DISAGREE, deliberately
  (insert leaves the commit to the router; remove's callers own their session and
  expect the row gone on return). This is the repository most likely to surprise
  you, and it also has `reload` and `save`.
- `chat_history_repository` has ONLY `save`.
- `documentation_embedding_repository` has `insert_raw(row: Dict)`, not `insert` —
  it takes a mapping rather than a model.
- `ui_config_repository` and `workflow_recipe_repository` have only `reload`;
  `user_repository` only `insert`.

Lifting these onto `BaseRepository` would need one signature for all of them, and
the disagreeing `commit` defaults above encode real caller expectations — so that
is a behavior change, not a refactor. Until someone makes that call deliberately,
this stays a per-repository convention and this section is the record of it.

## Who may construct you

A repository belongs to ONE service domain, and that domain's service is the only
place that should build it. A service in another domain that constructs your
repository gets your table without your rules — group scoping, encryption, cascade
order, status transitions.

`tests/unit/architecture/test_service_repository_ownership.py` maps every repository
to its owning domain and fails on a new cross-domain construction. If a caller in
another domain needs your data, add an accessor to YOUR service — that is what
`ExecutionService.get_run_by_job_id`, `HITLService.get_approval`,
`CrewService.get_crews_by_ids` and `AgentService.update_prompt_text_with_group_check`
are: methods added so the caller stopped reaching past the owner.

Two shapes recur, and both are worth copying:

- **Return the ORM row when the caller needs one.** `ToolService.get_all_tools`
  returns response schemas, which drops the fields another domain filters on; hence
  `list_tool_records`/`get_tool_record` beside it.
- **Take the `GroupContext`, not a raw id.** `update_prompt_text_with_group_check`
  verifies membership AND allowlists which fields may change, so a malformed key
  cannot reach `enabled` or `group_id`.

Two files under `repositories/` are not database repositories at all —
`dashboard_repository` and `genie_repository` are httpx API clients — so ownership
does not apply to them.

## Who commits

The owner of the session commits — the router's DI session at end of request, or
whoever opened it for background work. A repository should `flush()` when it
needs an ID back, and leave `commit()` alone: committing inside a repository ends
a transaction the service is still composing, so a later failure in the same
service call can no longer roll the earlier writes back.

Four repositories still commit their injected session and are the exception, not
the pattern (`mlflow_repository`, `powerbi_semantic_model_cache_repository`,
`execution_history_repository` — behind an explicit `commit` flag — and
`database_backup_repository`, which drives raw DBAPI connections for
export/import). Do not copy them into new code.

`rollback()` on a caught exception is fine and common here — the session is
unusable after a failed statement, so a repository that re-raises without
rolling back hands the service a poisoned session.

## Conventions (match `agent_repository.py`)

- File named `<resource>_repository.py`, one class per model.
- Extend `BaseRepository[Model]` from `src.core.base_repository` for standard
  CRUD; add named finders (`find_by_name`, `get_by_group`) for anything else.
  About 20 repositories don't extend it — appropriate when there is no single
  model behind them (`crew_generator_repository`, `genie_repository`,
  `memory_maintenance_repository`), not as a default.
- Return ORM models or plain data. Never Pydantic response schemas — that
  mapping is the service's and router's job.

## Group isolation (required)

Any finder over tenant data takes `group_id` (or `group_ids`) and puts it in the
`WHERE` clause. A query without it is a cross-tenant data leak, and it is a leak
that testing rarely catches because single-tenant dev data looks identical either
way. When a service passes `group_context`, the repository gets the `group_id`
off it — the repository does not import `user_context` to look it up itself.

**Do not let a caller reach past a service into a repository to skip a check.**
`ApiKeysService.find_by_name` RAISES when `group_id` is None; that raise IS the
isolation guarantee. Calling `ApiKeyRepository` directly to avoid it drops the
check silently.

## Async

- Every method is `async`; `await` every `session.execute`.
- Raw SQL is a last resort. When it is genuinely needed, use a SQLAlchemy
  construct instead of a driver-specific string: `literal_column("... :param")`
  bound with `:name` breaks on asyncpg, which uses `$1` paramstyle — that shipped
  a knowledge search that found nothing on Lakebase and worked locally. Prefer
  typed operators (`Model.embedding.cosine_distance(...)`), which compile per
  dialect.
