---
name: new-resource
description: Scaffold a full backend vertical slice (model, migration, self-heal, schema, repository, service, router, DI wiring, test) for a new Kasal entity, following the Router→Service→Repository→DB clean-architecture pattern with group isolation. Use when adding a new persisted resource/entity to the backend.
---

# new-resource

Scaffold a complete, group-isolated backend resource in Kasal that matches the
existing Router → Service → Repository → DB pattern. Model your output on the
`agent` slice (`models/agent.py`, `schemas/agent.py`,
`repositories/agent_repository.py`, `services/agent_service.py`,
`api/agents_router.py`) — read those first if unsure.

## Inputs to gather

- **Resource name** (singular, snake_case): e.g. `widget`. Derive:
  - Class: `Widget` (PascalCase)
  - Table: `widgets` (plural, snake_case)
  - Route prefix: `/widgets`
  - Files: `widget.py`, `widget_repository.py`, `widget_service.py`, `widgets_router.py`
- **Fields**: name/type pairs and which are required.
- Whether the resource is **group-isolated** (default: YES — almost everything in
  Kasal is).

## The 8 artifacts to produce

Create them in this order. All backend paths are under `src/backend/src/`.

### 1. Model — `models/<resource>.py`
- Extend `Base` from `src.db.base`.
- String UUID PK via a `generate_uuid` default (copy the pattern from `models/agent.py`).
- Group isolation columns (unless explicitly not group-scoped):
  ```python
  group_id = Column(String(100), index=True, nullable=True)
  created_by_email = Column(String(255), nullable=True)
  ```
- `created_at` / `updated_at` `DateTime` columns with `timezone`-aware defaults.
- Register the model so `create_all` sees it: import it in `src/db/all_models.py`.

### 2. Alembic migration — `migrations/` (via autogenerate)
- `cd src/backend && alembic revision --autogenerate -m "add <resource> table"`.
- Review the generated migration; test `alembic upgrade head` on **SQLite first**,
  then PostgreSQL.

### 3. Self-heal hook — `db/session.py`
- `create_all` never ALTERs existing tables, and deployed marketplace DBs cannot
  be manually migrated. If you add columns to an existing table, add an idempotent
  `_ensure_<resource>_columns(conn)` (SQLite: `PRAGMA table_info`; PG:
  `ADD COLUMN IF NOT EXISTS`) and call it from `init_db()`. For a brand-new table,
  add an `_ensure_<resource>_table(conn)` that does `Model.__table__.create(checkfirst=True)`.
  Follow the existing `_ensure_chat_sessions_table` / `_ensure_crew_columns` patterns.

### 4. Schema — `schemas/<resource>.py` (Pydantic v2)
- `WidgetBase(BaseModel)` with shared fields (use `Field(...)` for required,
  defaults otherwise).
- `WidgetCreate(WidgetBase)`, `WidgetUpdate(BaseModel)` (all optional),
  and read model `Widget(WidgetInDBBase)` with
  `model_config = ConfigDict(from_attributes=True)`.
- Do NOT reuse ORM models as response types — always return these schemas.

### 5. Repository — `repositories/<resource>_repository.py`
- Extend `BaseRepository[Widget]` from `src.core.base_repository`.
- Constructor: `def __init__(self, session: AsyncSession): super().__init__(Widget, session)`.
- Add group-scoped query methods (e.g. `get_by_group`, `list_by_group`) filtering
  on `self.model.group_id.in_(group_ids)`.
- Use `select(...)`, `await self.session.execute(...)`, `.scalars()`.
- **Do NOT `commit()`** — the session lifecycle owns the transaction. `flush()` to
  get generated IDs if needed.

### 6. Service — `services/<resource>_service.py`
- Extend `BaseService[Widget, WidgetCreate]` from `src.core.base_service`.
- Constructor takes `session` first, builds the repo, keeps `repository_class`/
  `model_class` injectable with defaults (for unit tests) — copy `agent_service.py`.
- Add group methods: `create_with_group(obj_in, group_context)`,
  `find_by_group(group_context)`, `get_with_group_check(id, group_context)`.
  Stamp `group_id` + `created_by_email` from `group_context` on create.
- If any field holds credentials, encrypt with
  `src.utils.sensitive_data_utils` on write and decrypt on read (in-memory only).

### 7. Router — `api/<resource>s_router.py`
- `router = APIRouter(prefix="/widgets", tags=["widgets"])`.
- Local DI provider + typed alias:
  ```python
  async def get_widget_service(session: SessionDep) -> WidgetService:
      return WidgetService(session=session)
  WidgetServiceDep = Annotated[WidgetService, Depends(get_widget_service)]
  ```
- Every data endpoint takes `group_context: GroupContextDep` and passes it down.
- Gate writes with `check_role_in_context(group_context, ["admin", "editor"])`
  (raise `ForbiddenError` otherwise); guard invalid context with `BadRequestError`.
- Raise domain exceptions from `src.core.exceptions` (`NotFoundError`, `ConflictError`,
  ...). Catch `IntegrityError` on writes → `ConflictError`. Never build raw
  `HTTPException` for these.
- Use `response_model=<Schema>` and explicit `status_code=status.HTTP_*`.

### 8. Wire-up + test (do not skip)
- **Register the router** in `api/__init__.py`: add the import and
  `api_router.include_router(widgets_router)`. A new router file alone does nothing.
- Add a unit test under `tests/unit/` mocking the repository (pass a fake
  `repository_class` into the service) covering create/list/get + a group-isolation
  case (a widget from another group must not be returned).

## Validation before done

```bash
cd src/backend
.venv/bin/python -m black src tests && .venv/bin/python -m isort src tests
.venv/bin/python -m mypy src
alembic upgrade head            # on SQLite, then PG
python run_tests.py --type unit -k widget
```

All async, no blocking I/O. See `src/backend/src/api/CLAUDE.md`,
`services/CLAUDE.md`, and `repositories/CLAUDE.md` for the per-layer rules.
