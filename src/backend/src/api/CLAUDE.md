# API Layer CLAUDE.md

Instructions for FastAPI route handlers in `src/backend/src/api/`.

## Role of this layer

Routers are the **thin HTTP boundary**. They translate requests into service
calls and services results into responses. They contain **no business logic and
no direct database access**. The only allowed chain is:

```
Router → Service → Repository → DB
```

## Conventions (match the existing files, e.g. `agents_router.py`)

- One router per resource, file named `<resource>_router.py`, exposing a module
  level `router = APIRouter(prefix="/<resource>", tags=["<resource>"])`.
- Register the router in `api/__init__.py` (or the aggregation point) — a new
  file alone does not wire it up.
- Provide the service via a local DI provider and a typed alias:
  ```python
  async def get_agent_service(session: SessionDep) -> AgentService:
      return AgentService(session=session)

  AgentServiceDep = Annotated[AgentService, Depends(get_agent_service)]
  ```
- Inject `session: SessionDep` (from `src.core.dependencies`) — this is
  `get_smart_db_session`, which routes SQLite/PostgreSQL/Lakebase automatically.
  Do not import engines or session factories directly.
- Use `response_model=<Schema>` and explicit `status_code=status.HTTP_*`. Return
  Pydantic schemas from `src.schemas`, never ORM models directly.

## Multi-tenancy and permissions (do not skip)

- Every data endpoint takes `group_context: GroupContextDep` and passes it to the
  service so reads/writes stay group-scoped. Missing this leaks data across
  tenants.
- Authorize with `check_role_in_context(group_context, [...])` from
  `src.core.permissions`, or the `require_*` decorators. Comment which roles are
  allowed (Admin / Editor / Operator).

## Errors

- Raise the domain exceptions from `src.core.exceptions`
  (`NotFoundError`, `ConflictError`, `ForbiddenError`, `BadRequestError`, ...).
  A global handler maps them to HTTP status codes. **Do not** hand-build
  `HTTPException` for these cases and do not leak stack traces to clients.
- Catch `IntegrityError` around writes and translate to `ConflictError`.

## Async and safety

- Every handler is `async def`. Never call blocking I/O directly.
- Never put SQLAlchemy queries in a router — that belongs in a repository.
- Keep secrets out of responses and logs (services already
  encrypt/decrypt/mask sensitive `tool_configs`).
