# Services Layer CLAUDE.md

Instructions for the business-logic layer in `src/backend/src/services/`.

## Role of this layer

Services own **business logic and orchestration**. They validate rules,
coordinate one or more repositories, enforce group isolation, and
encrypt/decrypt sensitive data. They are called by routers and by the engine,
never the reverse.

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
- Accept the request-scoped `session` from the router. **Do not** open your own
  engine/session for request work.

## Two ways to get repositories (pick deliberately)

1. **Request-scoped session** (the default): the router passes the DI session and
   the service instantiates repositories on it. The router's `get_db`/smart
   session commits at the end of the request.
2. **`UnitOfWork`** (`src.core.unit_of_work`): use only for multi-repository
   atomic work outside a single request (background tasks, seeders, engine
   subprocesses) where you need explicit transaction control across repositories.
   Do not mix a UoW session with the request session.

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
