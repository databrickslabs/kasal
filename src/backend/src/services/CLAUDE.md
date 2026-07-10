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
