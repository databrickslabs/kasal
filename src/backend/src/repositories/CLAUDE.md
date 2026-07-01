# Repositories Layer CLAUDE.md

Instructions for the data-access layer in `src/backend/src/repositories/`.

## Role of this layer

Repositories are the **only** place that talks to the database. They wrap
SQLAlchemy queries behind typed methods and return ORM models. They contain no
business rules, no HTTP concerns, and no cross-repository orchestration (that is
the service/UoW job).

## Conventions (match `agent_repository.py`)

- File named `<resource>_repository.py`. Extend `BaseRepository[Model]` from
  `src.core.base_repository` for standard CRUD (`get`, `list`, `create`, `add`,
  `update`, `delete`).
- Constructor takes the session and passes the model up:
  ```python
  def __init__(self, session: AsyncSession):
      super().__init__(Agent, session)
  ```
- Add custom queries as async methods using SQLAlchemy 2.0 style:
  ```python
  query = select(self.model).where(self.model.id == id)
  result = await self.session.execute(query)
  return result.scalars().first()
  ```

## Transactions (critical)

- **Do NOT `commit()` in a repository.** The request session (`get_smart_db_session`)
  or the owning `UnitOfWork` commits. `BaseRepository.create` uses `flush()` +
  optional `refresh()` to surface DB-generated IDs without ending the transaction.
- Use `flush()` when you need the generated PK before the request ends.
- On error, the calling layer rolls back; a repository should not swallow errors
  and silently continue.

## Group isolation

- Tenant-scoped tables carry `group_id`. Provide group-filtered query methods
  (e.g. `list_by_group`, `get_by_group`) that filter on
  `self.model.group_id.in_(group_ids)`, and prefer them over unscoped `list()`
  for anything user-facing. An unscoped query on a group-scoped table is a
  data-leak bug.

## Async

- Every method is `async` and uses the async session. Never use a sync engine or
  blocking driver here.
