# Event-driven triggers (Lakebase queue)

A third way to start a crew/flow run, beside **interactive** (`api`) and
**scheduled** (`cron`): a durable Postgres/Lakebase queue table (`triggerqueue`)
that a background consumer drains and dispatches. A producer drops an *event*
row; Kasal runs the bound crew/flow, tagged `trigger_type="lakebase_queue"`.

> Status: **Phase 1** — the queue + consumer + direct-target dispatch. Phase 2
> (subscription `event_type → target` routing and emit rules so one crew's output
> can trigger the next) is planned; the `event_type` / `correlation_id` /
> `causation_run_id` columns are already present for it.

## Enabling it

Off by default. Set on the backend process:

| Env var | Default | Meaning |
|---|---|---|
| `KASAL_EVENT_TRIGGERS_ENABLED` | (off) | `1`/`true`/`yes` starts the consumer loop |
| `KASAL_EVENT_TRIGGERS_INTERVAL` | `5` | seconds between claim polls |
| `KASAL_EVENT_TRIGGERS_BATCH` | `5` | rows claimed per poll |

The consumer only starts when the database is initialized **and** the flag is
set. Claims use `FOR UPDATE SKIP LOCKED` on Postgres/Lakebase, so running the
loop on multiple app replicas is safe (no double-claim). On SQLite (local dev)
the lock clause is omitted — single-worker, which is all dev needs.

## The message

A row is a **trigger**, not the agents' state — a pointer + inputs + tenancy,
never definitions, tools, memory, or secrets. Producers insert into `triggerqueue`:

| Column | Purpose |
|---|---|
| `target` (JSON) | what to run — see below |
| `payload` (JSON) | event body; run **inputs** are read from `payload.inputs` |
| `group_id` | tenant — becomes the run's `GroupContext` |
| `event_type` | topic name (reserved for Phase 2 subscription matching) |
| `correlation_id` | chain id — threads crew→crew hand-offs |
| `causation_run_id` | run that emitted this event (Phase 2) |
| `idempotency_key` | unique — dedupes duplicate producers |

`target` shapes (Phase 1):

```jsonc
// run a saved flow by id
{ "kind": "flow", "id": "<flow-uuid>", "harness": "kasal" }   // harness optional: kasal|crewai

// run a full config inline (crew or flow)
{ "kind": "inline", "config": { /* CrewConfig fields: agents_yaml, tasks_yaml, execution_type, ... */ } }
```

`kind: "crew"` (run a *saved crew* by id) is Phase 2 — for now pass the crew
config with `kind: "inline"`, or use a saved flow.

## Enqueuing an event

Any producer with write access to the `triggerqueue` schema can insert a row —
another service, a Databricks Job, a webhook handler, or a Lakebase **synced
table** mirroring a Delta table. Example (psql / Lakebase):

```sql
INSERT INTO triggerqueue (group_id, target, payload, correlation_id, status, attempts)
VALUES (
  'user_dev_localhost',
  '{"kind":"flow","id":"a898336c-2483-44e2-9e0c-ff8e938165c5"}',
  '{"inputs":{"topic":"technology news today"}}',
  'chain-9f2c',
  'pending',
  0
);
```

The consumer claims it, builds the run config, creates the run record, launches
it, and marks the row `dispatched`. The run's status/result live in
`execution_history` (the queue only records dispatch).

## Delivery semantics

- **At-least-once.** A crash between `claimed` and `dispatched` leaves a row in
  `claimed`; the consumer's periodic **reclaim** returns rows stuck longer than
  ~15 min to `pending`. `idempotency_key` (unique) is the backstop against a
  producer re-emitting the same logical event.
- **Retry/backoff.** A dispatch failure requeues the row with exponential
  backoff (`available_at`); after `MAX_ATTEMPTS` (5) it is dead-lettered
  (`status='dead'`, `last_error` set).
- **Non-blocking.** The consumer launches each run as its own task and never
  awaits a whole run, so one slow run can't stall the queue.

## Where the code lives

| Piece | Path |
|---|---|
| Table model | `src/models/trigger_queue.py` |
| Data access + claim | `src/repositories/trigger_queue_repository.py` |
| Consumer | `src/services/triggers/queue_consumer_service.py` |
| DTOs (producer contract) | `src/schemas/triggers.py` |
| Loop wiring | `src/main.py` (lifespan) |
| Migration | `migrations/versions/20260825_trigger_queue.py` |

Dispatch reuses the scheduler's path: `ExecutionService.create_run_record(...)`
+ `ExecutionService.run_crew_execution(...)`.
