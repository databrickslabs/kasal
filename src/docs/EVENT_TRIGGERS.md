# Event-driven triggers (Lakebase queue)

A third way to start a crew/flow run, beside **interactive** (`api`) and
**scheduled** (`cron`): a durable Postgres/Lakebase queue table (`triggerqueue`)
that a background consumer drains and dispatches. A producer drops an *event*
row; Kasal runs the bound crew/flow, tagged `trigger_type="lakebase_queue"`.

On top of the queue sits **choreography**: an *emit rule* makes a saved
crew/flow announce its completion as an event, and a *subscription* binds an
event to the next crew/flow to run — so one run's output triggers the next,
with no orchestrator in between.

## Enabling it

Off by default, controlled by a **database setting**, not an environment
variable: **Configuration → Engines → Event triggers** in the UI, or
`PATCH /engine-config/kasal/event-triggers` (system-admin only). The consumer
loop always starts with the app and checks the setting every tick, so flipping
the toggle takes effect within one poll interval — no restart. Emit-on-completion
is gated on the same setting: while it is off, finished runs emit nothing and no
pending rows pile up.

Environment variables tune the mechanics only:

| Env var | Default | Meaning |
|---|---|---|
| `KASAL_EVENT_TRIGGERS_INTERVAL` | `5` | seconds between claim polls |
| `KASAL_EVENT_TRIGGERS_BATCH` | `5` | rows claimed per poll |
| `KASAL_EVENT_TRIGGERS_MAX_HOPS` | `5` | chain-depth cap (see Chains below) |

Claims use `FOR UPDATE SKIP LOCKED` on Postgres/Lakebase, so running the loop on
multiple app replicas is safe (no double-claim). On SQLite (local dev) the lock
clause is omitted — single-worker, which is all dev needs.

## The message

A row is a **trigger**, not the agents' state — a pointer + inputs + tenancy,
never definitions, tools, memory, or secrets. Columns of `triggerqueue`:

| Column | Purpose |
|---|---|
| `target` (JSON) | what to run — see below |
| `payload` (JSON) | event body; run **inputs** are read from `payload.inputs` |
| `group_id` | tenant — becomes the run's `GroupContext` |
| `event_type` | the canonical event name (used for subscription matching) |
| `correlation_id` | chain id — threads the whole choreography chain from its origin run |
| `causation_run_id` | the run whose completion emitted this event |
| `idempotency_key` | unique — dedupes duplicate producers and double emissions |

Via the REST API (`POST /triggers/events`), `group_id` is **always stamped from
the authenticated caller's group context** — the request body has no tenant
field. Only a direct-SQL producer sets the column itself.

`target` shapes:

```jsonc
// run a saved flow by id
{ "kind": "flow", "id": "<flow-uuid>", "harness": "kasal" }   // harness optional: kasal|crewai

// run a saved crew by id
{ "kind": "crew", "id": "<crew-uuid>" }

// run a full config inline (crew or flow)
{ "kind": "inline", "config": { /* CrewConfig fields: agents_yaml, tasks_yaml, execution_type, ... */ } }

// POST the event to an external service instead of running anything
{ "kind": "webhook", "url": "https://example.com/hooks/kasal" }
```

A **webhook target** turns a subscription into server-to-server delivery: when
the event fires, the consumer POSTs `{event_type, event, inputs,
correlation_id, causation_run_id}` to the URL (inputs are shaped/mapped exactly
as for a crew target). A 2xx marks the row dispatched; a non-2xx or network
error retries with the queue's normal backoff and dead-letters after
`MAX_ATTEMPTS`; a malformed URL dead-letters immediately. Delivery is
at-least-once — receivers should dedupe on the `X-Kasal-Delivery` header.

Webhook URLs pass the same SSRF guard as HITL webhooks and A2A push configs
(`assert_safe_outbound_url`): **https only, public hosts only**, re-checked
after DNS resolution. A blocked URL dead-letters immediately. Local development
against a localhost receiver can opt out with
`KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS=1` on the backend process.

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

## Choreography: emit rules + subscriptions

Event names are **canonical, never free-text**: `{kind}:{id}:{type}`, e.g.
`crew:<uuid>:completed`. The lifecycle types are a constant enum (`completed`,
`failed`) — nobody invents names, so `research` vs `research.done` drift cannot
happen.

- An **emit rule** (`POST /triggers/emit-rules`) opts a saved crew/flow in to
  announcing a lifecycle type. When a run of that producer reaches a terminal
  status, the emit hook fires.
- A **subscription** (`POST /triggers/subscriptions`) binds a canonical event
  name to a target to run. Every enabled subscription to the emitted event
  becomes one queue row.

The downstream run's inputs come from the upstream output, resolved in this
order:

1. **`input_mapping`** — static input overrides, used as-is when present (it is
   *not* a payload projection — JSONPath-style mapping is future work).
2. **Schema shaping** — with a `schema_ref` on the subscription (or the emit
   rule), the result is shaped into the schema's STRUCTURE: a dict result (or a
   JSON-object string) is projected onto the schema's properties; a scalar
   result maps onto the schema's single required (or only) property. A `color`
   schema turns the result `"green"` into `{"color": "green"}`, so the
   subscriber's templates can use `{color}` directly. Shaping never invents
   data — an unresolvable schema or an unshapeable result falls through to:
3. **Passthrough** — a `completed` event passes the producer's output under
   `payload` (stringified when it is not a mapping); a `failed` event passes
   the error under `error`. The row's `payload.event` carries the canonical name,
an optional `schema` pointer (Object Management schema ref), the `source_run`,
and the chain depth (`hops`).

**How the downstream crew SEES the context.** Inputs are template variables:
`{payload}` (or any event key) written in a task's description/expected output
is interpolated with the value. Keys the templates never reference are not
dropped — the dispatcher appends them to the first task's description as an
explicit "Context from the triggering event" block, so a subscriber authored
without knowledge of its producer still sees the hand-off.

Emission only happens for runs of a **saved** crew/flow — an ad-hoc run has no
identity for a rule to match. Scheduled runs of a saved crew carry their
`crew_id` and emit like any other.

## Chains and the hop cap

`correlation_id` threads the ORIGIN of a chain: the first run's job id (unless
the producer supplied one) is carried through every downstream row, so all runs
of one chain can be queried together. `causation_run_id` is the immediate
parent.

A subscription whose target **is its own producer** (crew X subscribed to
`crew:X:completed`) is refused outright with a warning — a direct self-loop has
no terminating condition, so one event means one run, always.

For **indirect** cycles the guard is the hop cap: every emitted row carries
`event.hops` = parent depth + 1, and the emit hook refuses to emit at
`KASAL_EVENT_TRIGGERS_MAX_HOPS` (default 5). That catches A → B → A, or a
handler subscribed to another producer's `failed` event feeding back — chains
that would otherwise generate runs (and LLM spend) forever. A legitimately
deeper pipeline can raise the env var; a cycle should be re-wired.

## Delivery semantics

- **At-least-once.** A crash between `claimed` and `dispatched` leaves a row in
  `claimed`; the consumer's periodic **reclaim** returns rows stuck longer than
  ~15 min to `pending`. `idempotency_key` (unique) is the backstop against a
  producer re-emitting the same logical event.
- **Idempotent emissions.** The emit hook writes each row under a deterministic
  key (`emit:{run}:{type}:{subscription}`) inside a savepoint, so a double
  emission (two terminal-status writers racing, or a crash-retry) collapses onto
  the unique constraint instead of double-firing the subscriber.
- **Retry/backoff.** A transient dispatch failure requeues the row with
  exponential backoff (`available_at`); after `MAX_ATTEMPTS` (5) it is
  dead-lettered (`status='dead'`, `last_error` set). A **permanent** failure — a
  malformed target/config (`ValueError`) — dead-letters immediately: retrying a
  bad message five times just burns the queue.
- **Retention.** Finished rows (`dispatched`/`dead`) older than ~7 days are
  purged by the consumer's housekeeping tick, so the queue table does not grow
  forever. Pending/claimed rows are never purged.
- **Non-blocking.** The consumer launches each run as its own task and never
  awaits a whole run, so one slow run can't stall the queue.

## Roles

All `/triggers` routes require an authenticated group context. On top of that:
enqueue needs **operator**+ (admin/editor/operator); creating or deleting
subscriptions and emit rules needs **editor**+; deleting a queue row and manual
`POST /triggers/dispatch` need **operator**+. Manual dispatch additionally
requires the feature toggle to be ON, and only claims rows belonging to the
caller's groups — the background consumer is the only unscoped dispatcher.

## Where the code lives

| Piece | Path |
|---|---|
| Table models | `src/models/trigger_queue.py`, `src/models/event_subscription.py` |
| Data access + claim | `src/repositories/trigger_queue_repository.py`, `src/repositories/event_subscription_repository.py` |
| Consumer | `src/services/triggers/queue_consumer_service.py` |
| Emit-on-completion | `src/services/triggers/emit_service.py` (hooked from `services/execution/status.py`) |
| Subscriptions/emit-rules CRUD | `src/services/triggers/subscription_service.py` |
| Event names | `src/services/triggers/event_types.py` |
| DTOs (producer contract) | `src/schemas/triggers.py` |
| Router | `src/api/triggers_router.py` |
| Feature toggle | `src/api/engine_config_router.py` (`/engine-config/kasal/event-triggers`) |
| Loop wiring | `src/main.py` (lifespan) |
| Migrations | `migrations/versions/20260825_trigger_queue.py` |

Dispatch reuses the scheduler's path: `ExecutionService.create_run_record(...)`
+ `ExecutionService.run_crew_execution(...)`.
