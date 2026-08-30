# Memory

How Kasal gives agents a memory: what a memory record is, how it is written and recalled, how the store is kept true over time, and how to configure it.

- [Code map](#code-map)
- [The record](#the-record)
- [Backends](#backends)
- [Isolation and scoping](#isolation-and-scoping)
- [The write path](#the-write-path)
- [The read path](#the-read-path)
- [Memory tuning](#memory-tuning)
- [Maintenance](#maintenance)
- [When maintenance runs](#when-maintenance-runs)
- [Schema management](#schema-management)
- [Memory browser](#memory-browser)
- [Environment variables](#environment-variables)
- [Known limits](#known-limits)
- [Rules for adding behavior](#rules-for-adding-behavior)
- [Related](#related)

## Code map

`src/backend/src/services/memory/` is organised by lifecycle stage. Read it in this order:

| Package | Stage | What is in it |
|---------|-------|---------------|
| `config/` | What the teamspace configured | `MemoryBackendService` (facade), CRUD on the `memory_backends` rows, which configuration is active, and `LakebaseMemoryService` for the pgvector table |
| `storage/` | Where records live | `LocalStorageBackend` (`local.py`) and `LakebaseStorageBackend` (`lakebase.py`) behind one `StorageBackend` protocol; `factory.py` builds one from a configuration and maps the scoring knobs onto it; `adapter.py` lets the engine talk to either |
| `engine/` | The `Memory` object | `memory.py` (`remember` / `recall`, every tuning knob as a declared field), `types.py` (`MemoryRecord`), `analyze.py` (what the memory LLM says about a record), `recall_planner.py` (distillation and exploration), `consolidation.py` (the save-time merge) |
| `run/` | A run's memory | `crew_memory.py` builds a `Memory` for Chat, Agent Builder and Flow Builder alike; `recall.py` injects the context block before a task; `persist.py` writes the result after it, through the `pending.py` overlay and the `write_hygiene.py` screen |
| `maintenance/` | Between runs | `passes.py` (dedupe, merge, the combined pass and its throttle), `supersession.py`, `forgetting.py`, `sweep.py` |
| `text.py` | Shared | The boilerplate strip and the token-set similarity both sides use |

A record's path through a run: `run.recall` asks `engine.Memory.recall`, which searches `storage` — the task runs — `run.persist` hands the output to `engine.Memory.remember`, which labels it, folds it into a near-duplicate if one exists, and saves it through `storage`. Later, `maintenance` tidies what accumulated.

## The record

There is one memory pool per teamspace and one record type in it: `MemoryRecord`, defined in `src/backend/src/services/memory/engine/types.py`.

Two fields drive most of the behavior on this page.

**`kind`** is the type of a memory, and it selects the retrieval policy:

| Kind | Holds | Recall policy |
|------|-------|---------------|
| `episodic` | What happened, and when: a chat turn, a task result, an observation tied to one moment | Decays with age on a half-life |
| `semantic` | What is currently true: a preference, a decision, a property of a system or dataset | Does not decay. A fact is not less true for being old |
| `procedural` | How to do something: a repeatable method or rule of thumb | Does not decay |

Episodic is the default, and any value the classifier does not recognise resolves to it — episodic decays and asserts nothing, so it is the safe reading of an unclassified record.

**The validity window** — `valid_from`, `valid_to`, `superseded_by` — applies to the durable kinds. `valid_to` is null while a fact holds. When a newer record contradicts it, the window closes and `superseded_by` names the replacement. Nothing is deleted: recall returns only currently-valid records, so a retired fact stops entering prompts while remaining available to answer "what did we believe on 3 March", and a wrong retirement can be undone.

The other record fields — `scope`, `categories`, `importance`, `source`, `private` — are stored in a `metadata` JSONB column on Lakebase. `kind` and the validity window are real columns, because every recall filters on `valid_to` and branches its scoring on `kind`, and a `->>` accessor in the hot scoring path cannot use an index.

## Backends

Two backends implement one `StorageBackend` protocol, so everything on this page behaves identically against either.

| Backend | Type value | Storage | When to use |
|---------|-----------|---------|-------------|
| Default | `default` | A SQLite file per teamspace, embeddings as float32 blobs, numpy cosine search | Local development. Microseconds at dev scale, no index required |
| Lakebase | `lakebase` | Postgres and pgvector with an HNSW index | Shared and production deployments |

The engine and the backends speak slightly different protocols — the engine passes query text, the backends expect a query embedding — so `storage/adapter.py` absorbs that in one place, with a small LRU cache so a repeated question costs no embedding call.

Records and queries are embedded with the same model and dimension. The default is `databricks-gte-large-en` at 1024 dimensions, and the configured `embedding_dimension` must match the pgvector column.

## Isolation and scoping

`group_id` — the teamspace identifier — is the tenant boundary and is applied to every operation. One teamspace can never observe or alter another's memory, on either backend, in any mode.

Within a teamspace there is a single scoping rule, and reads and deletes both use it:

| Mode | Scoped by |
|------|-----------|
| Teamspace memory on (the default) | `group_id` |
| Teamspace memory off | `group_id` + `session_id` |

In Chat mode this is the "Teamspace memory" toggle: on, recall spans everything the teamspace has learned; off, it is confined to the current conversation. Non-chat executions always run teamspace-wide.

Rows also carry `crew_id`, written as provenance so a trace can say which run produced a memory. **It filters nothing.** `crew_id` is a hash of crew structure and changes whenever that structure does — including with each chat prompt, since the prompt is part of the hash — so any query scoped by it would wall a run off from its own history.

## The write path

Persistence never blocks a run. `remember_async` in `run/persist.py` is the single boundary where run-produced content enters memory — chat turns and task outputs both arrive there — and the write goes through five steps in order.

1. **Screening.** Content is scanned for prompt-injection patterns. The scan is deterministic and regex-based (`PromptInjectionDetector`, shared with the security scanner pipeline) rather than an LLM call, because it runs on every chat turn and every finished task. High-severity content is not persisted; lower-severity findings are recorded in the record's metadata.
2. **Hand off to a writer pool.** The durable write runs on a two-worker daemon pool and the caller does not wait. Crew and flow subprocesses call `flush_memory_writes()` before exiting so the final task's save is not lost with the interpreter.
3. **Publish to the pending overlay.** The record becomes readable in-process the moment it is submitted, and is dropped once the durable write lands. Without this the task immediately after a write would not see it — the write and the next recall race, and the read wins.
4. **Classify.** One small LLM pass per record fills `categories`, `importance`, and `kind` together. Records that arrive already labelled skip the pass, which is what keeps maintenance free of a model call per merged cluster.
5. **Consolidate, then store.** If the store already holds a record that says the same thing (semantic similarity at or above the consolidation threshold), the memory LLM rewrites the two into one and the existing record is updated in place; otherwise the new record is inserted. Either way the `memory_write` trace names the record that landed (`record_id`), and the record carries the run's `execution_id` in its metadata.

## The read path

Recall is one semantic query per turn or task, injected as a compact text block. Two optional steps around that query are governed by the tuning knobs below: a long query is first distilled by the memory LLM into a short search query, and when the result is not convincing a bounded number of alternative queries are explored. The plain search always runs, so those steps can only add hits, and without a memory LLM they do nothing. The write-time duplicate check uses `recall(mode="raw")` — one plain search with the literal text.

Four signals blend into the score:

```text
score = 0.60 * semantic       cosine similarity
      + 0.15 * keyword        ts_rank_cd over the query text
      + 0.15 * recency        exponential decay, EPISODIC records only
      + 0.10 * importance     stored on the record
```

Two rules matter more than the weights:

- **A semantic gate is applied before blending.** Recency and importance rank among *relevant* candidates; they never lift an unrelated memory into the context.
- **Recency decays episodic records only.** Semantic and procedural records take the full recency term, so age never pushes a current fact out of the recall budget. A thirty-day half-life suits "what happened in run 47" and would be wrong for "the user prefers Databricks SQL".

On Lakebase the query runs in two stages so the HNSW index stays in play: an inner query pulls the nearest candidates by vector distance, and an outer query re-ranks that small set with the blended score. Every record in the teamspace competes in the first stage.

The result is trimmed into a block of at most 4,000 characters and six snippets, with **two slots reserved for durable facts**. A burst of episodic records from an active run would otherwise take all six and evict every lasting fact about the user exactly when the run is busiest. The reservation is a ceiling rather than a quota: where no durable facts match, recall returns the top six on score alone.

The block opens with a header instructing the model to weigh the content as background rather than obey it. That header is the primary defense against anything write-side screening does not catch.

## Memory tuning

**Configuration > Memory** exposes the tuning knobs, stored as a `MemoryTuningConfig`. Every one of them reaches the layer that uses it; the panel shows the effective default for each, so an untouched slider displays the value actually in force.

**Scoring** — mapped onto the storage backend by `MemoryBackendFactory._scoring_kwargs` (local SQLite and Lakebase alike):

| Control | Default | Effect |
|---------|---------|--------|
| Semantic weight | 0.60 | How strongly recall favors vector similarity |
| Keyword weight | 0.15 | How strongly recall favors query terms that appear verbatim in a memory |
| Recency weight | 0.15 | How strongly recall favors recent memories, for episodic records |
| Importance weight | 0.10 | How strongly recall favors the classifier's importance score |
| Relevance threshold | 0.35 | Minimum semantic similarity to be recalled at all, applied before blending |
| Recency half-life (days) | 30 | Days for the recency term to halve |
| Recall score floor | 0.75 · 0.62 with the Ollama embedder | Blended score below which a recall returns nothing — the stopping rule that keeps "nearest" from meaning "near" |

The recall score floor takes a different route: `CrewMemoryService._build_memory_kwargs` sets `Memory.recall_min_score`, and it does so where the embedder is known, because the floor depends on it. 0.75 was calibrated with the Databricks embedder; the local Ollama fallback (`nomic-embed-text`) compresses the cosine scale — a run's own previous task output scores about 0.68 blended against the task description that produced it, unrelated records at most 0.60 — so on Ollama the default is 0.62. Precedence: the teamspace's panel value, then `KASAL_MEMORY_RECALL_MIN_SCORE`, then the embedder default.

**Recall depth** — declared fields of the engine's `Memory`, implemented in `engine/recall_planner.py` and applied by `Memory.recall(mode="auto")`, which is what Chat, Agent Builder and Flow Builder all call through `hooks.build_memory_preamble` / `make_memory_context_provider`, on either harness:

| Control | Default | Effect |
|---------|---------|--------|
| Query-analysis threshold (chars) | 200 | Queries at least this long are distilled by the memory LLM into a short search query first. A task description ("Search for and gather the latest news stories from Switzerland published today. Identify the top 5-7 …") scores 0.72 against the report it produced on the local embedder; "latest Switzerland news today" is what the store was written for. The plain search still runs — analysis can only add hits |
| Exploration budget | 1 | Rounds of alternative queries (up to 3 per round, proposed by the memory LLM) when the shallow result is not convincing. 0 = shallow only |
| Confidence — stop exploring at | 0.8 | Best-hit score at or above which no exploration round runs |
| Confidence — explore below | 0.5 | Best-hit score below which the budget is spent |
| Complex-query threshold | 0.7 | Complexity (0–1, judged by the analysis call) at or above which recall explores even between the two confidence bounds |
| Memory LLM | The crew's model | Model used for the analysis, exploration and merge calls, classification, and the maintenance passes |

No LLM, an empty store, or a malformed reply degrades every one of these to the shallow search — never to a failed recall. The write-time duplicate check (`hooks._already_remembered`) calls `recall(mode="raw")`: it compares the literal text it is about to write, so distilling it would be wrong. The trace's Memory Read row carries `query`, and when analysis ran, `distilled_query` and `exploration_rounds`.

**Save-time consolidation** — `engine/consolidation.py`, run inside `Memory.remember` before the insert:

| Control | Default | Effect |
|---------|---------|--------|
| Consolidation threshold | 0.85 | Similarity at or above which the new memory is merged INTO the closest existing one instead of being stored beside it (0 disables) |
| Consolidation limit | 5 | How many nearest existing memories are compared — also the neighbour count of the write-time duplicate check |
| Default importance | 0.5 | Importance given when neither the writer nor the analysis supplies one |

The similarity compared is the semantic component the local backend reports on each hit (`metadata["semantic"]`); Lakebase reports only the blended score, which is used instead. The merge is a rewrite by the memory LLM that keeps every fact from both notes — so **without a memory LLM the pass is skipped** and the note is inserted: silently dropping a note that merely resembles an old one (yesterday's report vs today's) would lose data, and the literal-duplicate case is already caught, model-free, by the write-time gate. The folded-into record is what the write's `memory_write` trace names (its `record_id`), and its metadata counts `consolidated_writes`. Maintenance's own merged output (`source: consolidation`) is never folded — its members are about to be deleted.

## Maintenance

Four passes tidy the store, cheapest first. All are best-effort: a failure logs and no-ops, and none may break a run.

| Pass | Module | Cost | What it does |
|------|--------|------|--------------|
| Dedupe | `maintenance/passes.py` (`consolidate_memory`) | No LLM | Deletes exact-content duplicates, keeping the newest |
| Merge | `maintenance/passes.py` (`merge_similar_memories`) | One LLM call | Replaces a cluster of near-duplicate or fragmented records with one merged record |
| Supersede | `maintenance/supersession.py` | One LLM call | Retires facts a newer record contradicts, by closing their validity window |
| Forget | `maintenance/forgetting.py` | No LLM | Deletes records past a retention rule. Off by default |

These run between runs over what accumulated. The save-time consolidation described under [Memory tuning](#memory-tuning) is different: it acts on one record as it is written, and its output (`source: consolidation` for the maintenance merge, `consolidated_writes` in metadata for the save-time fold) is never fed back into either pass as if the run had written it.

The first two make the store smaller; supersession makes it truer, and the two pull in opposite directions on the same input. The merge prompt is therefore told that contradictions are not fragments: preserving every distinct detail is right for two halves of one fact and wrong for a conflict, which supersession resolves instead.

Forgetting is conservative. It removes superseded records once past a retention window — they are already excluded from recall, so removing them changes no recall result — and old, low-importance *episodic* records. A current fact is never aged out, at any age.

## When maintenance runs

The three execution paths finish at very different rates, so each enters through a different door.

| Path | Entry point | Why |
|------|-------------|-----|
| Crew, flow | `run_memory_maintenance` at teardown | A run is a coarse boundary. Flows register their memory at build time, since a flow's crews build it deep inside the flow |
| Chat | `schedule_maintenance_after_writes` | A turn takes seconds, so a per-scope slot is claimed at most once per interval |
| Every teamspace | The scheduled sweep | Coverage that does not depend on anyone running anything |

The chat throttle lives in process memory and is a rate limiter. The **sweep** is the durable half: a background loop asks which teamspace has gone longest without maintenance, takes the most overdue few, maintains them, and stamps a row in `memory_maintenance_watermarks`. A teamspace with no watermark sorts first, so a newly configured one is picked up on the next tick rather than after a full interval.

A failed pass still advances the watermark, with the reason recorded on the row. Otherwise a teamspace whose backend is unreachable would hold the front of the queue and starve every teamspace behind it, and its failures would be visible only in logs.

## Schema management

The Lakebase memory table is created by `LakebaseService.initialize_tables`, reachable from an admin-only endpoint.

Because that endpoint is the only thing that runs the table's DDL, the backend also repairs its own schema. Every database operation routes through one session helper that, once per process per table, checks `information_schema` and adds any column the current code expects. The repair runs in its own transaction: sessions here roll back on exception and Postgres DDL is transactional, so sharing the caller's transaction would let an unrelated failure undo the repair while the process cache recorded it as done.

The local SQLite backend applies the same guarantee in `LocalStorageBackend._migrate_columns`.

## Memory browser

The Memory Browser (`src/frontend/src/components/MemoryBackend/MemoryRecordsBrowser.tsx`) shows what a teamspace remembers: stored records with their scope and categories, and a concept graph of how memories relate. It reads the configured backend directly rather than through the recall path, so it also shows records recall would filter out.

The browser, and the chat's memory pane beside a finished run, can be scoped to one run. That scoping is exact, and it is the same on all three execution paths (Chat, Agent Builder, Flow Builder):

- **Saved** — the records the run wrote. A run's `memory_write` traces carry the stored `record_id` (the OTel event bridge stamps it for Agent Builder and Flow Builder runs from `MemorySaveCompletedEvent`; the Chat path stamps it from its save hook), and every record a run writes carries the run's `execution_id` in its metadata (`hooks._persist_task_output` for task outputs, the chat service for turns). A record matching either is the run's, except consolidation output: end-of-run maintenance re-saves merged records under whichever run triggered it, and those are maintenance's, not the run's. Runs traced before the ids existed are matched by the text their write traces recorded. There is no time-window inference: a run whose traces carry no writes shows nothing under Saved. (Windowing was how the browser used to attribute a run's records, and it attributed everything written in the window — chat turns, maintenance merges, and for the oldest run in the database everything ever written — to the selected run.)
- **Recalled** — the records the run read: the `record_ids` on its `memory_retrieval` traces.

Both views read the run's memory rows through `GET /traces/job/{job_id}?event_type_prefix=memory_` so a long run's writes, which land last, are not cut off by the default trace page.

## Environment variables

Behavior that is not configured per teamspace is controlled by environment variables:

| Variable | Default | Effect |
|----------|---------|--------|
| `KASAL_MEMORY_RECALL_MIN_SCORE` | by embedder | Deployment-wide recall score floor; a teamspace's Memory Tuning value takes precedence over it |
| `KASAL_MEMORY_WRITE_SCREENING` | `quarantine` | `quarantine` blocks high-severity injection matches; `annotate` records findings without blocking; `off` disables screening |
| `KASAL_MEMORY_LLM_CONSOLIDATION` | `true` | Set to `false` to disable the LLM merge pass |
| `KASAL_MEMORY_SUPERSESSION` | `true` | Set to `false` to disable retiring contradicted facts |
| `KASAL_MEMORY_FORGETTING` | `false` | Set to `true` to enable deletion of records past their retention rule |
| `KASAL_MEMORY_SUPERSEDED_RETENTION_DAYS` | `90` | How long a retired fact is kept as history |
| `KASAL_MEMORY_EPISODIC_TTL_DAYS` | `180` | Age past which a low-importance episodic record may be removed |
| `KASAL_MEMORY_IMPORTANCE_FLOOR` | `0.4` | Records at or above this importance are kept regardless of age |
| `KASAL_MEMORY_MAINTENANCE_INTERVAL` | `900` | Seconds between chat-triggered maintenance passes for one scope |
| `KASAL_MEMORY_SWEEP` | `true` | Set to `false` to disable the scheduled sweep |
| `KASAL_MEMORY_SWEEP_INTERVAL_HOURS` | `6` | How stale a scope must be before the sweep revisits it |
| `KASAL_MEMORY_SWEEP_BATCH` | `5` | Scopes maintained per tick |
| `KASAL_MEMORY_DIR` | `~/.kasal/memory` | Root directory for local memory stores |

## Known limits

Four things are worth knowing before relying on the behavior above.

- **Maintenance reads the newest 500 records per pass.** The listing is ordered newest-first with a fixed limit, so a record is eligible for consolidation while it remains in that window. This bounds tidying only; recall searches the entire store.
- **The sweep runs without a user token.** It builds memory outside any request, so the embedder and analysis model resolve through the service-principal or PAT chain. Where that is unavailable the scope is recorded as unavailable and skipped whole rather than half-maintained.
- **`last_accessed` is not refreshed on recall.** It is written at save time, so forgetting cannot use "never re-accessed" as a signal and uses `importance` as the proxy.
- **The memory browser does not display `kind` or the validity window.** A retired fact is not visually distinguishable there.
- **Recall planning costs model calls.** With the defaults, a task-start recall over a non-empty store spends one memory-LLM call on distillation (task descriptions exceed 200 characters) and, when the result is weak, one more per exploration round. Raise the query-analysis threshold or set the exploration budget to 0 to opt out per teamspace.
- **Save-time consolidation on Lakebase compares the blended score.** The local backend reports raw semantic similarity per hit; Lakebase's search does not, so the threshold is applied to the blend there.

## Rules for adding behavior

- **Memory must never break a run.** Every pass, hook, and screen catches its own exceptions. A broken backend degrades a run to one without memory; it does not fail it.
- **Recall and persistence rules belong in the app layer.** `run/recall.py` owns recall assembly and `run/persist.py` owns persistence; the engine's execution loop does not consult memory itself.
- **Keep the two backends in step.** They implement one protocol, and any divergence between them is a bug a customer finds by switching backends. Scoping and scoring changes land in both, with tests pinning both.
- **A new column needs a self-heal, not only DDL.** Alembic does not manage Lakebase, and the table DDL runs only from an admin endpoint.
- **Expensive work belongs in the sweep.** An LLM call or a delete on a run's critical path is paid for by whoever happened to run something.
- **A new tuning knob must reach the layer that uses it.** Scoring knobs go to the storage backend through `MemoryBackendFactory._scoring_kwargs`; every other knob must be a declared field on `engine.Memory`, forwarded by `CrewMemoryService._build_memory_kwargs`. Pydantic drops unknown kwargs silently — five knobs were inert for months that way — so `test_memory_tuning_plumbing.py` pins that every knob lands.
- **One file per concern, one package per stage.** A new module goes in the stage it belongs to (`config/`, `storage/`, `engine/`, `run/`, `maintenance/`), and the package `__init__` docstring names it. A helper both sides need goes in `text.py`, not in one side with an import from the other.

## Related

- [Solution architecture](./ARCHITECTURE_GUIDE.md): where memory sits among the platform layers and the security model.
- [LLM architecture](./LLM_ARCHITECTURE.md): the layering behind the model calls that classification and maintenance make.
- [Lakebase deployment](./lakebase-deployment.md): provisioning the Postgres and pgvector instance that backs production memory.
- [API endpoints reference](./api_endpoints.md): the memory backend configuration and browser endpoints.

---

Back to the [documentation hub](./README.md).
