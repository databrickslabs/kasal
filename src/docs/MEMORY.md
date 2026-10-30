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
- [Graph memory](#graph-memory)
- [Settings](#settings)
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
| `engine/graph_*.py`, `engine/extract.py`, `storage/*_graph.py` | Graph memory | The alternative to records, behind a teamspace switch — see [Graph memory](#graph-memory) |
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

- **A semantic gate is applied before blending, and it is the only gate.** Whether a memory is recalled at all is decided by cosine similarity alone: the relevance threshold, raised to the recall score floor. Recency, keyword and importance rank among the *relevant* candidates; they never lift an unrelated memory into the context, and they never drop a relevant one. That second half is what makes teamspace memory reach across sessions: a memory from an earlier session is older, and when the floor was applied to the blend, the recency decay pushed every such memory below it.
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
| Recall score floor | 0.60 · 0.64 with the Ollama embedder | Cosine similarity below which a memory is not recalled — the stopping rule that keeps "nearest" from meaning "near". Applied before blending, so age and importance never decide it |

The recall score floor takes a different route: `CrewMemoryService._build_memory_kwargs` sets `Memory.recall_min_score`, and it does so where the embedder is known, because the floor depends on it. The storage backend applies it as its semantic gate (`min_semantic`), the same cosine on local SQLite and Lakebase.

0.60 was calibrated on 28 Oct 2026 with the Databricks embedder (`databricks-gte-large-en`), on 25 chat questions scored against 20 stored turns. Related question-to-turn pairs had a median cosine of 0.71, unrelated pairs a 95th percentile of 0.59, and off-topic probes ("how do I bake sourdough bread?") reached at most 0.49. At 0.60, with the relative cliff and the six-snippet budget, recall surfaced a related memory for 17 of 22 questions and nothing for the off-topic ones. The previous floor, 0.75 on the blended score, surfaced a related memory for none of them. The local Ollama fallback (`nomic-embed-text`) compresses the cosine scale, with unrelated records measured at 0.58–0.63, so its floor sits above that band at 0.64. Precedence: the teamspace's panel value, then the embedder default.

A value a teamspace saved before this change was a blended score and is now read as a cosine. A saved 0.75 is a much stricter floor than it was; clear it to return to the default.

Short questions about a stated fact match their turn weakly on this embedder: "what is my name?" scored 0.50 against "User: My name is …", no higher than an unrelated question scored against the same turn. No fixed floor separates those, so personal facts are the case a turn-shaped memory record recalls worst; graph memory, which stores the extracted fact rather than the turn, is the better fit for them.

**Recall depth** — declared fields of the engine's `Memory`, implemented in `engine/recall_planner.py` and applied by `Memory.recall(mode="auto")`, which is what Chat, Agent Builder and Flow Builder all call through `build_memory_preamble` / `make_memory_context_provider` (`run/recall.py`), on either harness:

| Control | Default | Effect |
|---------|---------|--------|
| Query-analysis threshold (chars) | 200 | Queries at least this long are distilled by the memory LLM into a short search query first. A task description ("Search for and gather the latest news stories from Switzerland published today. Identify the top 5-7 …") scores 0.72 against the report it produced on the local embedder; "latest Switzerland news today" is what the store was written for. The plain search still runs — analysis can only add hits |
| Exploration budget | 1 | Rounds of alternative queries (up to 3 per round, proposed by the memory LLM) when the shallow result is not convincing. 0 = shallow only |
| Confidence — stop exploring at | 0.8 | Best-hit score at or above which no exploration round runs |
| Confidence — explore below | 0.5 | Best-hit score below which the budget is spent |
| Complex-query threshold | 0.7 | Complexity (0–1, judged by the analysis call) at or above which recall explores even between the two confidence bounds |
| Memory LLM | The crew's model | Model used for the analysis, exploration and merge calls, classification, and the maintenance passes |

No LLM, an empty store, or a malformed reply degrades every one of these to the shallow search — never to a failed recall. The write-time duplicate check (`_already_remembered` in `run/persist.py`) calls `recall(mode="raw")`: it compares the literal text it is about to write, so distilling it would be wrong. The trace's Memory Read row carries `query`, and when analysis ran, `distilled_query` and `exploration_rounds`.

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

The Memory Browser (`src/frontend/src/features/memory/components/MemoryRecordsBrowser.tsx`) shows what a teamspace remembers: stored records with their scope and categories, and a concept graph of how memories relate. It reads the configured backend directly rather than through the recall path, so it also shows records recall would filter out.

The browser, and the chat's memory pane beside a finished run, can be scoped to one run. That scoping is exact, and it is the same on all three execution paths (Chat, Agent Builder, Flow Builder):

- **Saved** — the records the run wrote. A run's `memory_write` traces carry the stored `record_id` (the OTel event bridge stamps it for Agent Builder and Flow Builder runs from `MemorySaveCompletedEvent`; the Chat path stamps it from its save hook), and every record a run writes carries the run's `execution_id` in its metadata (`_persist_task_output` in `services/memory/run/persist.py` for task outputs, the chat service for turns). A record matching either is the run's, except consolidation output: end-of-run maintenance re-saves merged records under whichever run triggered it, and those are maintenance's, not the run's. Runs traced before the ids existed are matched by the text their write traces recorded. There is no time-window inference: a run whose traces carry no writes shows nothing under Saved. (Windowing was how the browser used to attribute a run's records, and it attributed everything written in the window — chat turns, maintenance merges, and for the oldest run in the database everything ever written — to the selected run.)
- **Recalled** — the records the run read: the `record_ids` on its `memory_retrieval` traces.

Both views read the run's memory rows through `GET /traces/job/{job_id}?event_type_prefix=memory_` so a long run's writes, which land last, are not cut off by the default trace page.

## Graph memory

A teamspace can store an entity-and-fact graph instead of whole records. It is off by default; **Configuration > Memory > Store memory as a fact graph** (`MemoryTuningConfig.graph_memory`) turns it on. The data model follows [Graphiti](https://github.com/getzep/graphiti) (Apache-2.0) — typed entities, fact edges with a validity window, replaced facts invalidated and never deleted — implemented on the stores Kasal already runs, with no graph database.

**What changes.** A write extracts zero or more durable facts from the turn or task output and stores those; the turn itself is not stored. A fact is `subject --relation--> object` plus the one sentence that states it. Most turns contain nothing durable and write nothing. Recall searches facts only: existing records stay in the store and in the Records view, but are not recalled while graph memory is on.

| Step | Where | What it does |
|------|-------|--------------|
| Extract | `engine/extract.py` | One memory-LLM call per write. Facts are taken from the **user's words only** — the assistant's reply is context, never a source — and cover preferences, standing instructions, decisions, and properties of the user and their systems, datasets and projects. The extractor is shown up to 40 facts already in memory and told not to repeat them, and to reuse a fact's relation name when its value changes. The same call returns the run note (below) |
| Skip a repeat | `engine/graph_memory.py` | One exchange reaches memory twice (the chat agent's own persist, then the conversation recorder once the answer settles). The second arrival is recognised by its text and skipped before it costs a model call |
| Resolve | `storage/*_graph.py` | An entity is found by its normalised name (`entity_key`: case, accents and punctuation folded), created on first mention. The person speaking is always the entity `user` |
| Dedupe | `storage/*_graph.py` | A fact whose subject, relation and object already exist as a current fact is not stored again. A partial unique index enforces it across processes |
| Replace | `storage/*_graph.py` | For a single-valued relation, a new value closes the current one (`valid_to`, `superseded_by`). No model call |
| Recall | `engine/graph_memory.py` | One embedding and one search: vector and keyword rankings fused by reciprocal rank, with standing preferences in front. No model call |

**Kind.** The ontology (entity types, relations) says what things are. Kind is the other axis: it decides where an item may come from, when it is recalled and how long it lives. It is a column on every fact (`kind`, in `engine/graph_types.py`), and both stores apply one policy per kind:

| Kind | Holds | May come from | Recalled | Lifetime |
|------|-------|---------------|----------|----------|
| `procedural` | Standing preferences and instructions: how the assistant should respond in every conversation | The user's words only | Always — up to 5, in front of every recall | Until the user changes it |
| `semantic` | Facts about the user and their work: role, employer, projects, decisions | The user's words only | On a match (see *Recall rule*) | Until replaced |
| `finding` | Facts a run established about a named entity: `Nestlé --revenue_fy2025--> CHF 89.5 bn`; when an answer compares several named things, each is an entity with its measures as qualifiers | The run's answer | On a match (see *Recall rule*) | Until replaced; a corrected measure replaces it |
| `episodic` | One run note: what a run produced, as *subject: main findings*, attached to the one thing it is mainly about | The run, only when it left no finding | On a closer match (0.6) | Stops being recalled after *Keep run and chat memories (days)* (`episodic_ttl_days`, default 180) |

The extractor assigns the kind and `parse_facts` enforces the source: nothing about the user can be a finding, and only the user holds a procedural fact. A finding is semantic memory in every other respect — it is typed by the ontology and a corrected value replaces the old one — but it is only as good as the source the run read, so it is kept distinguishable and needs the closer match. News of the day, forecasts and opinions are not findings; such a run leaves a note. The record kinds recall consumers read (`MemoryRecord.kind`) map from these: a finding reads as `semantic`, with its own kind in `metadata["kind"]`.

**Recall rule.** A fact must reach a cosine of 0.52 (`DEFAULT_MIN_SIMILARITY`; the *Relevance threshold* knob overrides it), a run note 0.6 (`DEFAULT_STRICT_MIN_SIMILARITY`), and every match must be within 0.09 of the best one (`DEFAULT_RECALL_CLIFF`). Fixed floors alone could not separate right from wrong on `nomic-embed-text`: right facts for "what was the most effective marketing campaign" scored 0.53–0.60 while wrong facts for other requests scored up to 0.62. Relative to the best match they separate. On every case measured on 15 Oct 2026 (12 requests, 21 right facts) the rule kept 15 right facts where fixed floors per kind kept 12, letting the same number of wrong ones through. Notes keep the closer floor because a note recalled for an unrelated request is a false memory of what a run did. Notes are embedded without their date and written as *subject: findings*, never "the user asked… the assistant produced…": shared text is what made unrelated notes look alike.

**Only what matches is recalled.** Recall returns standing preferences plus the facts that matched the request — nothing "related". An earlier version followed a matched fact's entity to its other facts. In real runs every such addition was noise: a revenue request came back with facts about an unrelated project through the `user` entity, and a Swiss-news request that matched "based in Zurich, Switzerland" came back with that person's whole career. It was removed.

**A standing preference needs the user's own words.** A `procedural` fact is applied to every later run, so a wrong one changes every later answer. "check only from postgres", said about one lookup, was once stored as a standing rule and a later news request refused to search the web. The extractor must now quote the words that make it standing ("always", "from now on", "I prefer"), and `parse_facts` drops it unless that quote is in what the user actually said. An instruction about one request is not stored as any kind of fact.

**Entity names are bare.** A named thing is stored under its name alone; what qualifies the relation (a role, a period, a field of study) is kept as `qualifiers` on the fact. "AWS (Solutions Architect, 2023–2024)" as an entity would be a different node for every job. `parse_facts` also splits a trailing parenthesis off an entity name when the model writes one anyway.

**Forgetting.** `DELETE /memory-backend/graph/facts/{id}` (the **Forget** button on each fact) retires one fact: it stops being recalled and stays listed under replaced facts.

**Vocabulary.** Entity types and seed relations are prescribed in `engine/graph_types.py`. The seed relations carry their own single-valued flag (`works_at`, `deadline_is`, `status_is` are single; `prefers`, `uses` are not). The extractor may coin a relation and says whether it is single-valued; an unknown entity type collapses to `concept`.

**Storage.** Two tables beside the record table: `graph_entities` / `graph_facts` in the teamspace's SQLite file (FTS5 with BM25 for keywords), and `<memory_table>_entities` / `<memory_table>_facts` in Lakebase (pgvector HNSW, a stored `tsvector` with a GIN index). `LakebaseGraphStore` creates its tables on first use and takes its session from `LakebaseStorageBackend.open_session`, so memory still has one place that reaches Lakebase.

**What reaches the extractor.** A fact store keeps only what it extracts, so it is shown far more of a run than a record store keeps: up to 12,000 characters of the answer (`_FACT_ANSWER_CHARS` in `run/persist.py`), against 1,400 for records. With the record budget, a Genie answer that opened with a paragraph of method never showed the extractor a figure and came back as a one-line note. JSON output — a tool's response handed back as the answer — is rendered as `key: value` lines with empty values dropped and nested JSON opened (`text.readable_output`), so ids, nulls and escape sequences do not crowd out the content; nothing there knows any tool's field names. A crew task's output arrives as `Task: <description> Result: <output>`, and only findings and a note are taken from it, since it holds no user words.

**One connected graph per answer.** The extractor is told that the findings from one answer form one graph: every finding connects, directly or through others, to what the request is about; when an answer compares several things, each links first to what it belongs to, with its measures as qualifiers on that link; a value several things can share — a category, a supplier, a channel, a region — is an entity, so the things sharing it connect; and links between the compared things themselves (a table that joins another) are facts too. Numbers, amounts and dates stay qualifiers or plain values. Measured with the real model: a product comparison went from two or three clusters to one, connected through the company, shared categories and shared suppliers; a schema answer gained its joins. A different subject in the same answer — a metric's definition, where the data came from — can still form its own small cluster; that is a separate subject, not a broken graph.

**No limit on facts per write.** Neither Graphiti nor Mem0 caps the facts one write may produce: Graphiti splits large, entity-dense input into ~3,000-token chunks and extracts from each; Mem0 extracts everything and reconciles each fact against similar memories. Every cap tried here (5, 8, 16) cost real facts — products lost their link to the company that sells them — so there is none. What keeps a write proportionate is the prompt: keep what the answer concludes and the things it names, never copy a table row by row, since the source can be asked again. On a 30-row table the model kept 4 to 8 facts. The model's own output limit bounds a runaway reply. A write's facts are embedded in as few requests as the endpoint allows (`EMBED_BATCH`, 64 texts per request — a transport limit, every fact is still embedded), falling back to one at a time if the embedder will not take a batch.

**Corrections.** The same fact (subject, relation, object) arriving with different qualifiers is a corrected measure: it replaces the old one, which is closed and points at it. A restatement without qualifiers, or with the same ones, is a repeat and changes nothing. The decision is `plan_write` in `graph_types`, shared by both stores.

**Scoping.** `group_id` on every statement, as for records. With the chat "Workspace memory" switch off, reads are confined to the session's own facts; dedupe and replacement stay teamspace-wide.

**Browser.** The Memory Browser's **Ontology** view (`MemoryOntologyView.tsx`, `GET /memory-backend/graph`) shows entities sized by their current facts, an edge wherever a fact links two entities, and the fact list, with replaced facts on request.

**Limits.**

- Entities are matched by normalised name only. "Databricks" and "Databricks Inc." are two entities.
- Lakebase keyword ranking is Postgres full-text (`ts_rank_cd`), not true BM25.
- The recall rule (0.52, 0.6 for notes, within 0.09 of the best) was measured on `nomic-embed-text` only, on twelve requests. Another embedder has another cosine scale. A generic question still pulls weak matches when nothing matches it well: "where do I work" also recalled a contact's employer at the same score as the right fact.
- Whether a preference is `procedural` (always recalled) or `semantic` (recalled on a match) is the extractor's call.
- A finding is what a run read on a given day. It carries the run and the date it was stored, not a verified source, and it does not expire.
- Only the first 12,000 characters of an answer reach the extractor.
- A question repeated in other words can leave a second, near-identical run note.
- A relation the extractor coins under two names ("employer", "works_at") is two relations, so the second does not replace the first. Showing the extractor the known facts reduces this; it does not prevent it.
- Expired run notes stop being recalled but are not deleted.
- The repeat check is per process: a crew subprocess and the server do not see each other's recent exchanges.
- Every graph-memory write logs one `[graph-memory] write (…): N fact(s) extracted, M stored` line to `crew.log`; an empty extraction and a write that never ran are otherwise indistinguishable.
- A run's Saved / Recalled facts are shown in the chat's Run memory pane and in the Memory Browser dialog's Ontology tab when a run is selected; the dialog's Records, Concepts and Graph tabs read the record table only.
- A recall made by the agent itself (the CrewAI engine's chat agent) is reported as the run's Memory Read, so Recalled lists it.
- The record store's concept views (Concepts, Graph) do not include facts.

## Settings

None of these are environment variables any more: a Databricks App never sets them, so they live in the UI.

**Per teamspace — Configuration → Memory → Memory Tuning → Write screening and retention.** Stored in the backend's `cognitive_config` and forwarded to `engine.Memory` as declared fields:

| Setting | Default | Effect | Replaced |
|---------|---------|--------|----------|
| Write screening (`write_screening`) | `quarantine` | `quarantine` blocks high-severity injection matches; `annotate` records findings without blocking; `off` disables screening | `KASAL_MEMORY_WRITE_SCREENING` |
| Recall cut-off below best match (`recall_max_drop`) | `0.12` | Drops recall candidates scoring this far below the best one | `KASAL_MEMORY_RECALL_MAX_DROP` |
| Merge near-duplicates between runs (`llm_consolidation_enabled`) | on | The LLM merge pass | `KASAL_MEMORY_LLM_CONSOLIDATION` |
| Retire contradicted facts (`supersession_enabled`) | on | Supersession | `KASAL_MEMORY_SUPERSESSION` |
| Forget expired memories (`forgetting_enabled`) | off | Deletion of records past their retention rule | `KASAL_MEMORY_FORGETTING` |
| Keep replaced facts (`superseded_retention_days`) | `90` | How long a retired fact is kept as history | `KASAL_MEMORY_SUPERSEDED_RETENTION_DAYS` |
| Keep run and chat memories (`episodic_ttl_days`) | `180` | Age past which a low-importance episodic record may be removed | `KASAL_MEMORY_EPISODIC_TTL_DAYS` |
| Never forget at importance (`importance_floor`) | `0.4` | Records at or above this importance are kept regardless of age | `KASAL_MEMORY_IMPORTANCE_FLOOR` |

The deployment-wide recall floor override (`KASAL_MEMORY_RECALL_MIN_SCORE`) is gone: the Memory Tuning recall score floor covers it.

**Server-wide — Configuration → Engines → System settings → Advanced** (system administrators). The sweep and its throttle act across every workspace:

| Setting | Default | Effect | Replaced |
|---------|---------|--------|----------|
| Background memory sweep | on | The scheduled sweep | `KASAL_MEMORY_SWEEP` |
| Sweep a workspace every (hours) | `6` | How stale a scope must be before the sweep revisits it | `KASAL_MEMORY_SWEEP_INTERVAL_HOURS` |
| Workspaces per sweep | `5` | Scopes maintained per tick | `KASAL_MEMORY_SWEEP_BATCH` |
| Seconds between passes on one scope | `900` | Throttle for the pass that follows a run; `0` runs it every time | `KASAL_MEMORY_MAINTENANCE_INTERVAL` |

The role knowledge search assumes on a Lakebase instance (`SET ROLE`) is **Database role** in the memory backend's Lakebase settings (`lakebase_config.db_role`; default `databricks_superuser`, empty assumes none). It replaced `LAKEBASE_KNOWLEDGE_ROLE`.

`KASAL_MEMORY_DIR` (default `~/.kasal/memory`) is still an environment variable: it is where local stores live on disk, a property of the host.

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
