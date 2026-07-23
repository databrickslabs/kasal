# Memory

Kasal gives agents a unified cognitive memory: a single store where crews capture what they learn during a run and recall relevant context on later runs, scoped to your teamspace. It is built on CrewAI's unified `Memory` class (one `Memory` instance over one storage backend), so short-term, long-term, and entity context all live in one place rather than as separate stores.

## Memory backends

The backend is chosen by `MemoryBackendType` in `src/backend/src/engines/crewai/memory/memory_backend_factory.py`. Kasal offers two memory backends:

| Backend | Type value | Storage | When to use |
| --- | --- | --- | --- |
| Default (LanceDB) | `default` | CrewAI's built-in LanceDB store on local disk | Local development and quick trials. The factory returns `None` so CrewAI falls back to its own LanceDB backend. |
| Lakebase | `lakebase` | Lakebase (Postgres + pgvector) table | Shared and production deployments on Databricks, with a Postgres-backed store and `pgvector` similarity search. |

Notes:

- The Lakebase backend (`lakebase_storage_backend.py`) stores cognitive fields (`scope`, `categories`, `importance`, `source`, `private`) inside a `metadata` JSONB column and runs similarity search with the pgvector cosine distance operator (`<=>`).

## Capture and recall

The Lakebase backend implements CrewAI's `StorageBackend` protocol on `MemoryRecord` objects; the default backend uses CrewAI's built-in LanceDB store.

- Capture (`save` / `asave`): during a run the crew writes memory records. Each record carries `content`, an `embedding`, a `scope`, optional `categories`, an `importance` score, a `source`, and a `private` flag. If a record arrives without an embedding, the backend embeds the content itself before storing. Records are tagged with `crew_id`, `group_id`, and `session_id`.
- Recall (`search` / `asearch`): a query embedding is compared against stored vectors. The backend returns the closest records with a similarity score, then applies optional filters (scope prefix, category overlap, metadata equality) and a `min_score` cutoff. Private records are only returned to the session or crew that wrote them.

Writes and deletes are crew scoped: a crew only prunes or consolidates memories it wrote, even though it can read across the wider scope. Consolidation merges highly similar records on save (controlled by `consolidation_threshold`, default `0.85`).

## Ranking

Recall ranking is tunable through `CognitiveMemoryConfig` in `src/backend/src/schemas/memory_backend.py`. These weights map directly to CrewAI's `Memory` parameters and combine into a composite score:

```text
semantic_weight    default 0.5   weight for semantic similarity
recency_weight     default 0.3   weight for recency decay
importance_weight  default 0.2   weight for explicit importance
```

Recency decays on a configurable half life (`recency_half_life_days`, default `30`). Deep recall is gated by confidence thresholds (`confidence_threshold_high` default `0.8`, `confidence_threshold_low` default `0.5`) and a small exploration budget (`exploration_budget`, default `1`) for harder queries.

## Embeddings

Memory records and recall queries are embedded with the same model and dimension. The default embedding model is `databricks-gte-large-en`, which produces 1024 dimension vectors. Both backends default `embedding_dimension` to `1024`. The Lakebase config exposes `embedding_dimension`, so the value must match the dimension of the configured pgvector column.

The same model is also used for documentation embeddings that improve crew generation. See `src/docs/archive/technical/EMBEDDINGS.md` for that flow.

## Per-teamspace isolation

Memory is group aware. Every read and write is filtered by `group_id` (the tenant or teamspace identifier), so one teamspace can never observe another teamspace's memory. `group_id` is the isolation boundary in all backends.

Within a teamspace, read scope is controlled per execution:

- Teamspace wide (default): recall spans the whole teamspace (`group_id`), so any crew can recall context written by earlier runs.
- Session only: recall is confined to the current chat session (`session_id`), so only this conversation's history is recalled.

In Chat mode this is the "Teamspace memory" versus "Session memory" toggle. Note that `crew_id` is deliberately not a read scoping key: it is a deterministic per-crew-structure hash used for tracing and write tagging, and it changes whenever the crew structure changes, so scoping reads by it would wall every run off from the rest of the teamspace.

## Memory browser

The Cognitive Memory Browser (`src/frontend/src/components/MemoryBackend/MemoryRecordsBrowser.tsx`) lets you explore what a crew remembers: browse stored records, their scope and categories, and a concept force graph view of how memories relate.

## Configuration

Memory is configured per teamspace under Configuration > Memory. There you select the backend (Default or Lakebase), point it at the storage target (for Lakebase, a table and instance), set the embedding dimension, and adjust the cognitive ranking knobs (`CognitiveMemoryConfig`). Settings are stored as a `MemoryBackendConfig` and loaded at run time by the factory.
