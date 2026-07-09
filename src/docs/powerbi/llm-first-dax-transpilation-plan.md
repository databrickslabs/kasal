# LLM-First DAX Transpilation — Implementation Plan

**Status:** IMPLEMENTED (2026-07-09) on `feat/pbi-ucmv-fixes-v2`. Skill corpus vendored
(10 files under `metric_view_utils/skills/`), `LLMManager.completion_with_usage`
added (cached structured content + usage), `dax_llm_fallback` is now LLM-first
(corpus system prompt with `cache_control:ephemeral`, 7-category `dax_class`,
topo-ordered batch), `DaxTranslator.translate(trivial_only=)` fast-path,
`translation_mode='llm_first'` default. Verify harness: `/tmp/verify_llm_first_transpilation.py`.
(Original plan below.)
**Scope:** DAX measure → Spark SQL / UC Metric View translation
**Owner tools:** `tool-85-dax-to-sql-translator`, `tool-86-uc-metric-view-generator`
**Goal:** Match or exceed Databricks engineering's PowerBI→UCMV transpilation quality by
flipping our translator from regex-first to LLM-first, driven by a vendored copy of
engineering's DAX knowledge corpus.

---

## Background: why this change

Databricks engineering's `powerbi-migrate` tool achieves its transpilation quality with an
**LLM-first** architecture. The LLM does the actual DAX→SQL/YAML translation, guided by a rich
set of markdown "skill" files loaded on demand. Deterministic Python is used only for *parsing*
and *validation*, not translation.

Kasal today is the inverse — **regex-first**:

1. `DaxTranslator.translate()` runs ~15 hardcoded `re.fullmatch` patterns.
2. Post-passes handle window fixups, manual overrides, and `[A]-[B]` → `MEASURE()` arithmetic.
3. `dax_llm_fallback.py` only runs on leftovers, and only when `use_llm_fallback=True`. The LLM
   sees a terse ~30-line prompt.

The regex approach is fast and free but caps quality: every new DAX shape needs a hand-written
pattern, and the LLM (the part that generalizes) is relegated to a thin fallback with no
knowledge corpus behind it.

### Reference (engineering source, for manual re-copy)

Under `ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/` in the
`databricks-eng/universe` repo. **Two skill groups are needed** (see "Why both groups" below):

**`dax-translation/` — the translation logic (what to produce):**

| File | Content |
|---|---|
| `SKILL.md` | 7-category DAX translation decision framework |
| `PATTERNS.md` | 12-pattern DAX → SQL → YAML catalog (side by side) |
| `UNSUPPORTED.md` | `ALLSELECTED`, `USERELATIONSHIP`, time-intelligence + workarounds |
| `EDGE_CASES.md` | 11 gotchas: BLANK-arithmetic, DIVIDE-by-zero, SUMX row-context, name collisions |

**`uc-metric-views/` — the target-language spec (how to write it correctly):**

| File | Content |
|---|---|
| `SYNTAX.md` | UC metric view YAML grammar (`CREATE VIEW ... WITH METRICS`) |
| `WINDOW.md` | Window-measure syntax: `range`, `order`, `semiadditive` |
| `LOD.md` | Level-of-detail measures (coarser-grain aggregation) |
| `JOINS.md` | Join grammar for `RELATED` / cross-table dimensions |
| `COMPOSABILITY.md` | `MEASURE()` composition model |
| `MATERIALIZATION.md` | Aggregated vs unaggregated materialization |

#### Why both groups (gap identified during planning)
The `dax-translation/` skills instruct the LLM to *emit* window measures, LOD patterns, joins,
and `MEASURE()` composition — but the **syntax and rules for those targets live only in
`uc-metric-views/`**. Concretely:
- `PATTERNS.md` / `UNSUPPORTED.md` say "use window measures with `range: all` / `cumulative` /
  `trailing 1 year` / `semiadditive: last`" → grammar defined in `WINDOW.md` + `LOD.md`.
- `PATTERNS.md` maps `RELATED` → a `joins:` block → grammar defined in `JOINS.md`.
- The `MEASURE()` references throughout → model defined in `COMPOSABILITY.md`.

Vendoring only the DAX skills would leave the LLM guessing the target YAML syntax — producing
output that looks plausible but may not deploy or compute correctly.

#### `pbi-schema/` — intentionally NOT vendored
`pbi-schema/` (`SCHEMA.md`, `MQUERY.md`, `SKILL.md`) teaches parsing the raw PBI
`DataModelSchema` and M-queries. Kasal already does this deterministically
(`mquery_parser.py`, `powerbi_semantic_model_fetcher_tool.py`) and feeds the LLM
*structured* measures — the DAX LLM never re-parses raw schema. Out of scope for DAX translation.
(`MQUERY.md` could help the separate m-query LLM path, which is also out of scope here.)

---

## Decisions locked

| Decision | Choice |
|---|---|
| Default translation mode | **`llm_first`** (confirmed 2026-07-09) — regex demoted to a trivial fast-path |
| Skill-file management | **Manual copy** — no sync script, no provenance header; re-copy by hand when desired |
| Canonical source | Engineering **CLI** skills (cleaner/more complete than the webapp `skills/migration/` copy) |
| Scope | DAX measure translation only |
| Branch | **`feat/pbi-ucmv-fixes-v2`** (current) — no new branch |
| Cost guardrail | **Unbounded** — rely on verified prompt caching (~10% after first call) + run-scoped cache |

> **Drift note (accepted):** the vendored `.md` files will go stale as engineering updates theirs.
> A `sync_skills.py` script + provenance headers were considered and declined for now; can be
> added later if staleness becomes a problem.

---

## Implementation steps

### Step 1 — Vendor the knowledge corpus
Copy 10 files verbatim (2 groups) into a new directory:

```
src/backend/src/engines/crewai/tools/custom/metric_view_utils/skills/
  dax/
    SKILL.md
    PATTERNS.md
    UNSUPPORTED.md
    EDGE_CASES.md
  uc-metric-views/
    SYNTAX.md
    WINDOW.md
    LOD.md
    JOINS.md
    COMPOSABILITY.md
    MATERIALIZATION.md
```

`pbi-schema/` is intentionally excluded (kasal parses schema deterministically — see Reference).

### Step 2 — Rewrite `dax_llm_fallback.py` → `dax_llm_translator.py`
- Replace the terse `_SYSTEM_PROMPT` with one that **loads and embeds all 10 `.md` files** at
  module load (from `skills/dax/` + `skills/uc-metric-views/`), mirroring engineering's skill
  loading. The DAX skills teach *what to translate*; the uc-metric-views skills teach *how to
  write the target YAML* — the LLM needs both to emit deployable, correct output.
- Emit the **7-category classification** per measure (translatable-direct / composed / filtered /
  architecture-change / display-layer / unsupported / out-of-scope) instead of binary
  translatable/not.
- **Preserve** the scaffolding that already works well:
  - run-scoped cache (prevents cross-tenant leakage),
  - bounded concurrency (`_DAX_LLM_CONCURRENCY = 6`),
  - fail-open behavior (LLM errors never block a measure),
  - `_validate_sql` DAX-leak guard,
  - cross-chunk reference merging (later chunks resolve earlier chunks' `MEASURE()` refs).

### Step 3 — Flip the cascade in `table_processor.process_table()`
- `DaxTranslator.translate()` → **fast-path only** for trivial aggregations
  (`SUM` / `COUNT` / `AVG` / `DISTINCTCOUNT` on a single column). High-confidence, zero-cost.
- Everything else → **LLM-first primary path** (promote today's Step 5d fallback to primary).
- **Preserve:** 5a window fixups, manual overrides, dependency-graph topological ordering, and
  the DAX-only validation gate.

### Step 3.5 — Skill delivery over DBX model serving (integration design)

**Architecture mismatch to solve:** engineering loads skills *on demand* via the Claude Agent
SDK's `readSkillFile` tool inside an agentic loop. Kasal's translation path is **single-shot**
(`LLMManager.completion()` → litellm → DBX serving endpoint) with **no tool loop**, so on-demand
loading is not available. The ~10-file corpus (~15–20K tokens) must be delivered in one request.

Options considered:

| Option | Approach | Trade-off |
|---|---|---|
| **1. Static embed + prompt caching** ✅ | All 10 `.md` concatenated into the system prompt; mark the stable corpus prefix with Anthropic `cache_control: ephemeral` | Simplest; prompt caching collapses per-call token cost to ~10% after first call. **Chosen.** |
| 2. Router / selective embed | Cheap pre-classifier tags each measure's category, embed only relevant skill sections | Lower tokens/call but fragile; risk of under-supplying context; most effort. Fallback if caching unavailable. |
| 3. Batched translation | N measures per request against one embedded corpus | Amortizes skill tokens but fights the per-measure cache + cross-ref merging; larger blast radius |

**Chosen: Option 1 + Anthropic prompt caching.**
- `_SYSTEM_PROMPT` = the 10 skill files concatenated at module load, as a **stable prefix**.
- Mark that prefix with `cache_control: ephemeral` so DBX serving caches it: full price once per
  run, ~10% on subsequent measures. High hit rate given many measures per run.
- litellm (already the transport in `LLMManager`) passes `cache_control` through to DBX serving.
- Preserves existing scaffolding (run-cache, bounded concurrency, cross-chunk `MEASURE()` refs).

**Prompt caching — VERIFIED on e2 (2026-07-09).** Probed the `databricks-claude-sonnet-4`
serving endpoint on `e2-demo-field-eng` with a 31K-token `cache_control: ephemeral` system
prefix, called twice:

| Field | Call 1 (cold) | Call 2 (warm) |
|---|---|---|
| `cache_creation_input_tokens` | 31,216 | 0 |
| `cache_read_input_tokens` | 0 | 31,216 |
| `prompt_tokens` | 31,237 | 31,237 |

The gateway forwards `cache_control` and returns cache-usage fields. On the warm call the cached
prefix bills at ~10% of input price. **Option 1 is validated; no fallback to Option 2 needed.**
In a real run the ~20K-token skill corpus is written once (first measure) and read from cache on
every subsequent measure. Requirement for the litellm call: pass the corpus as a structured
system block with `cache_control: {"type": "ephemeral"}` (min 1024 tokens to be cacheable — the
corpus far exceeds this).

### Step 3.6 — Make caching actually engage in kasal (BLOCKER found during planning)

**Finding (2026-07-09):** prompt caching works at the HTTP level on e2 (Step 3.5), but the
**current kasal call path defeats it**. Every LLM call — including the one whose docstring claims
"direct HTTP" — routes through `LLMManager.completion()`, which:
1. Accepts `messages: List[Dict[str, str]]` (**plain-string content**) and forwards to CrewAI's
   `LLM.call()`; structured content blocks with `cache_control` are not in that contract and are
   likely flattened/dropped before litellm.
2. **Returns only `str`** — discards the `usage` block, so cache hits are unobservable
   (`llm_converter.py` already hardcodes `"usage": {}` for this reason).

So calling `completion()` as-is → the verified caching **does not fire**; the ~20K-token corpus
bills at full price per measure. This must be fixed for the cost model to hold.

**Two implementation options (decision needed):**

| Option | Approach | Trade-off |
|---|---|---|
| **A. Extend `LLMManager`** | Add a structured-content + usage-returning path (new method or params on `completion()`) that passes `cache_control` through and returns `usage` | Keeps auth/telemetry/tracing centralized; touches shared core (`llm_manager.py`) |
| **B. Direct HTTP in the tool** | New translator POSTs to `/serving-endpoints/{model}/invocations` itself (the pattern the e2 probe used), reusing kasal's auth helper | Isolated to the tool; bypasses `LLMManager` central telemetry/tracing |

**Decision (2026-07-09): Option A — extend `LLMManager`.** Keeps auth/telemetry/MLflow tracing
centralized rather than forking a second transport path.

The extension must: (1) accept structured content blocks and send the corpus as a system block
with `cache_control: {"type": "ephemeral"}`, and (2) return the `usage` block (not just `str`) so
a startup/self-check can assert `cache_read_input_tokens > 0` on the 2nd call.

**Open implementation detail (verify at build time):** confirm that CrewAI's `LLM.call()` — the
layer `completion()` delegates to at `llm_manager.py:778` — preserves structured content blocks
through to litellm. If `LLM.call()` flattens them, the `LLMManager` extension must bypass it for
this path (call litellm directly with the structured payload) while still reusing the centralized
auth/header/tracing setup. This is the one remaining transport risk in Option A.

### Step 3.7 — `category` field: do NOT overload it (resolved during planning)

**Finding (2026-07-09):** `TranslationResult.category` is **already load-bearing for YAML emission**,
not free-form metadata. `yaml_emitter.py` routes measures into output sections by exact value:

```python
# yaml_emitter.py:266-268
base_measures   = [m for m in spec.measures if m.category == 'base']
dax_measures    = [m for m in spec.measures if m.category not in ('base', 'switch_decomposition')]
switch_measures = [m for m in spec.measures if m.category == 'switch_decomposition']
```

`pipeline.py` and `table_processor.py` additionally branch on `cross_table`,
`cross_table_translated`, and `manual_override` for stats/routing. Existing assigned values:
`base`, `single_table`, `cross_table`, `cross_table_translated`, `measure_arithmetic`,
`llm_translated`, `manual_override`, `switch_decomposition`, `calculation_group`, `unassigned`.

**Decision:** the 7-category engineering classification (translatable-direct / composed / filtered /
architecture-change / display-layer / unsupported / out-of-scope) is a **different concept** —
translation provenance/quality, not emission routing. It must **not** be written into `category`.

- Add a **new field** to `TranslationResult`, e.g. `dax_class: str | None`, to hold the 7-category
  label. Keep `category` semantics exactly as today.
- The LLM-first path continues to set `category = 'llm_translated'` (so `yaml_emitter` routes it
  into the DAX-measures section correctly) and additionally sets `dax_class` for
  reporting/telemetry.
- `report_emitter.py` / stats can surface `dax_class`; emission logic stays untouched.

This keeps the emission contract intact while still capturing the richer classification.

### Step 3.8 — Trivial fast-path: exact rule (resolved during planning)

"Trivial fast-path" = the existing `DaxTranslator` patterns that are **single-column, zero-risk,
`confidence='high'`**. Precisely these matchers in the pattern registry (`_register_patterns`):

| Matcher | Regex / logic | Emits |
|---|---|---|
| `_match_quick_reject` | display-only rejects (FORMAT w/o agg, `*_Color`, ISBLANK/BLANK guards, ISFILTERED, SELECTEDVALUE) | untranslatable w/ reason (keep — cheap, avoids wasting LLM tokens on display artifacts) |
| `_match_simple_sum` | `RE_SIMPLE_SUM` = `SUM(tbl[col])` (optionally CALCULATE-wrapped) | `SUM(source.col)`, `confidence='high'` |
| `_match_simple_sumx` | `RE_SIMPLE_SUMX` = `SUMX(tbl, tbl[col])` (no FILTER) | direct, `confidence='high'` |
| `_match_distinctcountnoblank` | `DISTINCTCOUNTNOBLANK(tbl[col])` | `COUNT(DISTINCT source.col)`, `confidence='high'` |

**Rule:** in `llm_first` mode, run only these four matchers as the fast-path. Any measure that
matches → keep the regex result (zero LLM cost). Everything else (all the `CALCULATE`/`FILTER`/
`DIVIDE`/`SUMX+FILTER`/`SAMEPERIODLASTYEAR`/measure-arithmetic patterns) → route to the LLM-first
translator. The remaining ~11 complex matchers are **not** deleted — they simply stop being the
primary path; they remain available if we ever want a `regex_first` mode toggle.

### Step 3.9 — Nested / composed measures: feed the LLM batch in topological order (gap found)

**Context:** nested measures (`[A] / [B]` → `MEASURE(a) / MEASURE(b)`) are supported today via
(1) `dependency_graph.py` (Kahn topo-sort, leaves-first, cycle detection) and (2) the LLM prompt
passing already-translated `base_names` as available `MEASURE()` refs, merged across concurrency
chunks (`dax_llm_fallback.py:296-297`).

**Gap found (2026-07-09):** the LLM batch does **not** run in topological order.
- `table_processor.py` computes `_topo_priority` but only uses it in its own Pass-2 loop
  (lines 323-333). At line 457 it passes the **raw `untranslatable` list** (not topo-sorted) to
  `translate_batch_with_llm`.
- `dax_llm_fallback.py` chunks `candidates` in **input order** (lines 278-279), no re-sort.
- Within a chunk of 6, measures run in parallel and cannot see each other's fresh translations
  (documented at lines 226-227). Cross-chunk refs work; intra-chunk sibling refs do not.

Under regex-first this was low-risk (few measures reached the LLM). Under **llm_first**, most
measures take this path, so a parent+child dependency landing in the **same chunk** becomes likely
— the child fails to resolve its `MEASURE()` ref and is falsely flagged untranslatable. (Not
unsafe — nothing wrong ships, it just caps the nested-measure translation rate.)

**Fix:** sort the LLM batch by `_topo_priority` before chunking so dependents land in later
chunks than their dependencies. Either:
- pass the already-topo-sorted list from `table_processor` into `translate_batch_with_llm`, **or**
- have `translate_batch_with_llm` accept a `topo_priority` map and sort `candidates` by it before
  the chunk loop.

Optionally lower intra-chunk risk further by not placing a known parent and child in the same
chunk. Topological chunk ordering is the primary fix; keep it simple.

### Step 4 — Config plumbing
- Add `translation_mode: 'llm_first' | 'regex_first'` to `llm_config`, **defaulting to `llm_first`**.
- Thread through `uc_metric_view_generator_tool.py` → `pipeline.py` → `table_processor`.
- Keep all I/O async / non-blocking via the existing `run_async` + `LLMManager`.

### Step 5 — Verify (test artifacts in `/tmp` per kasal convention)
- `/tmp` harness runs a sample DAX set through `llm_first` vs `regex_first`, diffs outputs, and
  reports translation-rate + correctness deltas.

---

## Explicitly out of scope
- No changes to the `databricks-eng/universe` repo.
- No changes to the M-query pipeline, `yaml_emitter`, or deployer.
- Regex patterns are **kept** as the fast-path (not deleted).
- M-query LLM side (`mquery_llm_fallback.py`) untouched.

---

## Open items — all resolved (2026-07-09)

1. **Branch** → current `feat/pbi-ucmv-fixes-v2`.
2. **Cost guardrail** → unbounded (rely on prompt caching + run-cache).
3. **Cache transport** → extend `LLMManager` (Option A, Step 3.6).

One residual **build-time check** (not a decision): confirm CrewAI's `LLM.call()` preserves
structured content blocks through to litellm; if it flattens them, the `LLMManager` extension
must call litellm directly for the cached path (see Step 3.6).

---

## Strategic positioning vs. engineering (verified during planning, 2026-07-09)

This plan targets **DAX measure → UCMV SQL translation quality**. On that axis it reaches parity
with engineering (same corpus, same LLM-first architecture) with an edge from kasal's richer
pre-parsed prompt context. On the **broader migration platform**, a code survey found kasal is at
parity-or-ahead across most axes — the earlier assumption that kasal was "metric-views only,
per-measure" was **wrong**. Verified capabilities:

- **Dashboard/visual migration** — `pbi_visual_ucmv_mapper_tool`, `databricks_dashboard_creator_tool`.
- **Genie space migration** — dedicated config-generator + deployer crews (a kasal strength).
- **Live semantic validation** — `metric_view_validator_tool` executes SQL against the warehouse
  and classifies each measure `VALID / EQUIVALENT / REVIEW / INVALID` by comparing DAX↔SQL
  results, not just syntax.
- **Whole-model structural reasoning** — `relationships_loader.py` reads PBI cardinality, orients
  fact→dim, tracks inactive (`USERELATIONSHIP`) and M:N relationships, skips system date tables;
  `join_detector.py` reconstructs the star schema.
- **Full transparency report** — `report_emitter.py` already emits `## Inactive Relationships`,
  `## M:N Relationships`, RLS / incremental-refresh / aggregation-table / perspectives / field-
  parameter warnings, and a `## PBI Native Features — Migration Status` ledger. **This is more
  complete than engineering's** — no work needed here (a candidate "add structural reporting"
  task was investigated and found already implemented).

### The 1:1 comparability bet (deliberate divergence — do NOT close this "gap")

Engineering's Pass-3 "architecture reasoning" (unifying `sales`+`sales_ytd`, domain
decomposition, slicer→dimension rewrites) is **model transformation**. Kasal deliberately does
the opposite: **faithful 1:1 reconstruction** — one metric view per fact table, joins mirroring
PBI relationships, no re-architecture.

These two choices are **coupled**: kasal's live `EQUIVALENT` validation only works *because* the
migrated model maps 1:1 to the source. The moment a model is optimized/unified, row-for-row
equivalence can no longer be proven. Engineering traded provable equivalence for cleaner target
models; **kasal's bet — provably identical numbers — is the better fit for the customer
job-to-be-done (migration acceptance / trust the numbers match).**

**Decision:** do **not** add Pass-3-style optimization to the default path. If ever wanted, it
must be a separate, explicitly-labeled opt-in "optimize" mode that clearly forfeits 1:1
validation — never the default, never coupled to the acceptance-testing migration path.

### Future phase (NOT this plan): data-level equivalence — the real moat

**Current state (verified 2026-07-09):** the validator (`ExpressionValidator`) checks
**structural** equivalence only — it parses both expressions and compares aggregations / filters /
columns via regex/token matching (`DAX_TO_DB_AGG_MAP`, `_compare_aggregations`). `EQUIVALENT`
means "the SQL is structurally faithful to the DAX", **not** "the numbers match". No data is
compared; no warehouse query runs (the `session.execute` calls hit kasal's own trace tables to
fetch measures, not a Databricks warehouse).

**The moat idea (deferred):** add a `DATA_VERIFIED` status that runs each measure on the **source**
(PBI Execute Queries / XMLA — `powerbi_dax_executor_tool.py` already authenticates this) **and**
on the **migrated UC metric view** (Databricks SQL), then diffs results at a defined grain (total
+ grouped by key dimensions, where fan-out / grain bugs surface), with float tolerance. Only the
1:1-reconstruction architecture makes this comparison *possible* — engineering's optimization
forfeits it. This is the guarantee a CFO actually trusts.

**Sequencing (deliberate):** fix transpilation quality FIRST (this plan), then build data
verification on top. Verifying low-quality SQL against source data just surfaces wrong answers
faster — the proof layer is only worth building once the output it checks is trustworthy. Data
verification is a larger, separate effort (source execution + warehouse execution + grain-aware
diffing + tolerance + a new report status) and deserves its own design doc when this plan lands.

---

## Risks & trade-offs (honest assessment)
- **Token cost rises** — every non-trivial measure now hits the LLM. Mitigated by the run-scoped
  cache (identical DAX translated once) and the trivial-aggregation fast-path. A budget ceiling
  (open item #2) would bound worst case.
- **Latency** — bounded concurrency already caps this; large models with hundreds of measures
  previously risked the flow's 10-minute crew timeout. Worth re-measuring under llm_first.
- **Determinism** — LLM output varies run to run; the `_validate_sql` guard + DAX-only gate catch
  leaked DAX constructs, but correctness of *semantics* (e.g. filter context) still needs the
  verification harness (Step 5) to build confidence.
- **Skill staleness** — accepted; manual re-copy when engineering's corpus improves.

---

## Appendix: Skill file transfer reference (monthly re-check list)

Source repo: `databricks-eng/universe`, local checkout at `~/workspace/universe`.
Base path: `ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/`

### Files to transfer (10)

**Group 1 — `dax-translation/` (translation logic):**
```
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/dax-translation/SKILL.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/dax-translation/PATTERNS.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/dax-translation/UNSUPPORTED.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/dax-translation/EDGE_CASES.md
```

**Group 2 — `uc-metric-views/` (target-language spec):**
```
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/uc-metric-views/SYNTAX.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/uc-metric-views/WINDOW.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/uc-metric-views/LOD.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/uc-metric-views/JOINS.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/uc-metric-views/COMPOSABILITY.md
ai_bi_migration/powerbi_migrate/src/powerbi_migrate/skills/uc-metric-views/MATERIALIZATION.md
```

### Intentionally NOT transferred
```
.../skills/pbi-schema/*                    # kasal parses PBI schema/M-query deterministically
.../skills/architecture-patterns/SKILL.md  # out of scope for DAX translation
```

### Destination in kasal
```
src/backend/src/engines/crewai/tools/custom/metric_view_utils/skills/dax/{SKILL,PATTERNS,UNSUPPORTED,EDGE_CASES}.md
src/backend/src/engines/crewai/tools/custom/metric_view_utils/skills/uc-metric-views/{SYNTAX,WINDOW,LOD,JOINS,COMPOSABILITY,MATERIALIZATION}.md
```

### Monthly staleness check

> **Pull universe first**, or you diff against a stale copy. `git pull` in `~/workspace/universe`
> requires the `david-schwarzenbacher_data` GitHub account active
> (`gh auth switch --user david-schwarzenbacher_data`) — it's the only account with repo access.

```bash
cd ~/workspace/universe
# (pull latest master first)
for f in skills/dax-translation/{SKILL,PATTERNS,UNSUPPORTED,EDGE_CASES}.md \
         skills/uc-metric-views/{SYNTAX,WINDOW,LOD,JOINS,COMPOSABILITY,MATERIALIZATION}.md; do
  echo "$(git log -1 --format='%cd' --date=short -- ai_bi_migration/powerbi_migrate/src/powerbi_migrate/$f)  $f"
done
```

Compare the printed dates against your last copy date. Any file with a newer date changed
upstream and should be re-copied.
