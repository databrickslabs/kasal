# PBI → UCMV Pipeline Architecture

How a Power BI semantic model becomes deployable Unity Catalog **Metric Views**
(UCMVs), stage by stage, with the code location of each step. This is the
"how it all fits together" reference — start here before diving into an
individual tool doc (`tool-*.md`).

## The shape of the problem

A Power BI dataset carries two things a metric view needs, in two different
languages:

- **Measures** — business calculations written in **DAX**
  (`DIVIDE(SUM(Sales[Amount]), …)`). These become metric-view **measures**.
- **Table sources** — each table's data source written in **Power Query M**
  (`let Source = Databricks.Catalogs(…) in Source`, or a native `SELECT`). These
  become the metric view's **`source:` SELECT** and its dimensions.

So the pipeline has two translation problems running in parallel — **DAX → Spark
SQL** and **M → source SQL** — feeding one YAML emitter.

## End-to-end flow

```text
Power BI REST / scan
        │
        ▼
1+2. EXTRACTION +     pipeline_config_generator_tool (90)  ── calls the PBI APIs
   CONFIG GENERATION   directly (Admin Scanner / Fabric TMDL, Execute Queries,
   (tool 90)           report definition) and builds the handoff
        │              → measures_json (DAX) + mquery_json (M) + relationships_json
        │                + report bindings; report_references (78) is optional
        │
        ▼
3. UCMV GENERATION     uc_metric_view_generator_tool (86)
   (tool 86)           └─ metric_view_utils/pipeline.py  (MetricViewPipeline)
        │                    ├─ M path    → mquery_parser.py  (+ mquery_llm_fallback)
        │                    ├─ DAX path  → dax_translator.py (+ dax_llm_fallback, skills/)
        │                    ├─ joins     → join_detector / relationships_loader
        │                    ├─ emit YAML → yaml_emitter.py
        │                    └─ report    → report_emitter.py
        ▼
4. VALIDATION          metric_view_validator_tool + metric_view_validation_utils
        │
        ▼
5. DEPLOY              metric_view_deployer_tool (88)  → CREATE METRIC VIEW in UC
```

All source lives under
`src/backend/src/services/tools/`, with shared helpers in
`src/backend/src/services/tools/metric_view_utils/`. Tool numbers match the
`tool-NN-*.md` docs in this folder.

---

## Stage 1 — Extraction

Pulls the model out of Power BI via REST / the scan API.

| What | Tool file | Doc |
|------|-----------|-----|
| Model extraction (tables, measures + DAX, M, relationships) | `pipeline_config_generator_tool.py` | tool-90 |
| Report → measure references | `powerbi_report_references_tool.py` | tool-78 |

**`report_id` is the key quality lever.** When a `report_id` is supplied (or
auto-discovered — `pipeline_config_generator_tool.py::discover_report_id`), the
extractor resolves each measure to its **full DAX body** instead of a bare
column reference. This single input is what moves measure output from
"bare-column stub" to "full-DAX translation." Auth setup is in
[01-authentication-setup.md](./01-authentication-setup.md).

---

## Stage 2 — Config generation (the handoff)

`pipeline_config_generator_tool.py` (tool 90) is the bridge between extraction
and UCMV generation. It builds the config the UCMV crew consumes.

**The critical contract:** the UCMV generator builds views from
**`measures_json`** and **`mquery_json`**, *not* from raw extraction output. If
config-gen doesn't emit these two keys, the UCMV crew produces **zero views**.

- `_build_ucmv_measures()` → `measures_json` (config-gen measures reshaped for UCMV)
- `_build_ucmv_mquery()` → `mquery_json` as `{table_name, transpiled_sql,
  validation_passed}`. Facts need **transpiled SQL** here, not raw M, and
  `validation_passed` must start with `'Yes'` or `mquery_parser` drops the row.

### Optional warehouse + LLM enrichment (additive post-pass)

By default config-gen is deterministic and LLM-free. When the user supplies a
`warehouse_id` (via the "Warehouse + LLM enrichment" toggle on the tool), an
**additive** post-pass runs after `build_config` and before the output is
returned, filling keys the PBI APIs can't supply — never overwriting a
human/derived value, recording each action in an `enrichment_log`:

- **P1** — `join_key_map[dim].source_table` parsed from the connector M-query
  (`mquery_parser.extract_source_table`). Deterministic; runs even without a
  warehouse.
- **P2** — flag-column `filter_sets` via a warehouse `SELECT DISTINCT`
  (`metric_view_utils/uc_query.py` — auth + SSRF allowlist + row-returning query).
  Needs the dim's `source_table` (from P1) + a detected flag column.
- **P3** — `fact_join_map` join strategy: warehouse grain probes + **one** LLM
  call, doubly gated behind `_detect_cross_fact_merge` (≥2 facts sharing a
  conformed dim). Drafts carry a `TODO: verify` suffix — a human confirms.

Warehouse queries use OBO (the signed-in user's token, via
`UserContext.get_user_token()`); failures degrade to a skip note, never abort
config-gen. Full details + the UI walkthrough in
[UCMV_PIPELINE_CONFIG_GUIDE.md](../UCMV_PIPELINE_CONFIG_GUIDE.md).

See [UCMV_PIPELINE_CONFIG_GUIDE.md](../UCMV_PIPELINE_CONFIG_GUIDE.md) for the
full config schema.

---

## Stage 3 — UCMV generation

`uc_metric_view_generator_tool.py` (tool 86) drives
`metric_view_utils/pipeline.py::MetricViewPipeline.run()`, which orchestrates
every module in `metric_view_utils/`. Phases (see `pipeline.py`):

- **Phase 1** — process each table: parse its M source, translate its measures,
  build the spec.
- **Phase 1b** — measure-driven facts (measures whose home table had no SQL fact).
- **Phase 2 / 2b / 2c** — artifact cascade + re-home secondary allocations +
  rebuild the "not emitted" comment blocks.
- **Phase 3** — collect stats (Kasal does no file I/O here; the caller emits).

### 3a. The M-query path (table sources → `source:` SELECT)

`mquery_parser.py` reads the transpiled SQL for each table and extracts source
table, joins, aggregate columns, group-bys, and static filters.

Catalog-browse M (`Databricks.Catalogs(…){[Name=…]}[Data]…`) carries no SQL, so
config generation compiles it deterministically
(`resolve_mquery_to_sql`). That compile only runs when **every** applied step
is on the allowlist in `mquery_steps.py`: the connector call, its navigation,
and the column-only steps `Table.SelectColumns`, `RenameColumns`,
`RemoveColumns`, `ReorderColumns` and `TransformColumnTypes`. Any other step,
such as a row filter (`Table.SelectRows`), a dedup (`Table.Distinct`), a group,
a join or a computed column, makes the table **not resolvable**: the raw M is
shipped instead of a `SELECT *` that would aggregate rows Power BI excluded,
and the skip reason names the refused steps. A native query is never compiled
to `SELECT *`; its own SQL, with its `WHERE` and `GROUP BY`, reaches the parser.
`TransformColumnTypes` stays on the allowlist, but the SQL source does not apply
it: conversions that can change values (whole-number types round, `Currency`
rounds to 4 decimals, `type date` drops the time) are listed per table, with the
Spark SQL that reproduces each one (`CAST(BROUND(c) AS BIGINT)`,
`BROUND(c, 4)`, `CAST(c AS DATE)`, `CAST(c AS BOOLEAN)`; Power Query rounds half
to even, as `BROUND` does), in the run's warnings and under "Power Query Type
Conversions Not Applied" in the migration report. The view itself still reads
the physical table. A dimension's physical table is used for a join only when
its M keeps every row of that table; otherwise the join is not built and the
migration report lists it under "Dimension Joins Not Emitted".

Not every M source is a warehouse table. `classify_mquery_source(mquery)` sorts
a no-SQL source into one of:

| Category | Meaning | Handling |
|----------|---------|----------|
| `inline_const` | base64 `Table.FromRows(Json.Document(Binary.Decompress(…)))` slicer/selector helper | **skip** — no warehouse source |
| `dax_calc` | `GENERATESERIES` / `SUMMARIZECOLUMNS` / `VALUES` / parameter query | **skip** — computed in-model |
| `external` | `Access.Database` / `Excel.Workbook` / dataflow / AAS | needs a customer source mapping |
| `extractable` | `Value.NativeQuery` / `Databricks.*` but no source resolved | **extraction gap — investigate** |
| `unknown` | unrecognized shape | flagged |

Skipped tables are **not dropped silently** — `pipeline.py::_skip_stat()` records
the reason + the original M, and `report_emitter.py` surfaces them under
**"Tables not emitted"** with the M in a fenced block (the table-level analogue
of how untranslatable measures are shown). A genuinely raw-M source that *should*
yield SQL can be routed to `mquery_llm_fallback.py` (M → SQL via the LLM).

### 3b. The DAX path (measures → SQL) — LLM-first with skill files

`dax_translator.py` translates DAX to Spark SQL. It runs in **`llm_first`** mode:

1. **Regex fast-path** (`_TRIVIAL_FAST_PATH`) handles only the trivially-safe,
   deterministic patterns — display-artifact rejects, single-column `SUM`,
   ANSI-safe `DIVIDE`, simple filtered aggregates. Complex/fragile shapes are
   deliberately **excluded** from the fast-path so they don't get botched by
   regex.
2. Everything else routes to the **LLM fallback** (`dax_llm_fallback.py`), which
   loads the **skill corpus** as a system prefix and calls
   `LLMManager.completion_with_usage` (cached, group-scoped auth):

   | Skill file (`metric_view_utils/skills/dax/`) | Role |
   |----------------------------------------------|------|
   | `SKILL.md` | translation rules + output contract |
   | `PATTERNS.md` | worked DAX→SQL patterns (var-chain ratios, join-alias FILTER, share-of-total, ALLEXCEPT, SUMMARIZE-LOD…) |
   | `EDGE_CASES.md` | tricky cases |
   | `UNSUPPORTED.md` | honest-skip list (TREATAS, LOOKUPVALUE, TOPN…) — what to *decline*, not fake |

3. **Correctness guards** — before accepting a translation, the pipeline checks
   for silently-wrong output: `sql_measure_sanitizer.py` and
   `detect_lost_dax_component` catch dropped prior-year windows, DIVIDE
   denominators, exclusions, additive terms, and share-of-total collapse. A
   translation that drops a DAX component is rejected (routed to LLM / skipped)
   rather than emitted wrong.
4. **Filters are all or nothing** (`dax_condition_sql.py`). A `FILTER` or
   `CALCULATE` condition translates only when every `&&` part and every `||`
   arm does, and each pattern must match the whole part. If one part cannot be
   read (for example `Sales[OrderDate] >= DATE(2024,1,1)`), the measure is a
   gap with an "Untranslatable FILTER condition" reason, never the same measure
   with a smaller filter.

Supporting DAX modules: `dependency_graph.py` (topological order so base
measures translate before the ratios that reference them),
`join_detector.py` + `relationships_loader.py` (auto-joins from DAX dimension
refs / PBI relationships), `metadata_generator.py` (display names, synonyms,
formats).

### 3c. Emit

`yaml_emitter.py` writes the UC Metric View YAML. Anything not translatable is
preserved in a **"not emitted" comment block** carrying the original DAX and a
categorized reason, so nothing disappears without a trace. `report_emitter.py`
writes the human migration report (per-table results, join map, untranslatable
measures, tables-not-emitted, native-feature status).

---

## Stage 4 — Validation

`metric_view_validator_tool.py` + `metric_view_validation_utils/` check the
generated YAML/SQL (expression validation, base-measure classification) before
deploy.

Statuses mean:

| Status | Meaning |
|--------|---------|
| `VALID` | The component checks found no difference, and the canonical forms do not contradict them |
| `EQUIVALENT` | Written differently, but **proven** the same: both sides reduce to the same fingerprint (`measure_fingerprint.py`): aggregation kind, aggregated expression, every filter conjunct and the ratio shape, with identifiers compared case- and punctuation-insensitively |
| `REVIEW` | Some parts match but equivalence is not proven, for example a missing filter conjunct, a different filter value or column, a dropped denominator, or a shape the fingerprint cannot read (VAR blocks, `ALL`, window functions). These go on the attention list |
| `INVALID` | Nothing matches |

A plain aggregate (`SUM(source.col)`) counts as correct by construction only
when there is no DAX measure to compare it with.

## Stage 5 — Deploy

`metric_view_deployer_tool.py` (tool 88) issues the `CREATE METRIC VIEW` against
Unity Catalog. The Genie-facing config is assembled by
`ucmv_genie_config_generator_tool.py`.

---

## Where to look when…

| You want to… | Go to |
|--------------|-------|
| Understand a single tool | `tool-NN-*.md` in this folder |
| Change how a DAX pattern translates | `skills/dax/PATTERNS.md` (corpus) or `dax_translator.py` (fast-path) |
| Add an "honest skip" for an unsupported DAX function | `skills/dax/UNSUPPORTED.md` |
| Change M-source classification / skip reasons | `mquery_parser.py::classify_mquery_source` |
| Trace why a table produced no view | `report_emitter.py` "Tables not emitted" + `pipeline.py::_skip_stat` |
| Fix the config→UCMV handoff (0 views) | `pipeline_config_generator_tool.py` `_build_ucmv_measures` / `_build_ucmv_mquery` |
| Understand measure quality vs `report_id` | Stage 1 above + [ucmv-migration-guide.md](./ucmv-migration-guide.md) |

Tests for the generation stage live in
`src/backend/tests/unit/services/tools/metric_view_utils/`.
