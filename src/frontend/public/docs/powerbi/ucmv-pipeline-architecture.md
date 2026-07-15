# PBI → UCMV Pipeline Architecture

How a Power BI semantic model becomes deployable Unity Catalog **Metric Views**
(UCMVs), stage by stage, with the code location of each step. This is the
"how it all fits together" reference — start here before diving into an
individual tool doc (`tool-72-*.md` … `tool-90-*.md`).

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

```
Power BI REST / scan
        │
        ▼
1. EXTRACTION          powerbi_semantic_model_fetcher_tool (79), _dax_tool,
   (tools 72, 78–81)   powerbi_metadata_reducer_tool (81), report_references (78)
        │              → raw measures (DAX), tables (M), relationships, report bindings
        ▼
2. CONFIG GENERATION   pipeline_config_generator_tool (90)  ── the handoff builder
   (tool 90)           → emits UCMV config: measures_json + mquery_json + report_id
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
`src/backend/src/engines/crewai/tools/custom/`. Tool numbers match the
`tool-NN-*.md` docs in this folder.

---

## Stage 1 — Extraction

Pulls the model out of Power BI via REST / the scan API.

| What | Tool file | Doc |
|------|-----------|-----|
| Semantic-model fetch (tables, measures, M) | `powerbi_semantic_model_fetcher_tool.py` | tool-79 |
| DAX per measure | `powerbi_semantic_model_dax_tool.py` | tool-80 |
| Metadata reduce (trim to what matters) | `powerbi_metadata_reducer_tool.py` | tool-81 |
| Report → measure references | `powerbi_report_references_tool.py` | tool-78 |
| Relationships | `powerbi_relationships_tool.py` | tool-75 |

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
`src/backend/tests/unit/engines/crewai/tools/custom/metric_view_utils/`.
