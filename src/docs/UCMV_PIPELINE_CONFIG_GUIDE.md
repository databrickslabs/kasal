# UC Metric View pipeline config JSON guide

Reference for every config key in the UCMV pipeline: which keys are auto-extracted from Power BI APIs and which require human domain knowledge.

## What gets automated vs. what needs human input

The UCMV pipeline has two phases: **Config Proposer** (automated extraction from PBI APIs — Tool 90, `pipeline_config_generator_tool`) and **UCMV Generator** (metric view generation from config — Tool 86). The Config Proposer auto-fills the large majority of the config, and now **auto-derives `switch_decompositions` and `filter_sets`** from a DAX scan (see [Recent automation](#recent-automation-what-changed)). What remains is a small set of keys that depend on **physical data-layout and cross-fact architecture knowledge** that simply is not present in the PBI APIs.

This document explains what still needs manual configuration, why, and — in [Entering manual config in the UI](#entering-manual-config-in-the-ui) — exactly where to type it.

---

## Config keys that are automated

These are extracted directly from PBI APIs (Admin Scanner, Execute Queries, XMLA):

| Config Key | Source | What It Contains |
|------------|--------|------------------|
| `relationships` | `INFO.VIEW.RELATIONSHIPS()` | Table relationships (1:N, M:N, inactive) |
| `measures` | `$SYSTEM.MDSCHEMA_MEASURES` | DAX expressions for all measures |
| `mquery` | Admin Scanner API | MQuery transpiled SQL for each table |
| `scan_data` | Admin Scanner API | Full M expressions, column metadata, storage mode |
| `column_overrides` | Extracted from DAX | Physical column name remappings |
| `dim_alias_map` | Derived from relationships | PBI table name to SQL alias mapping |

---

## Config keys that cannot be fully automated

### filter_sets  *(now mostly auto-derived)*

**What**: Named collections of filter values used in CALCULATE/FILTER expressions.

**Example**:
```json
{
  "CWC_FILTER": ["APET", "CAN", "HDPE FG", "JUICE-BRICK", "NRGB", "PET", "PMX", "RGB", "TNK"],
  "WCT_CORE": ["PET", "APET", "RGB", "JUICE-BRICK", "CAN", "HDPE FG", "NRGB", "CHIPS FG"]
}
```

**What's automated now**: The Config Proposer scans DAX and auto-populates
`filter_sets` from both **inline literals** (`CALCULATE(SUM(...), Table[col] = "PET")`)
and **inline value lists** (`Table[col] IN {"APET","CAN",...}`), *and* it harvests
the value lists referenced by the auto-derived `switch_decompositions`. Code:
`pipeline_config_generator_tool._auto_enrich_from_dax` (§1) and
`generate_config.derive_filter_sets`. In practice most filter sets now arrive pre-filled.

**The remaining manual case — boolean flag columns**: When DAX filters on a
pre-computed flag (`CALCULATE(SUM(...), Table[CWC_Filter] = 1)`), the flag is a
boolean on the dimension table. The actual values it maps to (`APET`, `CAN`, ...)
live in the **database rows**, not the DAX, so they cannot be read from the API.
(A named variable defined in a *shared* measure or parameter table is opaque for
the same reason — the value list isn't in the referencing expression.)

**What to do**: If a flag-based filter set is missing or shows as a `TODO`, query
the dimension table to find which values the flag maps to and add them as a named set.

---

### switch_decompositions  *(now auto-derived)*

**What**: Decomposition of `SWITCH(TRUE(), SELECTEDVALUE(...) = "X", expr, ...)` DAX
patterns into individual metric view measures (the slicer dropdown becomes N measures).

**Example**:
```json
{
  "fact_pe002": [
    {
      "name": "sle_waterfall_installed_capacity",
      "num": "total_hours",
      "num_fs": "WCT_CORE",
      "den": "total_hours",
      "den_fs": "WCT_CORE",
      "comment": "SLE Waterfall: Installed Capacity = 1.0 (100% baseline)"
    },
    {
      "name": "sle_waterfall_epl",
      "num": "epl",
      "num_fs": "CWC_FILTER",
      "den": "paid_hours",
      "den_fs": "WCT_CORE",
      "comment": "SLE Waterfall: EPL (CWC_FILTER) / paid time (WCT_CORE)"
    }
  ]
}
```

**What's automated now**: This is no longer a hand-authored key. The Config
Proposer decomposes SELECTEDVALUE+SWITCH measures into individual `{name, num,
num_fs, den, den_fs}` entries automatically — including the geo/plant/company
selector variant (`ISFILTERED`/`HASONEVALUE`) — emitting **real SQL branches, not
skeletons**. Code: `generate_config.derive_switch_decompositions` +
`derive_geo_switch_decompositions`, plus `_auto_enrich_from_dax` (§ SWITCH).

**The residual manual case**: A branch whose `[Measure]` reference the proposer
can't resolve to a physical column, or a decomposition whose *intent* (waterfall
vs. category-selector vs. conditional formatter) changes the math, may surface as
a `TODO`. Review those branches and set `num`/`den`/`*_fs`, or drop in a `raw_expr`
with pre-built SQL when the formula is complex. Most models need no edits here.

---

### source_table in join_key_map

**What**: The physical 3-level Databricks table name for each dimension table used in joins.

**Example**:
```json
{
  "Dim_wkctr": {
    "alias": "dim_wkctr",
    "join_key": "plant_workcenter_key",
    "source_table": "dc_datalake_prod_001.udm_example_md.ca_dim_workcenter",
    "dim_columns": ["bic_cwc_type", "workcenter_txtmd"]
  }
}
```

**Why it can't be automated**: The PBI Admin Scanner API returns table metadata with M expressions (Power Query), not physical table names. M expressions reference data sources using connection strings, database names, and schema paths that may not directly map to the 3-level UC table name. The translation requires knowing:

- How the data lake is organized (e.g., `dc_datalake_prod_001.udm_example_md` prefix convention).
- Whether the table was imported, DirectQuery, or uses a gateway; each has a different M expression format.
- Whether table names were flattened (e.g., `schema__table` vs. `schema.table`).

The MQuery transpiler extracts source tables for **fact tables** (which have `Value.NativeQuery` with embedded SQL), but dimension tables often use `Sql.Database` or `Sql.Databases` with simple table references that aren't transpiled into SQL.

**What to do**: For each entry in `join_key_map`, add `source_table` with the full 3-level UC table name. Check your data catalog or the flattened table names in your UC schema.

---

### source_table and target_fact in fact_join_map

**What**: Cross-fact join configuration for union-mode or source-embed joins.

**Example**:
```json
{
  "fact_scorecard_Actuals_wc": {
    "alias": "sc_actuals",
    "source_table": "dc_datalake_prod_001.curated_cch_bw_mdm.ca_kbi52r_maa_sub_kbis_rt",
    "target_fact": "fact_pe002",
    "union_mode": true,
    "primary_exclude_filter": "comp_code NOT IN ('0403', '0550', '0307')",
    "union_key_expr": "CONCAT(plant, '/', workcenter) AS plant_workcenter_key",
    "pivot_col": "bic_csubkbi",
    "grain": ["plant", "workcenter", "fiscper", "comp_code"]
  }
}
```

**Why it can't be automated**: Cross-fact joins represent **business relationships between tables** that aren't declared in the PBI data model. For example, fact_pe002 (line performance actuals) and fact_scorecard_Actuals_wc (KBI scorecard) share the same physical source table but with different WHERE filters and pivoting logic. This is a data architecture decision made by the report builder:

- **`union_mode`**: Whether to UNION ALL the two tables (same grain, different measures) vs. LEFT JOIN (different grain).
- **`primary_exclude_filter`**: Which rows belong to the primary fact vs. the union arm. This is business logic (e.g., "company codes 0403, 0550, 0307 use KBI-based calculations instead of direct measures").
- **`pivot_col` + `grain`**: How to pivot a narrow/vertical KBI table into wide columns. This requires knowing which KBI codes map to which measures.

The Config Proposer can detect that two fact tables reference the same physical source (via scan data), but it cannot determine the union/join strategy, the filter split, or the pivot mapping.

**What to do**: For each cross-fact relationship, determine whether it's a union-mode or join-mode relationship. Specify `source_table`, `target_fact`, and the union/pivot configuration.

---

### manual_overrides

**What**: Hand-written SQL expressions for measures where automated DAX-to-SQL translation fails.

**Example**:
```json
{
  "FT_BPC003": [
    {
      "name": "cost_supply_per_uc_actual",
      "expr": "SUM(source.value) FILTER (WHERE source.bic_chversion = '0000' AND source.fis_code_parent IN ('DCD2','DHF2')) / NULLIF(SUM(co012.volume_in_uc), 0)",
      "comment": "Cost to Supply per Unit Case (Actuals)"
    }
  ]
}
```

**Why it can't be automated**: These are measures where the DAX pattern is too complex for pattern-based translation:

- **Multi-table aggregations**: DAX `CALCULATE(SUM(TableA[col]), FILTER(TableB, ...))` crossing table boundaries.
- **Row-iteration patterns**: `SUMX(SUMMARIZE(table, dim1, dim2), [measure] * CALCULATE(SUM(col)))`.
- **Geography-routed logic**: `IF(SELECTEDVALUE(Geo[code]) IN {550, 403}, KBI_path, direct_path)`.
- **Complex var chains**: Multi-step variable assignments with conditional logic.

The translator now runs **LLM-first with a skill corpus** (see the [pipeline
architecture doc](./powerbi/ucmv-pipeline-architecture.md#3b-the-dax-path-measures--sql--llm-first-with-skill-files))
plus correctness guards that reject silently-wrong output, so the set of measures
that land here is **smaller than it used to be** — var-chain ratios, join-alias
FILTER, share-of-total, ALLEXCEPT and SUMMARIZE-LOD patterns are handled
automatically. `manual_overrides` is now the fallback for the genuinely hardest,
business-critical measures where you want a human-verified SQL expression.

**What to do**: Review the "Untranslatable Measures" section of the migration
report. For each measure important to the dashboard that's still listed, write the
equivalent SQL using `source.column`, `FILTER (WHERE ...)`, and `MEASURE()` references.

---

### switch_join_alias and switch_join_col

**What**: The default dimension join alias and column used for SWITCH decomposition FILTER clauses.

**Example**:
```json
{
  "switch_join_alias": "dim_wkctr",
  "switch_join_col": "bic_cwc_type"
}
```

**Why it can't be automated**: This is a global default that tells the SWITCH builder which dimension table to filter on. Different PBI models may use different dimension tables for SWITCH parameterization. The Config Proposer can detect SELECTEDVALUE references but cannot determine which one is the "primary" SWITCH dimension without understanding the report's visual layout and slicer configuration.

---

## Summary

| Config Key | Auto-Filled? | Human Effort | Difficulty |
|------------|-------------|--------------|------------|
| `relationships` | Yes | None | - |
| `measures` (DAX) | Yes | None | - |
| `mquery` (SQL) | Yes | None | - |
| `scan_data` | Yes | None | - |
| `column_overrides` | Partial | Low | Easy |
| `switch_decompositions` | **Yes** (auto-derived) | Rare | Only residual unresolved branches |
| `filter_sets` | **Mostly** (auto-derived) | Low | Only boolean-flag columns |
| `switch_join_alias/col` | Partial | Low | Identify the SWITCH dimension |
| `manual_overrides` | No (smaller set now) | Medium | SQL for the hardest measures only |
| `join_key_map.source_table` | No | Medium | Look up UC table names |
| `fact_join_map` | No | High | Requires data-architecture knowledge |

The HITL review step between Config Proposer and UCMV Generator is where this
enrichment happens. The Config Proposer fills what it can and marks unresolved
spots with a `TODO` marker; the human fills in the remaining gaps in the UI.

### The irreducible manual core

After the recent automation, the keys that genuinely **cannot** come from the PBI
APIs — because they encode physical data-layout and cross-fact architecture, not
model metadata — are:

1. **`join_key_map.*.source_table`** — the physical 3-level UC table name per dimension.
2. **`fact_join_map`** — cross-fact union/join + pivot strategy.
3. **`filter_sets` for boolean-flag columns** — the `Flag = 1 → [values]` mapping that lives in DB rows.
4. **`manual_overrides`** — hand-verified SQL for the hardest business-critical measures.

Everything else is auto-filled or auto-derived.

## Recent automation (what changed)

Earlier revisions of this guide listed `switch_decompositions` as "High effort,
manual" and `filter_sets` as broadly manual. That is **no longer accurate** — both
are now auto-derived by the Config Proposer:

- **`switch_decompositions`** → `generate_config.derive_switch_decompositions` +
  `derive_geo_switch_decompositions` (plant/company selectors), emitting real SQL
  branches.
- **`filter_sets`** → `generate_config.derive_filter_sets` +
  `pipeline_config_generator_tool._auto_enrich_from_dax`, harvesting inline
  literals, `IN {…}` lists, and the values used by the switch branches.
- **DAX translation** is LLM-first with a skill corpus + correctness guards, so
  fewer measures fall through to `manual_overrides`.

Treat the summary table above as the current state.

## Entering manual config in the UI

You do **not** hand-write `pipeline_config.json`. The flow is:

**1. Generate the config (Pipeline Config Generator — Tool 90).**
In the pipeline/flow, the Pipeline Config Generator node takes only *connection*
inputs — Workspace ID, Dataset ID, `report_id` (supply it for best measure
quality — it resolves full-DAX bodies instead of bare columns), and the two
credential sets. Run it; it calls the PBI APIs and emits the full config with all
26 keys, auto-deriving `switch_decompositions`/`filter_sets` and marking anything
unresolved with a `TODO`.

**2. Review and fill gaps in the Config Editor (`/config-editor`).**
Open the generated config in the Config Editor. The left sidebar lists every key
with a color-coded status badge (`getKeyStatus` in `types/configEditor.ts`):

| Badge | Meaning |
|-------|---------|
| `AUTO` | Filled by the proposer — usually leave as is |
| `TODO` | Value contains a `TODO` marker — **needs your input** |
| `EMPTY` | Empty object/list — fill if relevant to your model |
| `NULL` | Explicitly null — set a value if needed |

Scan for `TODO` and `EMPTY` badges first — that's the irreducible core above.
Click a key to edit it, either with the per-entry **form view** or the **raw
JSON** toggle (paste a whole object and click *Apply JSON*). Keys that ship a
literal `TODO` string when unresolved include `switch_join_alias` /
`switch_join_col`, `budget_suffix`, a `fact_join_map` grain decision, and any
measure `base_expr` the proposer couldn't translate.

**3. Example — adding `source_table` to a `join_key_map` entry.**
The proposer builds each dimension entry with alias/join_key/dim_columns but
**omits `source_table`** (the physical UC name isn't in the PBI M expression — it
can't be inferred, so the key is simply left out rather than guessed):

```json
// BEFORE (as generated — no source_table key)
"join_key_map": {
  "Dim_wkctr": {
    "alias": "dim_wkctr",
    "join_key": "plant_workcenter_key",
    "dim_columns": ["bic_cwc_type", "workcenter_txtmd"]
  }
}
```

In the Config Editor, open **Join Key Map**, switch to raw JSON, and add
`source_table` with the real catalog.schema.table:

```json
// AFTER (what you type)
"join_key_map": {
  "Dim_wkctr": {
    "alias": "dim_wkctr",
    "join_key": "plant_workcenter_key",
    "source_table": "your_catalog.your_schema.dim_workcenter",
    "dim_columns": ["bic_cwc_type", "workcenter_txtmd"]
  }
}
```

Click *Apply JSON*. Same pattern for a flag-based `filter_sets` entry (add the
resolved value list), a `switch_join_alias`/`switch_join_col` that shows a `TODO`
(replace with the real dimension alias/column), or a `manual_overrides` measure
(add `{name, expr, comment}`). When no `TODO`/`EMPTY` badges remain for keys your
model uses, download the config.

**4. Feed the reviewed config to the UCMV Generator (Tool 86).**
In JSON mode, paste the reviewed config into the generator's config input; it
builds the metric views from `measures_json` + `mquery_json` and your enrichment.

## See also

- [Power BI tools reference](./powerbi/README.md): the tools that extract and translate PBI metadata
- [Example crews and flows](./examples/README.md): importable UCMV pipeline definitions
- [PBI → UCMV pipeline architecture](./powerbi/ucmv-pipeline-architecture.md): end-to-end walkthrough of the extraction → config → generation → deploy flow, with the code location of each stage

Back to the [documentation hub](./README.md).
