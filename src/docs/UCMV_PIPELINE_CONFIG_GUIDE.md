# UC Metric View Pipeline — Config JSON Guide

## What Gets Automated vs. What Needs Human Input

The UCMV pipeline has two phases: **Config Proposer** (automated extraction from PBI APIs) and **UCMV Generator** (metric view generation from config). The Config Proposer auto-fills ~70% of the config. The remaining ~30% requires human domain knowledge and **cannot be fully automated**.

This document explains what needs manual configuration, why, and how to fill it in.

---

## BI Specialist Quick Reference — What You Actually Edit

The extractor auto-fills the joins/columns (~70%). A BI specialist mainly hand-authors the **Measure & Switch Logic**, plus one review pass on the fact joins:

| You edit | Why |
|----------|-----|
| **Switch Decompositions** | Split each `SWITCH(...)` DAX measure into one metric-view measure per branch (the heaviest item — real projects have dozens across multiple fact tables). |
| **Switch Join Column + Switch Join Alias** | Mandatory companions to Switch Decompositions — the dimension column each SWITCH filters on (e.g. `bic_cwc_type` / `dim_wkctr`). Switches don't work without them. |
| **Filter Sets** | The named value-sets the switch branches reference via `num_fs` / `den_fs`. |
| **Measure Resolutions** | Disambiguate/redirect measures to the right physical column or fact. |
| **Manual Overrides** | Hand-written SQL for DAX too complex to auto-translate. |
| **Fact Join Map** (review) | Seeded but usually flagged TODO — needs a human review/confirm pass for cross-fact union/join logic. |

Hands-off (auto, do not touch): **Join Key Map, Enrichment Joins, Dimension Alias Map**. Optional polish only: `column_overrides`, `column_alias_map`, `comment_overrides`, `measure_metadata`, `budget_suffix`.

---

## Config Keys That ARE Automated

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

## Config Keys That CANNOT Be Fully Automated

### 1. `filter_sets`

**What**: Named collections of filter values used in CALCULATE/FILTER expressions.

**Example**:
```json
{
  "CWC_FILTER": ["APET", "CAN", "HDPE FG", "JUICE-BRICK", "NRGB", "PET", "PMX", "RGB", "TNK"],
  "WCT_CORE": ["PET", "APET", "RGB", "JUICE-BRICK", "CAN", "HDPE FG", "NRGB", "CHIPS FG"],
  "WCT_SLE": ["APET", "CAN", "HDPE FG", "PMX", "JUICE-BRICK", "NRGB", "PET", "RGB", "TNK", "CHIPS FG"]
}
```

**Why it can't be automated**: DAX measures reference filter values in two ways:

1. **Inline literals** — `CALCULATE(SUM(...), Table[col] = "PET")` — these CAN be extracted automatically.
2. **Boolean flag columns** — `CALCULATE(SUM(...), Table[CWC_Filter] = 1)` — the flag column `CWC_Filter` is a pre-computed boolean on the dimension table. The actual values it represents (`APET`, `CAN`, `HDPE FG`, ...) live in the **database rows**, not in the DAX expression. You'd need to query the dimension table to resolve `CWC_Filter = 1` into the list of values it maps to.
3. **Named variable references** — `var CWC_List = {"APET","CAN",...}` — these CAN be extracted when the variable is defined inline in the same DAX expression. But when the variable is defined in a shared measure or parameter table, the reference is opaque.

**What to do**: Identify filter columns in dimension tables that act as boolean flags (e.g., `CWC_Filter`, `CWC_Filter2`). Query the dimension table to find which values each flag maps to, and add them as named filter sets.

---

### 2. `switch_decompositions`

**What**: Manual decomposition of `SWITCH(TRUE(), SELECTEDVALUE(...) = "X", expr, ...)` DAX patterns into individual metric view measures.

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

**Why it can't be automated**: SWITCH measures in DAX are **parameterized by slicer context** — the user selects a value from a dropdown (e.g., "Installed Capacity" or "EPL"), and the SWITCH returns a different calculation for each selection. This is a **UI interaction pattern** that has no equivalent in static metric views. To convert it:

- Each SWITCH branch must become a **separate measure** with its own name.
- Each branch's numerator and denominator must be mapped to **physical columns** and **filter sets**.
- The relationship between branches (e.g., "this is a waterfall where each step decomposes the previous") is **business logic** that only the report author understands.

The Config Proposer CAN detect that a `SWITCH(TRUE(), ...)` pattern exists and extract the branch conditions. But it cannot determine: (a) which physical column each branch's `[Measure]` reference maps to, (b) which filter set applies to numerator vs. denominator, or (c) whether the decomposition is a waterfall, a category selector, or a conditional formatter.

**What to do**: For each SELECTEDVALUE+SWITCH measure flagged in the untranslatable report, examine the DAX branches and map each to a `{name, num, num_fs, den, den_fs}` entry. Use `raw_expr` for pre-built SQL when the formula is complex.

---

### 3. `source_table` in `join_key_map`

**What**: The physical 3-level Databricks table name for each dimension table used in joins.

**Example**:
```json
{
  "Dim_wkctr": {
    "alias": "dim_wkctr",
    "join_key": "plant_workcenter_key",
    "source_table": "dc_datalake_prod_001.udm_cchbc_md.ca_dim_workcenter",
    "dim_columns": ["bic_cwc_type", "workcenter_txtmd"]
  }
}
```

**Why it can't be automated**: The PBI Admin Scanner API returns table metadata with M expressions (Power Query), not physical table names. M expressions reference data sources using connection strings, database names, and schema paths that may not directly map to the 3-level UC table name. The translation requires knowing:

- How the data lake is organized (e.g., `dc_datalake_prod_001.udm_cchbc_md` prefix convention).
- Whether the table was imported, DirectQuery, or uses a gateway — each has a different M expression format.
- Whether table names were flattened (e.g., `schema__table` vs. `schema.table`).

The MQuery transpiler extracts source tables for **fact tables** (which have `Value.NativeQuery` with embedded SQL), but dimension tables often use `Sql.Database` or `Sql.Databases` with simple table references that aren't transpiled into SQL.

**What to do**: For each entry in `join_key_map`, add `source_table` with the full 3-level UC table name. Check your data catalog or the flattened table names in your UC schema.

---

### 4. `source_table` and `target_fact` in `fact_join_map`

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
- **`primary_exclude_filter`**: Which rows belong to the primary fact vs. the union arm — this is business logic (e.g., "company codes 0403, 0550, 0307 use KBI-based calculations instead of direct measures").
- **`pivot_col` + `grain`**: How to pivot a narrow/vertical KBI table into wide columns — requires knowing which KBI codes map to which measures.

The Config Proposer can detect that two fact tables reference the same physical source (via scan data), but it cannot determine the union/join strategy, the filter split, or the pivot mapping.

**What to do**: For each cross-fact relationship, determine whether it's a union-mode or join-mode relationship. Specify `source_table`, `target_fact`, and the union/pivot configuration.

---

### 5. `manual_overrides`

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

- **Multi-table aggregations** — DAX `CALCULATE(SUM(TableA[col]), FILTER(TableB, ...))` crossing table boundaries.
- **Row-iteration patterns** — `SUMX(SUMMARIZE(table, dim1, dim2), [measure] * CALCULATE(SUM(col)))`.
- **Geography-routed logic** — `IF(SELECTEDVALUE(Geo[code]) IN {550, 403}, KBI_path, direct_path)`.
- **Complex var chains** — Multi-step variable assignments with conditional logic.

The LLM fallback can translate some of these, but for business-critical measures, a human-verified SQL expression is more reliable.

**What to do**: Review the untranslatable measures report. For each measure that's important for the dashboard, write the equivalent SQL using `source.column`, `FILTER (WHERE ...)`, and `MEASURE()` references.

---

### 6. `switch_join_alias` and `switch_join_col`

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

### 7. `measure_resolutions`

**What**: Disambiguation/redirection of measures whose name or source is ambiguous after extraction — e.g. mapping a measure to the correct physical column, or resolving two measures that collapse to the same name.

**Example**:
```json
{
  "Net Sales": { "column": "net_sales_value", "table": "fact_pe002" },
  "Volume (UC)": { "column": "volume_in_uc", "table": "fact_co012_actuals" }
}
```

**Why it can't be automated**: When a DAX measure references `[Measure]` or a column whose name doesn't match the physical schema (renamed, aliased, or sourced from a different fact), only the report author knows the intended target. The extractor can list the candidates but cannot pick the correct one.

**What to do**: For each flagged measure, point it at the right physical `column` (and `table` when it differs from the owning fact).

---

## Editing in the Pipeline Config Editor (UI)

The config is curated in the **Pipeline Config Editor** during the HITL review step — you do **not** hand-edit JSON in normal use.

**Status colors** (left sidebar):
- 🟢 **auto** — extractor filled it; no action.
- 🟡 **TODO** — seeded but needs human review/completion (e.g. *Fact Join Map*).
- 🔴 **empty** — nothing yet; fill in if your model needs it (e.g. *Switch Decompositions*).

**The sidebar maps 1:1 to config keys**: Join Key Map → `join_key_map`, Fact Join Map → `fact_join_map`, Enrichment Joins → `enrichment_joins`, Dimension Alias Map → `dim_alias_map`, Switch Decompositions → `switch_decompositions`, Measure Resolutions → `measure_resolutions`, Filter Sets → `filter_sets`, Manual Overrides → `manual_overrides`, Switch Join Alias/Column → `switch_join_alias` / `switch_join_col`, Budget Suffix → `budget_suffix`.

**The action buttons (how the config reaches the generator):**
- **Save JSON** — downloads the `config_json` blob (for backup / manual hand-off).
- **Save to Execution** — writes the config straight into the running execution's `config_json` (the production path — no manual upload).
- **Save & Approve Flow** — approves the HITL step; the config flows forward and the UCMV Generator reads it as `config_json`.

**How it wires to the generator (no code change needed):** whatever JSON the editor saves lands in the UCMV Generator tool's `config_json` field (`tool_configs.config_json`, persisted on the task node). At run time the tool parses it and passes it to `MetricViewPipeline(config=...)`, which reads `switch_decompositions`, `switch_join_col/alias`, `filter_sets`, etc. The generator does not care whether the JSON arrived via the editor's *Save to Execution* or a manual upload — the input is identical. The editor's job is simply to produce valid JSON of the shapes documented above.

---

## Summary

| Config Key | Auto-Filled? | Human Effort | Difficulty |
|------------|-------------|--------------|------------|
| `relationships` | Yes | None | - |
| `measures` (DAX) | Yes | None | - |
| `mquery` (SQL) | Yes | None | - |
| `scan_data` | Yes | None | - |
| `column_overrides` | Partial | Low | Easy |
| `join_key_map.source_table` | No | Medium | Look up UC table names |
| `filter_sets` | Partial | Medium | Query dimension tables for flag values |
| `measure_resolutions` | No | Medium | Map ambiguous measures to the right column/fact |
| `switch_decompositions` | No | High | Requires understanding DAX business logic |
| `fact_join_map` | No | High | Requires data architecture knowledge |
| `manual_overrides` | No | High | Requires SQL writing skill |
| `switch_join_alias/col` | No | Low | Identify the SWITCH dimension |

The HITL review step between Config Proposer and UCMV Generator is where this manual enrichment happens, via the Pipeline Config Editor. The Config Proposer flags what's missing (🟡/🔴); the human fills in the gaps and approves.
