# End-to-End UCMV Migration Guide

Migrate a Power BI semantic model to Databricks Unity Catalog Metric Views - step by step.

---

## What You'll Have at the End

For each Power BI fact table, a Unity Catalog Metric View that can be queried with standard SQL:

```sql
-- Before (Power BI, DAX):
[Total Revenue] := SUM(Fact_Sales[Amount])
[YoY Growth] := CALCULATE([Total Revenue], SAMEPERIODLASTYEAR('Date'[Date]))

-- After (Databricks, UC Metric View):
SELECT
  MEASURE(Total_Revenue),
  MEASURE(YoY_Growth)
FROM my_catalog.metrics.fact_sales_uc_metric_view
GROUP BY Region, Quarter
```

Any BI tool (including Power BI) can query UC Metric Views. The business logic now lives in Databricks, governed by Unity Catalog.

---

## Prerequisites

- [ ] Non-Admin SP configured ([Authentication Setup](./01-authentication-setup.md))
- [ ] Admin SP configured ([Authentication Setup](./01-authentication-setup.md))
- [ ] Kasal running (backend + frontend)
- [ ] Target Databricks workspace with UC enabled
- [ ] Power BI Workspace ID and Dataset ID for the model to migrate

---

## Overview: The 5-Phase Pipeline

```
Phase 1: EXTRACT (automatic, ~5 minutes)
Tool 74: M-Query → source SQL per table
Tool 73: Measures → DAX expressions
Tool 75: Relationships → FK structure (optional, recommended)
    ↓
Phase 2: PROPOSE CONFIG (automatic, ~2 minutes)
Tool 90: Call 4 PBI APIs → proposed pipeline_config.json (26 keys)
    ↓
Phase 3: HUMAN REVIEW (manual, 2-3 hours first time, 30 min for similar models)
SA fills TODO markers in config:
  - switch_decompositions (biggest effort)
  - filter_sets, measure_resolutions, mapping_only_tables
    ↓
Phase 4: GENERATE (automatic, ~30 seconds)
Tool 87: Allocate measures to fact tables (if needed)
Tool 86: Full pipeline → YAML + SQL per fact table + migration report
    ↓
Phase 5: VALIDATE + DEPLOY
Tool 88: Dry-run validation → human approval → live deployment
```

Failures in Phase 5 loop back to Phase 3 (adjust config, re-run Phase 4).

---

## Phase 1: Extract

Run these three tasks in Kasal. They can run in parallel or sequentially.

### Task A - Extract M-Query (Tool 74)
Requires Admin SP.
```
workspace_id: your-workspace-guid
dataset_id: your-dataset-guid
tenant_id: your-tenant-guid
client_id: ADMIN_SP_client_id
client_secret: ADMIN_SP_client_secret
```

### Task B - Extract Measures (Tool 73)
Requires Non-Admin SP.
```
workspace_id: your-workspace-guid
dataset_id: your-dataset-guid
tenant_id: your-tenant-guid
client_id: NON_ADMIN_SP_client_id
client_secret: NON_ADMIN_SP_client_secret
outbound_format: uc_metrics
```

### Task C - Extract Relationships (Tool 75)
Requires Non-Admin SP.
```
workspace_id: your-workspace-guid
dataset_id: your-dataset-guid
tenant_id: your-tenant-guid
client_id: NON_ADMIN_SP_client_id
client_secret: NON_ADMIN_SP_client_secret
```

**Save the JSON outputs** from each task - you'll need them for Phase 4.

---

## Phase 2: Propose Config

Run Tool 90. This calls all 4 PBI APIs and produces the initial `pipeline_config.json`.

```
workspace_id: your-workspace-guid
dataset_id: your-dataset-guid
tenant_id: your-tenant-guid
client_id: NON_ADMIN_SP_client_id
client_secret: NON_ADMIN_SP_client_secret
admin_client_id: ADMIN_SP_client_id
admin_client_secret: ADMIN_SP_client_secret
report_id: your-report-guid  (optional, adds synonym metadata)
catalog: my_catalog
schema_name: metrics
```

Download the proposed config from the result. It will have `TODO` markers where human input is needed.

---

## Phase 3: Human Review

This is the most important phase. Open the proposed config and work through the TODOs:

### Priority Order (biggest translation rate unlock first)

**1. `switch_decompositions`** - largest impact
SWITCH statements in DAX often represent dimension-based branching (e.g. "if this is an HR measure, use this SQL; if Finance, use that SQL"). The SA provides the SQL for each branch.

```json
"switch_decompositions": {
  "Fact_HR_A": {
    "HC_Headcount": {
      "num": "SUM(source.headcount)",
      "den": null,
      "filters": ["source.employee_type = 'FTE'"]
    }
  }
}
```

**2. `measure_resolutions`** - cross-table measure references
When a measure in one fact table references a measure defined in another table, the SA provides the base SQL expression.

```json
"measure_resolutions": {
  "[Fact_B Revenue]": {
    "base_expr": "SUM(source_b.revenue)",
    "base_filters": []
  }
}
```

**3. `filter_sets`** - reusable CALCULATE filter groups
Named filter sets that multiple CALCULATE measures reference.

```json
"filter_sets": {
  "CORE_FILTER": ["source.status IN ('Active', 'Pending')"],
  "YTDA_FILTER": ["source.fiscal_year = YEAR(CURRENT_DATE)"]
}
```

**4. `mapping_only_tables`** - pure-mapping fact tables
Tables that just map keys to dimensions (no aggregatable measures).

```json
"mapping_only_tables": {
  "Fact_HR_B": {
    "source_table": "my_catalog.raw.fact_hr_b",
    "dimensions": ["employee_id", "cost_center"],
    "aggregate_columns": []
  }
}
```

### What to Skip

- `join_key_map` - auto-filled by Tool 90, review only
- `column_metadata` - auto-filled, usually no changes needed
- `dimension_exclusions` - auto-filled, verify the table name is correct

### Estimating Effort

| Model Complexity | First Migration | Repeat (similar model) |
|-----------------|-----------------|----------------------|
| Simple (<50 measures, few SWITCH) | 30-60 minutes | 15-30 minutes |
| Medium (100-300 measures) | 2-3 hours | 45-90 minutes |
| Complex (300+ measures, many SWITCH) | 4-6 hours | 2-3 hours |

---

## Phase 4: Generate

Run Tool 86 with the completed config. If measures don't have `proposed_allocation` yet, run Tool 87 first.

**Tool 87 (if needed):**
```
measures_json: [from Tool 73 output]
mquery_json: [from Tool 74 output]
```

**Tool 86:**
```
measures_json: [from Tool 87 output or Tool 73 directly]
mquery_json: [from Tool 74 output]
relationships_json: [from Tool 75 output]
config_json: [your completed pipeline_config.json]
catalog: my_catalog
schema_name: metrics
```

### Reviewing the Migration Report

Tool 86 produces a migration report. Key things to check:

```
Fact Table: fact_sales
  Total measures: 45
  Translated: 40 (88.9%)
  Untranslatable: 5
    - [Color_Flag]: FORMAT pattern - PBI display artifact, skip
    - [Dynamic_Switch]: SELECTEDVALUE+SWITCH - add to switch_decompositions
    - [Cross_Table_Ref]: References [Fact_HR_A Revenue] - add to measure_resolutions
```

**If translation rate is below your target:** go back to Phase 3, fix the flagged items, re-run Tool 86. Each iteration takes ~30 seconds.

### Typical Iteration Count

- First run: ~60-70% translation rate (config needs more detail)
- After switch_decompositions: ~75-80%
- After measure_resolutions: ~85-90%
- With LLM fallback enabled: ~90-95%

---

## Phase 5: Validate + Deploy

### Step 1: Dry Run (Tool 88)
```
yaml_specs_json: [from Tool 86 output]
sql_specs_json: [from Tool 86 output]
catalog: my_catalog
schema_name: metrics
dry_run: true
```

Review the validation report. All metric views should show `status: validated`.

### Step 2: Customer Review
Share the YAML and migration report with the customer. Get approval on:
- Measure names and SQL expressions
- Join structures
- Translation rate and list of skipped measures

### Step 3: Live Deploy (Tool 88)
After customer approval:
```
dry_run: false
databricks_host: https://xyz.cloud.databricks.com
databricks_token: your-databricks-pat
warehouse_id: your-warehouse-id
```

### Step 4: Smoke Test
After deployment, verify with:
```sql
SHOW METRIC VIEWS IN my_catalog.metrics;

SELECT MEASURE(Total_Revenue), MEASURE(YoY_Growth)
FROM my_catalog.metrics.fact_sales_uc_metric_view
GROUP BY Region
LIMIT 10;
```

---

## Running the Demo (No PBI Credentials Needed)

The fastest way to demo the pipeline is the **BI Specialist workspace** built into Kasal.
On startup, Kasal seeds 9 pre-configured crew templates covering the full pipeline:

1. Open Kasal → **Workspaces** → switch to **BI Specialist**
2. Go to **Crews** — the UCMV Generation Pipeline crew is already there
3. Build a flow by connecting crews on the **Flows** canvas
4. Run it against your own data or use the pre-configured demo inputs

No Python setup, no backend access required.

---

## Iteration Tips

- **Re-run Tool 86 as many times as needed** — it's deterministic and fast (~30 seconds)
- **LLM fallback** (`use_llm_fallback: true`) can handle complex SWITCH and cross-table patterns that regex rules miss — use it after exhausting config-based improvements
- **Each fact table is independent** — if one fails validation, the others still deploy; fix and re-run that table alone
- **Tool 89** (gap analysis) shows which config keys would unlock the most additional measure translations

---

## Example Crews (JSON)

Ready-to-import crew definitions are in [`src/docs/examples/`](../docs/examples/):

| File | Description |
|------|-------------|
| `crew_ucmv_pipeline_config_generator.json` | Config generation crew (Tool 90) |
| `crew_uc_metric_view_generator.json` | UC Metric View generation crew (Tool 86) |
| `crew_ucmv_quality_validator.json` | Validation crew (Tool 91) |

---

## Related Documentation

- [Authentication Setup](./01-authentication-setup.md)
- [Simple Migration Story](./02-simple-migration-story.md)
- [Tool 86 - UC Metric View Generator](./tool-86-uc-metric-view-generator.md)
- [Tool 90 - Pipeline Config Generator](./tool-90-pipeline-config-generator.md)
- [Pipeline Config Guide](../UCMV_PIPELINE_CONFIG_GUIDE.md)
