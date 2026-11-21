# Tool 86 - UC Metric View generator

**What it is:** The core migration tool. Takes extracted Power BI data (measures, M-Query sources, relationships) and runs the full pipeline to produce Unity Catalog Metric View YAML definitions and deploy SQL for each fact table.

---

## Why it exists

UC Metric Views are Databricks's governed semantic layer: they define business metrics in a reusable, queryable format. Manually converting hundreds of DAX measures, M-Query source definitions, and relationship structures to UC Metric View YAML is weeks of work. Tool 86 automates this pipeline end-to-end.

## What problem it solves

- **The core migration challenge:** DAX to SQL translation, dependency ordering, join detection, and YAML generation, all in one deterministic pipeline
- **Reproducibility:** Same input always produces the same output (no LLM required by default)
- **Scale:** A 470-measure model completes in under 30 seconds; 823 tests validate correctness

---

## How it works

```text
Input: measures_json + mquery_json + (optional) relationships_json + config_json
    |
    v
MQuery Parser: identify fact tables and source SQL
    |
    v
DAX Translator: 14+ patterns to SQL expressions per measure
    |
    v
Kahn's Algorithm: topological sort of measure dependencies
    |
    v
Join Detector: discover dimension joins from DAX references + relationships
    |
    v
YAML Emitter: generate UC Metric View YAML per fact table
    |
    v
SQL Emitter: generate CREATE METRIC VIEW SQL per fact table
    |
    v
Output: {yaml: {...}, sql: {...}, stats: {...}, migration_report: "..."}
```

---

## Two input modes

### Mode 1: API mode (pulls from PBI live)
Provide `workspace_id` + `dataset_id` + PBI credentials. Tool 86 calls the PBI APIs internally to extract measures, M-Query, and relationships.

### Mode 2: JSON mode (uses pre-extracted data)
Provide `measures_json` from Tool 73, `mquery_json` from Tool 74, `relationships_json` from Tool 75. No PBI API calls at generation time.

JSON mode is preferred for the full pipeline (each extraction tool runs separately and their outputs chain into Tool 86).

### Prior-year measures

A measure that is another calculation one year earlier is emitted as that calculation with a window:

```yaml
- name: sales_py
  expr: MEASURE(sales)
  window:
    - order: fiscper_key
      range: current
      offset: -1000
      semiadditive: last
```

Recognised in the DAX, when the whole result is shifted:

| DAX | Example |
|-----|---------|
| `SAMEPERIODLASTYEAR` | `CALCULATE([Sales], SAMEPERIODLASTYEAR(Calendar[Date]))` |
| `DATEADD` by one year (`-1 YEAR`, `-12 MONTH`, `-4 QUARTER`) | `CALCULATE([Sales], DATEADD(Calendar[Date], -1, YEAR))` |
| `PARALLELPERIOD` by `-12 MONTH` | `CALCULATE([Sales], PARALLELPERIOD(Calendar[Date], -12, MONTH))` |
| A prior-year column on the calendar | `CALCULATE([Sales], ALL(Calendar), Calendar[Date] IN VALUES(Calendar[Date_PY]))` |

The calculation without the shift is homed and translated like any other measure (rules or LLM). A measure built on prior-year measures (`[Sales PY] / [Volume PY]`) needs nothing extra.

The window orders by a dimension the generator adds to the view:

| The view has | Dimension added | One year back |
|--------------|-----------------|---------------|
| A period column with year-first keys: `fiscper` (YYYYPPP), `calmonth` / `yearmonth` (YYYYMM) | `<period>_key`: the period as a number | 1000 keys (YYYYPPP) or 100 keys (YYYYMM) |
| The calendar the DAX shifts on, joined | `<date>_month`: the month of the calendar date | `-1 year` |

A view with neither does not emit the measure; the reason is in the review list.

Limits:

- A prior-year measure returns a value when the query groups by that dimension and by no other time dimension (not by `fiscper` or `year` next to `fiscper_key`). Other dimensions are fine.
- A filter on another time dimension (`WHERE year = 2026`) removes the earlier year's rows, so the measure is empty. Filter on the window's dimension instead.
- A measure that mixes this year and last year in one expression (`[Sales] - CALCULATE([Sales], SAMEPERIODLASTYEAR(...))`) is not handled here. It goes to the LLM as before.

Pipeline config: `prior_year_period_steps` (for example `{"fy_period": 100}`) names further period columns and how many keys one year is. `prior_year_measures: false` turns the handling off.

With `enable_reconciliation`, prior-year measures are compared in a second pass: the metric view grouped by the window's dimension, Power BI by the period column of the measure's calendar (`shifted_time_dimension` in the mapping, `time_shifted: true` on the measure). Power BI returns the earlier value only when grouped by that calendar. If the second pass fails, the other measures are still compared and these are listed as not compared.

Periods the metric view has and Power BI returns no row for (history before the model's calendar starts, rows the refresh does not load) are not compared: there is nothing to compare them with. Each view's report counts them under `periods_not_in_power_bi` (`periods`, `first`, `last`), and `review_summary` carries the same. When the two sides share no period at all, nothing is left out, so a period format that does not match stays visible.

### Cross-fact measures (combined source)

A measure whose DAX reads columns of two fact tables cannot sit on one metric view. Inside the reconciliation loop, the generator builds one **combined source** per pair of facts those measures need: a `UNION ALL` of the two facts' source views, each projected to the shared grain (the group-by columns both facts have), with each fact's measure columns `NULL` in the other arm. The measures are translated on it like any other measure, and the view is a normal entry in the output (YAML, resolved measures, source-layer DDL, comparison).

```sql
CREATE OR REPLACE VIEW cat.sch.src_combined_fact_a_fact_b AS
SELECT CalMonth, Region, Amount, NULL AS Budget FROM cat.sch.src_fact_a
UNION ALL
SELECT CalMonth, Region, NULL AS Amount, Budget FROM cat.sch.src_fact_b
```

Why the union is exact for the measures it accepts: it adds rows and never joins, so no row is multiplied; a column is filled only by its own fact, and every SQL aggregate ignores `NULL`, so `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` and `DISTINCTCOUNT` of a fact column equal the same aggregate over that fact alone. A ratio of such aggregates is exact too.

Not accepted (the reason is in the review list): row counts (`COUNTROWS`, `COUNT(*)`) and row iterators (`SUMX` and similar), filter context (`CALCULATE`, `FILTER`, `ALL`, time intelligence), related lookups, measures that read dimension columns, three or more facts, two facts that share no grain column, a shared grain without a period column and a second column (the view could not be compared), and a column present on both facts that is not a shared grain column.

The shared grain becomes the view's dimensions. Each fact's source view already carries its Power Query filters and transformations, so the other fact's filters apply to its arm.

Requires `read_from_source_layer`. Pipeline config: `cross_table_union_sources: false` turns it off (default on). Only the reconciliation loop runs it, so it has no effect offline.

Limits:

- A measure that is also translated on its own fact view stays there, so it appears on both views. Each view is compared on its own.
- Columns of unparsed tables are unknown to the generator, so a measure that reads one is not accepted. On a model with unparsed tables this is the usual reason nothing is combined.

---

## Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `measures_json` | JSON mode | - | Output from Tool 73 |
| `mquery_json` | JSON mode | - | Output from Tool 74 |
| `relationships_json` | No | - | Output from Tool 75 (improves join detection) |
| `config_json` | No | `{}` | Pipeline config (see [Config Guide](../UCMV_PIPELINE_CONFIG_GUIDE.md)) |
| `catalog` | No | `main` | Target Unity Catalog name |
| `schema_name` | No | `default` | Target schema name |
| `workspace_id` | API mode | - | PBI Workspace GUID |
| `dataset_id` | API mode | - | PBI Dataset GUID |
| `tenant_id` | API mode | - | Azure AD tenant ID |
| `client_id` | API mode | - | SP client ID |
| `client_secret` | API mode | - | SP client secret |
| `use_llm_fallback` | No | `false` | Enable LLM for untranslatable DAX |
| `llm_workspace_url` | LLM | - | Databricks workspace URL |
| `llm_token` | LLM | - | Databricks PAT |
| `inner_dim_joins` | No | `false` | Use INNER JOIN for dimensions |
| `unflatten_tables` | No | `false` | Convert `cat__sch__tbl` to `cat.sch.tbl` |
| `report_scope` | No | - | Scope table from Tool 90 (injected in a flow); completed with `in_kasal`, `mirrored_as` and the review buckets |
| `enable_reconciliation` | No | `false` | Deploy and reconcile against Power BI each cycle (needs `warehouse_id`) |

With `enable_reconciliation`, the output also carries the review list: `review_csv` (one row per measure: `reconciled`, `below_bar`, `not_compared`, `removed_on_deploy`, `not_deployed` or `not_translated`, with the share of matching cells and the reason; statistics only, no measure values) and `review_summary` (the same per view). The bar is the pipeline config key `review_bar_pct` (default 90). A view is compared by a joined table only when the model has an active relationship between that table and the fact; otherwise the fact's own join key is used. Each row also carries `kind` (`measure` or `column_sum`) and `scope` (`in_scope`, `display`, `counted_elsewhere`), and `review_overview` counts per view the in-scope measures that are translated, match Power BI, are below the bar, were lost on deploy or are not translated. A view that has no period column but a date the model relates to a calendar is compared through that calendar's period.

Pipeline config keys added for production readiness: `read_from_source_layer` and `source_layer_naming` (metric views read from the `src_<table>` views). Each measure now carries a `format` derived from its Power BI format string. See [Step 2: convert](./ucmv-production-readiness.md#step-2-convert).

---

## Example crew (JSON mode, full pipeline)

```json
{
  "name": "UCMV Generation",
  "tasks": [
    {
      "name": "Extract M-Query",
      "description": "Extract M-Query expressions from the Power BI workspace",
      "tool_ids": [74],
      "tool_config": {"74": {"workspace_id": "{workspace_id}", "dataset_id": "{dataset_id}", "tenant_id": "{tenant_id}", "client_id": "{admin_client_id}", "client_secret": "{admin_client_secret}"}}
    },
    {
      "name": "Extract Measures",
      "description": "Extract all DAX measures from the Power BI model",
      "tool_ids": [73],
      "tool_config": {"73": {"workspace_id": "{workspace_id}", "dataset_id": "{dataset_id}", "tenant_id": "{tenant_id}", "client_id": "{client_id}", "client_secret": "{client_secret}", "outbound_format": "uc_metrics"}}
    },
    {
      "name": "Extract Relationships",
      "description": "Extract table relationships",
      "tool_ids": [75],
      "tool_config": {"75": {"workspace_id": "{workspace_id}", "dataset_id": "{dataset_id}", "tenant_id": "{tenant_id}", "client_id": "{client_id}", "client_secret": "{client_secret}"}}
    },
    {
      "name": "Generate UC Metric Views",
      "description": "Generate UC Metric View YAML and SQL from the extracted Power BI data. Use the pipeline_config.json for join mappings and config overrides.",
      "tool_ids": [86],
      "tool_config": {
        "86": {
          "catalog": "my_catalog",
          "schema_name": "metrics",
          "config_json": "{pipeline_config_json}"
        }
      },
      "depends_on": ["Extract M-Query", "Extract Measures", "Extract Relationships"]
    }
  ]
}
```

---

## Example output

```json
{
  "yaml": {
    "fact_sales": "name: fact_sales_uc_metric_view\ncatalog: my_catalog\nschema: metrics\nsource: my_catalog.raw.fact_sales\ndimensions:\n  - name: Region\n    expr: \"`region`\"\nmeasures:\n  - name: Total Revenue\n    expr: \"SUM(`amount`)\"\n  - name: YoY Growth\n    expr: \"...\"\n"
  },
  "sql": {
    "fact_sales": "CREATE OR REPLACE METRIC VIEW my_catalog.metrics.fact_sales_uc_metric_view ..."
  },
  "stats": {
    "fact_sales": {
      "total": 45,
      "translated": 40,
      "untranslatable": 5,
      "base": 12,
      "dax": 28,
      "coverage_pct": "88.9%"
    }
  },
  "migration_report": "## Migration Report\n\n### fact_sales\n- 40/45 measures translated (88.9%)\n- 5 untranslatable (FORMAT patterns, display-only)\n..."
}
```

---

## Proven results

Tested against the SC Reporting project (26 fact tables, 470 measures):
- 20 of 23 reference views: **exact measure-level match**
- 3 of 23: **strict supersets** (more translated, zero missing)
- Translation rate: **93% business coverage**
- Runtime: **~30 seconds per run**

---

## After generation

Pass the output directly to:
- **Tool 88** (Metric View Deployer) for validation and deployment
- Download YAML/SQL files from the Kasal UI result viewer

---

## Notes

- `config_json` is the key to improving translation rate. See the [Pipeline Config Guide](../UCMV_PIPELINE_CONFIG_GUIDE.md) and [Tool 90](./tool-90-pipeline-config-generator.md) for how to generate it
- LLM fallback (`use_llm_fallback: true`) can push translation rate from 70% to 85%+ on complex models, at the cost of non-determinism
- Each re-run with the same inputs produces identical output (without LLM fallback), safe to iterate
- SWITCH decompositions that need `switch_join_alias` / `switch_join_col` are reported as untranslatable while those config keys are still TODO markers; the rest of the run continues

## Opt-in snapshot of Power BI-only tables

Some tables have no warehouse source: an Excel, SharePoint or Web import, or a
`#table(...)` literal. Their measures cannot be put on a metric view, so the generator
lists them as `pbi_only_ingestion_tasks`. The snapshot step (package
`services/tools/pbi_snapshot_utils/`) can copy their rows out of Power BI into the
workspace.

**It copies data out of Power BI.** It is OFF by default.

| Config key | Default | Meaning |
|---|---|---|
| `pbi_snapshot_tables` | `false` | OPT-IN. When true, the rows of each listed PBI-only table are copied from Power BI into a Delta table in the run's source-layer schema. The copy is a snapshot: it is not refreshed. |
| `pbi_snapshot_max_rows` | `200000` | A table with more rows is skipped with the reason and stays an ingestion task. It is never partially loaded. Allowed: 1 to 5,000,000. |

**What is written.** Table `snap_<snake_table_name>` in the schema the run writes its
source layer to. Its table comment says it is a snapshot of the named Power BI table
taken at a given time (UTC) and not refreshed. Table properties: `kasal.pbi_snapshot = 'true'`
and `kasal.pbi_snapshot.taken_at`. Per table, the run output reports `rows`, `taken_at`
and `note: "snapshot, not refreshed"`. Nothing is created outside that schema; the schema
must exist.

**Reading Power BI (verified against Microsoft Learn, executeQueries page updated 2026-09-15).**
- At most 100,000 rows or 1,000,000 values per query, whichever is hit first; at most 15 MB
  per query; one table per query; 120 query requests per minute per user; DAX only (no
  INFO or DMV functions).
- A limit breach returns HTTP 200 with an error on the table and only the rows up to the
  limit. So the runner never trusts a row count: it compares the rows read with a
  `COUNTROWS` probe and refuses the table on any difference.
- Order and window: a table with a unique whole-number column is read by keyset pages
  (`TOPN(n, FILTER(T, key > last), key, ASC)`, 10,000 rows per page, bounded by the values
  limit). The next page starts after the largest key read, so a page cut short by the 15 MB
  limit loses nothing. A table without such a column is read in one query, and only when it
  fits one query. Otherwise it is refused.

**Refused, and written nothing** (the reason is in the output):
- row-level security applies to the table (a snapshot would show one identity's rows to everyone);
- its rows are embedded in the model (the source layer already emits them as SQL);
- over `pbi_snapshot_max_rows`, or more rows than the read can verify;
- a column type with no Databricks mapping (binary, variant, unknown); a value that does not match its type;
- the target name is taken by a table this tool did not create, or by a view.

**Types.** string to STRING; int64 to BIGINT; double and decimal to DOUBLE (the JSON
number's precision); boolean to BOOLEAN; dateTime to TIMESTAMP_NTZ (a value with a time
zone is refused). Values are typed literals: strings go through the shared SQL string
escaper, numbers are cast from quoted text, identifiers are backtick-quoted.

**Write order.** Check the target. Create a marked staging table. Insert in batches of 500
rows. Check the staged count equals the rows read. `CREATE OR REPLACE TABLE` the target from
the staging table, then drop the staging table. A failed drop is logged and does not hide the error.

**Not wired.** The module and its tests exist, but no generator run calls it yet. Wiring needs a
pre-pipeline step that has the Power BI token, the warehouse, and the model's column types,
and a change so the source-layer view of a snapshotted table reads the snapshot. Metric
views read the source layer only in read mode (`read_from_source_layer`).

**Known detection issue.** `detect_pbi_only_source` labels any `Binary.FromText` expression as
`tmdl_expression`, so "enter data" tables whose rows the source layer already emits are
listed as ingestion tasks. On one real model this was 23 of 27 listed tasks. The
snapshot step excludes them by decoding their rows, but the task list itself is unchanged.

## See also

- [Power BI integration hub](./README.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)
- [Tool 88 - metric view deployer](./tool-88-metric-view-deployer.md)
- [Pipeline config guide](../UCMV_PIPELINE_CONFIG_GUIDE.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
