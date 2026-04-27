# UC Metric View Tools (85–88)

**Tools 85–88** | **No PBI API Required** (works on JSON outputs from upstream tools)

Generate Databricks Unity Catalog Metric View YAML + deploy SQL from Power BI measures and MQuery transpilation data.

---

## Overview

These four tools replace the standalone `generate_metric_views.py` monolith from the SC Reporting project. They split the pipeline into composable steps that integrate with the existing Kasal Power BI tool chain.

### Tool Chain

```
Tool 74: MQuery Conversion Pipeline  ──→  mquery_transpilation JSON
Tool 73: Measure Conversion Pipeline  ──→  measures_raw JSON
Tool 75: Relationships Tool           ──→  relationships JSON (optional)
         │                                    │                    │
         └────────────────┬───────────────────┘                    │
                          ▼                                        │
         Tool 87: PBI Measure Allocator                            │
                  (group measures → fact tables)                   │
                          │                                        │
                          ▼                                        │
         Tool 86: UC Metric View Generator  ◄──────────────────────┘
                  (full pipeline → YAML + SQL per fact table)
                          │
                          ▼
         Tool 88: Metric View Deployer
                  (deploy to Databricks)
```

### What Each Tool Does

| ID | Tool | Purpose | When to Use |
|----|------|---------|-------------|
| **85** | DAX to SQL Translator | Translate individual DAX expressions to Spark SQL | Standalone DAX translation without full pipeline |
| **86** | UC Metric View Generator | Full pipeline: parse MQuery + translate DAX + detect joins + emit YAML/SQL | Main tool — generates complete metric views |
| **87** | PBI Measure Allocator | Group measures into fact tables based on DAX `Table[Column]` references | Pre-step when measures lack `proposed_allocation` field |
| **88** | Metric View Deployer | Deploy generated YAML/SQL to Databricks workspace | Final step — validates or deploys metric views |

---

## How to Execute

### Method 1: Visual Workflow (Recommended)

1. **Open Kasal UI** → Go to the workflow designer
2. **Create a crew** with the following task sequence:

```
Task 1: "Extract MQuery transpilation"
  → Tool: M-Query Conversion Pipeline (74)
  → Config: workspace_id, dataset_id, tenant_id, client_id, client_secret

Task 2: "Extract measures"
  → Tool: Measure Conversion Pipeline (73)
  → Config: inbound_connector=powerbi, outbound_format=uc_metrics, same PBI creds

Task 3: "Extract relationships"
  → Tool: Power BI Relationships Tool (75)
  → Config: same PBI creds

Task 4: "Generate UC Metric Views"
  → Tool: UC Metric View Generator (86)
  → Config: catalog=your_catalog, schema_name=your_schema
  → Expected input: Pass outputs from Tasks 1-3 as measures_json, mquery_json, relationships_json
```

3. **Run the crew** — the agent will chain the tools and produce YAML + SQL for each fact table

### Method 2: Single Tool (Agent Decides)

Create a single task with tools 73, 74, 75, 86, and 88 available. Give the agent an instruction like:

> "Extract all Power BI measures and MQuery expressions from the semantic model, generate UC Metric View YAML definitions for each fact table, and validate the output."

The agent will orchestrate the tool chain automatically.

### Method 3: API (Programmatic)

```python
# Create a crew via the API that uses tool 86 directly
import requests

crew_config = {
    "name": "UC Metric View Generation",
    "tasks": [{
        "name": "Generate Metric Views",
        "description": "Generate UC Metric Views from the provided measures and MQuery data",
        "tools": [{"id": 86}],
        "tool_config": {
            "86": {
                "catalog": "my_catalog",
                "schema_name": "my_schema",
                "measures_json": "{measures_json_from_tool_73}",
                "mquery_json": "{mquery_json_from_tool_74}"
            }
        }
    }]
}
```

---

## Tool 85: DAX to SQL Translator

Standalone DAX→SQL translation using 14+ pattern-based rules.

### Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dax_measures_json` | Yes | JSON array of measures with `dax_expression` fields |
| `table_key` | No | Target fact table key for context |
| `config_json` | No | Pipeline config overrides (filter_sets, measure_resolutions) |

### Example Input

```json
[
  {"measure_name": "Total Sales", "dax_expression": "SUM(Sales[Amount])", "original_name": "Total Sales"},
  {"measure_name": "Sales %", "dax_expression": "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Target]))", "original_name": "Sales %"}
]
```

### Example Output

```json
{
  "results": [
    {"measure_name": "total_sales", "sql_expr": "SUM(source.Amount)", "is_translatable": true, "confidence": "high"},
    {"measure_name": "sales_pct", "sql_expr": "SUM(source.Amount) / NULLIF(SUM(source.Target), 0)", "is_translatable": true, "confidence": "high"}
  ],
  "summary": {"total": 2, "translated": 2, "untranslatable": 0, "rate": "100%"}
}
```

### Supported DAX Patterns

| Pattern | Example DAX | Output SQL |
|---------|-------------|------------|
| Simple SUM | `SUM(T[col])` | `SUM(source.col)` |
| CALCULATE+SUM | `CALCULATE(SUM(T[col]))` | `SUM(source.col)` |
| SUMX without FILTER | `SUMX(T, T[col])` | `SUM(source.col)` |
| SUMX+FILTER | `SUMX(FILTER(T, cond), T[col])` | `SUM(source.col) FILTER (WHERE cond)` |
| CALCULATE+SUMX+FILTER | `CALCULATE(SUMX(FILTER(T, c), T[col]))` | `SUM(source.col) FILTER (WHERE c)` |
| COUNTX+FILTER | `COUNTX(FILTER(T, c), T[col])` | `COUNT(source.col) FILTER (WHERE c)` |
| AVERAGEX+FILTER | `AVERAGEX(FILTER(T, c), T[col])` | `AVG(source.col) FILTER (WHERE c)` |
| DIVIDE | `DIVIDE(a, b)` | `a / NULLIF(b, 0)` |
| DISTINCTCOUNTNOBLANK | `DISTINCTCOUNTNOBLANK(T[col])` | `COUNT(DISTINCT source.col)` |
| SAMEPERIODLASTYEAR | `CALCULATE(SUMX(...), SAMEPERIODLASTYEAR(...))` | SQL + `window: trailing 12 month` |
| CALCULATE([ref], filter) | `CALCULATE([Measure], FILTER(D, c))` | Resolved SQL + merged filters |
| VAR/RETURN+DIVIDE | `var a = CALC(...) return DIVIDE(a, b)` | Resolved vars + NULLIF |
| Quick reject | FORMAT, Color, ISBLANK, SELECTEDVALUE+SWITCH | Skipped (PBI artifacts) |

---

## Tool 86: UC Metric View Generator

The main pipeline tool. Combines everything to produce YAML + deploy SQL per fact table.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `measures_json` | Yes | — | JSON from Measure Conversion Pipeline (tool 73) |
| `mquery_json` | Yes | — | JSON from MQuery Conversion Pipeline (tool 74) |
| `relationships_json` | No | — | JSON from Relationships Tool (tool 75) |
| `scan_data_json` | No | — | PBI scan data for enrichment (inline SQL source) |
| `config_json` | No | `{}` | Pipeline config overrides (see below) |
| `catalog` | No | `main` | Target Unity Catalog name |
| `schema_name` | No | `default` | Target schema name |
| `inner_dim_joins` | No | `false` | Use INNER JOIN for dimensions |
| `unflatten_tables` | No | `false` | Convert `cat__sch__tbl` → `cat.sch.tbl` |

### Pipeline Config JSON

The `config_json` parameter accepts customer-specific overrides that were previously hardcoded in the monolith:

```json
{
  "join_key_map": {
    "Dim_Geography": {
      "alias": "dim_geography",
      "join_key": "comp_code",
      "dim_columns": ["country", "business_unit"]
    }
  },
  "fact_join_map": {
    "fact_scorecard": {
      "alias": "sc_actuals",
      "join_key": ["fiscper", "comp_code"],
      "column_map": {"val": "fltp"}
    }
  },
  "enrichment_joins": {},
  "switch_decompositions": {},
  "measure_resolutions": {},
  "column_metadata": {},
  "measure_metadata": {},
  "dimension_metadata": {},
  "comment_overrides": {},
  "dimension_exclusions": {},
  "dimension_order": {},
  "mapping_only_tables": {}
}
```

### Example Output

```json
{
  "yaml": {
    "fact_pe002": "name: fact_pe002_uc_metric_view\ncatalog: main\nschema: default\n...",
    "Fact_HR_A": "name: fact_hr_a_uc_metric_view\n..."
  },
  "sql": {
    "fact_pe002": "CREATE OR REPLACE METRIC VIEW main.default.fact_pe002_uc_metric_view ...",
    "Fact_HR_A": "CREATE OR REPLACE METRIC VIEW main.default.fact_hr_a_uc_metric_view ..."
  },
  "stats": {
    "fact_pe002": {"total": 45, "translated": 38, "untranslatable": 7, "base": 12, "dax": 26},
    "Fact_HR_A": {"total": 30, "translated": 25, "untranslatable": 5, "base": 8, "dax": 17}
  }
}
```

---

## Tool 87: PBI Measure Allocator

Groups measures into fact tables before running the main pipeline. Use this when your measures don't already have `proposed_allocation` fields.

### Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `measures_json` | Yes | Raw measures from Power BI Connector/Fetcher |
| `mquery_json` | Yes | MQuery transpilation JSON |

### How It Works

1. Parses MQuery JSON to identify fact tables (tables with `SUM` + `GROUP BY`)
2. Scans each measure's DAX for `Table[Column]` references
3. Matches references against known fact tables
4. Assigns confidence: **high** (single fact), **medium** (multiple facts), **low/none** (no match)

### Example Output

```json
{
  "allocations": [
    {"measure_name": "Total Sales", "proposed_allocation": "fact_sales", "confidence": "high"},
    {"measure_name": "Cross Ratio", "proposed_allocation": "fact_sales", "confidence": "medium"}
  ],
  "summary": {"total": 50, "allocated": 42, "unassigned": 8}
}
```

---

## Tool 88: Metric View Deployer

Validates or deploys generated metric views to Databricks.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `yaml_specs_json` | Yes | — | YAML specs from tool 86 |
| `sql_specs_json` | Yes | — | SQL specs from tool 86 |
| `catalog` | No | `main` | Target catalog |
| `schema_name` | No | `default` | Target schema |
| `dry_run` | No | `true` | Validate only (no deployment) |
| `databricks_host` | For deploy | — | Workspace URL (for `dry_run=false`) |
| `databricks_token` | For deploy | — | PAT token (for `dry_run=false`) |

---

## Comparison: Standalone Script vs Kasal Tools

| Aspect | Standalone `generate_metric_views.py` | Kasal Tools 85–88 |
|--------|--------------------------------------|-------------------|
| **Input** | Excel/JSON files on disk | JSON strings from upstream tools |
| **Output** | Files written to `output/` directory | JSON returned to agent/API |
| **Config** | Hardcoded Python dicts | JSON `config_json` parameter |
| **Execution** | `python generate_metric_views.py --mquery ... --mapping ...` | Kasal UI workflow or API |
| **Integration** | Standalone CLI | Composes with 11 Power BI tools |
| **Multi-tenant** | Single-run | Group-isolated via Kasal platform |

---

## Architecture

### Module Structure

```
src/engines/crewai/tools/custom/
├── metric_view_utils/           # Utility modules (extracted from monolith)
│   ├── data_classes.py          # TranslationResult, TableInfo, MetricViewSpec
│   ├── constants.py             # Shared regex patterns
│   ├── utils.py                 # to_snake_case, spark_sql_compat
│   ├── mquery_parser.py         # Parse MQuery transpilation JSON → TableInfo
│   ├── scan_data_parser.py      # Parse PBI scan data → ScanTableInfo
│   ├── pbi_parameter_resolver.py # Resolve FiscperFilter, RE_Version params
│   ├── m_transform_folder.py    # Fold M steps (SelectRows, ReplaceValue) into SQL
│   ├── sql_post_processor.py    # Strip aliases, fix keywords, normalize SQL
│   ├── metadata_generator.py    # Generate display names, synonyms, formats
│   ├── relationships_loader.py  # Auto-build joins from PBI relationships
│   ├── join_detector.py         # Detect dim + fact-to-fact joins from DAX refs
│   ├── dax_translator.py        # 14-pattern DAX→SQL translator
│   ├── yaml_emitter.py          # Emit UC Metric View YAML
│   ├── sql_emitter.py           # Emit deploy SQL
│   └── pipeline.py              # MetricViewPipeline orchestrator
├── dax_to_sql_translator_tool.py    # Tool 85
├── uc_metric_view_generator_tool.py # Tool 86
├── pbi_measure_allocator_tool.py    # Tool 87
└── metric_view_deployer_tool.py     # Tool 88
```

---

## Troubleshooting

### "No measures translated"
- Ensure `measures_json` contains `dax_expression` fields
- Check that `proposed_allocation` matches fact table names in `mquery_json`
- Use Tool 87 first to allocate measures if they don't have allocations

### "0 fact tables discovered"
- Ensure `mquery_json` contains entries with `SUM(...)` + `GROUP BY` in `transpiled_sql`
- Check `validation_passed` starts with "Yes"

### "Join not detected"
- Ensure `join_key_map` in `config_json` maps PBI dim table names to physical tables
- Alternatively, provide `relationships_json` from Tool 75 for auto-detection
