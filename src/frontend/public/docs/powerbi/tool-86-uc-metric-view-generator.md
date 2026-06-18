# Tool 86 - UC Metric View Generator

**What it is:** The core migration tool. Takes extracted Power BI data (measures, M-Query sources, relationships) and runs the full pipeline to produce Unity Catalog Metric View YAML definitions and deploy SQL for each fact table.

---

## Why It Exists

UC Metric Views are Databricks's governed semantic layer - they define business metrics in a reusable, queryable format. Manually converting hundreds of DAX measures, M-Query source definitions, and relationship structures to UC Metric View YAML is weeks of work. Tool 86 automates this pipeline end-to-end.

## What Problem It Solves

- **The core migration challenge:** DAX → SQL translation, dependency ordering, join detection, and YAML generation - all in one deterministic pipeline
- **Reproducibility:** Same input always produces the same output (no LLM required by default)
- **Scale:** A 470-measure model completes in under 30 seconds; 823 tests validate correctness

---

## How It Works

```
Input: measures_json + mquery_json + (optional) relationships_json + config_json
    ↓
MQuery Parser: identify fact tables and source SQL
    ↓
DAX Translator: 14+ patterns → SQL expressions per measure
    ↓
Kahn's Algorithm: topological sort of measure dependencies
    ↓
Join Detector: discover dimension joins from DAX references + relationships
    ↓
YAML Emitter: generate UC Metric View YAML per fact table
    ↓
SQL Emitter: generate CREATE METRIC VIEW SQL per fact table
    ↓
Output: {yaml: {...}, sql: {...}, stats: {...}, migration_report: "..."}
```

---

## Two Input Modes

### Mode 1: API Mode (pulls from PBI live)
Provide `workspace_id` + `dataset_id` + PBI credentials. Tool 86 calls the PBI APIs internally to extract measures, M-Query, and relationships.

### Mode 2: JSON Mode (uses pre-extracted data)
Provide `measures_json` from Tool 73, `mquery_json` from Tool 74, `relationships_json` from Tool 75. No PBI API calls at generation time.

JSON mode is preferred for the full pipeline (each extraction tool runs separately and their outputs chain into Tool 86).

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
| `unflatten_tables` | No | `false` | Convert `cat__sch__tbl` → `cat.sch.tbl` |

---

## Example Crew (JSON Mode - Full Pipeline)

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

## Example Output

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

## Proven Results

Tested against the SC Reporting project (26 fact tables, 470 measures):
- 20 of 23 reference views: **exact measure-level match**
- 3 of 23: **strict supersets** (more translated, zero missing)
- Translation rate: **93% business coverage**
- Runtime: **~30 seconds per run**

---

## After Generation

Pass the output directly to:
- **Tool 88** (Metric View Deployer) for validation and deployment
- Download YAML/SQL files from the Kasal UI result viewer

---

## Notes

- `config_json` is the key to improving translation rate - see [Pipeline Config Guide](../UCMV_PIPELINE_CONFIG_GUIDE.md) and [Tool 90](./tool-90-pipeline-config-generator.md) for how to generate it
- LLM fallback (`use_llm_fallback: true`) can push translation rate from 70% to 85%+ on complex models - at the cost of non-determinism
- Each re-run with the same inputs produces identical output (without LLM fallback) - safe to iterate
