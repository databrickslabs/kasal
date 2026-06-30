# Tool 73 - measure conversion pipeline

**What it is:** Extracts DAX measures from a Power BI semantic model and converts them to your chosen output format: Unity Catalog Metrics YAML, Databricks SQL, or DAX (reformatted).

---

## Why it exists

Power BI measures encode business logic (revenue calculations, KPIs, ratios) in DAX. When migrating to Databricks, you need that logic in a format Databricks understands. This tool bridges the gap: it connects live to the Power BI API, pulls all measures, and converts them.

## What problem it solves

- **Migration teams** need measure definitions extracted before they can build UC Metric Views
- **Documentation:** generates a complete inventory of all measures in a model with their SQL equivalents
- **Multi-format output:** same input, different output (UC Metrics, SQL, or just clean DAX for reference)

---

## How it works

```text
Connect to PBI Execute Queries API
  |
  v
Run EVALUATE INFO.MEASURES(): fetch all measures + DAX expressions
  |
  v
Parse DAX patterns (14+ rules) into SQL equivalents
  |
  v
Emit in target format (UC Metrics YAML / SQL / DAX)
```

---

## Microsoft API reference

Uses: `POST /groups/{groupId}/datasets/{datasetId}/executeQueries`
DAX: `EVALUATE INFO.MEASURES()`
Docs: [Datasets - ExecuteQueries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries)

---

## Authentication

**Non-Admin SP** (workspace member), `Dataset.Read.All` permission.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Power BI Workspace GUID |
| `dataset_id` | Yes | Semantic Model / Dataset GUID |
| `tenant_id` | Yes | Azure AD tenant ID |
| `client_id` | Yes | SP client ID |
| `client_secret` | Yes | SP client secret |
| `inbound_connector` | No | `powerbi` (default) or `yaml` |
| `outbound_format` | No | `uc_metrics` / `sql` / `dax` (default: `uc_metrics`) |
| `target_catalog` | No | Target UC catalog name |
| `target_schema` | No | Target UC schema name |

---

## Example crew

```json
{
  "name": "PBI Measure Extraction",
  "tasks": [{
    "name": "Extract and convert measures",
    "description": "Extract all DAX measures from the Power BI model and convert to UC Metrics YAML",
    "tool_ids": [73],
    "tool_config": {
      "73": {
        "workspace_id": "{workspace_id}",
        "dataset_id": "{dataset_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "inbound_connector": "powerbi",
        "outbound_format": "uc_metrics",
        "target_catalog": "my_catalog",
        "target_schema": "metrics"
      }
    }
  }]
}
```

---

## In the UCMV pipeline

Tool 73 is Phase 1 of the full migration. Its output (`measures_json`) feeds directly into:
- **Tool 87** (Measure Allocator: groups measures to fact tables)
- **Tool 86** (UC Metric View Generator: the main pipeline)
- **Tool 89** (Config Generator: proposes the pipeline config)

---

## Notes

- Also supports YAML as inbound format, useful if you already have measure definitions in YAML and want to convert to SQL
- For the full UCMV pipeline, use this alongside Tool 74 (M-Query) and Tool 75 (Relationships)
- See the [end-to-end UCMV migration guide](./ucmv-migration-guide.md) for the complete flow

## See also

- [Power BI integration hub](./README.md)
- [Authentication and service principal setup](./01-authentication-setup.md)
- [Tool 74 - M-Query conversion pipeline](./tool-74-mquery-conversion.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
