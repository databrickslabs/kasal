# Tool 77 - Field Parameters & Calculation Groups

**What it is:** Extracts Field Parameters and Calculation Groups from Microsoft Fabric semantic models and generates Unity Catalog SQL views and metadata tables that replicate the same dynamic switching behavior.

---

## Why It Exists

Field Parameters and Calculation Groups are advanced Fabric features:
- **Field Parameters** let report users switch between measures dynamically (e.g. show Revenue, Profit, or Units Sold in the same chart)
- **Calculation Groups** provide reusable time intelligence patterns (YTD, Prior Year, YoY%) applied across many measures

Neither has a direct equivalent in Unity Catalog. This tool creates SQL UNION views that approximate the same functionality.

## What Problem It Solves

- **Feature parity:** Customers who rely heavily on Field Parameters need an equivalent in the migrated layer
- **Documentation:** Even if the exact behavior can't be replicated, documenting which measures participate in which parameter helps SAs plan the migration
- **Time intelligence migration:** Calculation Groups are often the only place where YTD/Prior Year logic lives - this tool extracts and converts it

---

## Fabric-Only Requirement

Same as Tool 76 - requires Microsoft Fabric workspace and TMDL format.
Classic Power BI Service workspaces are not supported.

---

## Microsoft API Reference

Uses: `GET /groups/{groupId}/items/{itemId}/getDefinition` (Fabric REST API)
Docs: [Fabric - Get Item Definition](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition)

---

## Authentication

**Non-Admin SP** with `SemanticModel.ReadWrite.All` permission.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Fabric Workspace GUID |
| `dataset_id` | Yes | Semantic Model GUID |
| `tenant_id` | Yes | Azure AD tenant ID |
| `client_id` | Yes | SP client ID |
| `client_secret` | Yes | SP client secret |
| `target_catalog` | No | Target UC catalog (default: `main`) |
| `target_schema` | No | Target UC schema (default: `default`) |
| `output_format` | No | `sql`, `json`, or `markdown` (default: `sql`) |
| `include_metadata_table` | No | Generate metadata DDL (default: `true`) |

---

## Example Crew

```json
{
  "name": "PBI Field Parameters Migration",
  "tasks": [{
    "name": "Extract field parameters and calculation groups",
    "description": "Extract Field Parameters and Calculation Groups and generate SQL equivalents",
    "tool_ids": [77],
    "tool_config": {
      "77": {
        "workspace_id": "{fabric_workspace_id}",
        "dataset_id": "{semantic_model_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "target_catalog": "my_catalog",
        "target_schema": "metrics",
        "output_format": "sql"
      }
    }
  }]
}
```

---

## Example Output

```sql
-- Field Parameter: Measure Selector
CREATE OR REPLACE VIEW my_catalog.metrics.vw_field_param_measure_selector AS
SELECT 'Revenue' AS metric_name, SUM(amount) AS metric_value FROM my_catalog.raw.fact_sales
UNION ALL
SELECT 'Profit' AS metric_name, SUM(profit) AS metric_value FROM my_catalog.raw.fact_sales
UNION ALL
SELECT 'Units Sold' AS metric_name, SUM(quantity) AS metric_value FROM my_catalog.raw.fact_sales;

-- Calculation Group: Time Intelligence
CREATE OR REPLACE VIEW my_catalog.metrics.vw_time_intelligence AS
SELECT 'Current' AS period, SUM(amount) AS value FROM my_catalog.raw.fact_sales
UNION ALL
SELECT 'YTD' AS period,
  SUM(SUM(amount)) OVER (PARTITION BY year ORDER BY month ROWS UNBOUNDED PRECEDING) AS value
FROM my_catalog.raw.fact_sales
UNION ALL
SELECT 'Prior Year' AS period, SUM(amount) AS value
FROM my_catalog.raw.fact_sales WHERE year = YEAR(CURRENT_DATE) - 1;
```

---

## Notes

- Not part of the core UCMV pipeline - run alongside it when the customer has Field Parameters or Calculation Groups
- The SQL equivalents approximate the behavior but don't replicate the interactive switching (that's a frontend/BI tool concern)
- Very complex NAMEOF() and SELECTEDMEASURE() patterns may need manual refinement
