# Tool 74 - M-Query conversion pipeline

**What it is:** Extracts Power Query (M-Query) table source expressions from a Power BI model using the Admin Scanner API, then converts them to Databricks SQL `CREATE VIEW` statements.

---

## Why it exists

Power BI semantic models don't just contain measures - they also define *where the data comes from* through M-Query expressions. These can be native SQL queries, Databricks catalog references, SQL Server connections, static tables, and more. When you migrate to Databricks, you need these source definitions recreated as SQL views in Unity Catalog. This tool automates that extraction and conversion.

## What problem it solves

- **Without this tool:** An SA has to manually open Power BI Desktop for each table, read the M-Query expression, translate it to SQL, and create the view - hours of repetitive work per model
- **With this tool:** The Admin Scanner API returns all M-Query expressions in one call; the tool converts them automatically

---

## How it works

```text
Connect to PBI Admin Scanner API (PostWorkspaceInfo)
    ↓
Scan workspace metadata → extract all M-Query expressions per table
    ↓
Classify expression type: NativeQuery, DatabricksMultiCloud, Sql.Database, Table.FromRows, etc.
    ↓
Rule-based conversion for known types
LLM-assisted conversion for complex/unknown expressions (optional)
    ↓
Emit CREATE VIEW statements for Unity Catalog
```

---

## Microsoft API reference

Uses (3-step process):
1. `POST /admin/workspaces/getInfo` - initiate scan
2. `GET /admin/workspaces/scanStatus/{scanId}` - poll until ready
3. `GET /admin/workspaces/scanResult/{scanId}` - retrieve results

Docs: [Admin - WorkspaceInfo PostWorkspaceInfo](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info)

---

## Authentication

**Admin SP** (tenant-level), with Power BI Admin Portal settings enabled.
This is the most complex auth setup - see [Authentication Setup](./01-authentication-setup.md) for the full step-by-step.

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Power BI Workspace GUID |
| `dataset_id` | No | Specific dataset GUID (scans all in workspace if omitted) |
| `tenant_id` | Yes | Azure AD tenant ID |
| `client_id` | Yes | Admin SP client ID |
| `client_secret` | Yes | Admin SP client secret |
| `target_catalog` | No | Target UC catalog (default: `main`) |
| `target_schema` | No | Target UC schema (default: `default`) |
| `use_llm` | No | Enable LLM fallback for complex M-Query (default: `false`) |
| `llm_workspace_url` | No | Databricks workspace for LLM |
| `llm_token` | No | PAT for LLM |
| `llm_model` | No | Model endpoint name |

## Supported M-Query expression types

| Expression type | Conversion method | Example output |
|----------------|-------------------|----------------|
| `Value.NativeQuery` | Direct SQL extraction | `CREATE VIEW ... AS SELECT ...` |
| `DatabricksMultiCloud.Catalogs` | Direct catalog reference | `CREATE VIEW ... AS SELECT * FROM catalog.schema.table` |
| `Sql.Database` | LLM or rule-based | `CREATE VIEW ... AS SELECT ...` |
| `Table.FromRows` | Rule-based | `CREATE VIEW ... AS SELECT * FROM VALUES (...)` |
| ODBC, Oracle, Snowflake | LLM conversion | Best-effort SQL |

---

## Example crew

```json
{
  "name": "PBI M-Query Extraction",
  "tasks": [{
    "name": "Extract M-Query and convert to SQL views",
    "description": "Scan the Power BI workspace and convert all M-Query table definitions to Databricks SQL CREATE VIEW statements",
    "tool_ids": [74],
    "tool_config": {
      "74": {
        "workspace_id": "{workspace_id}",
        "dataset_id": "{dataset_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{admin_client_id}",
        "client_secret": "{admin_client_secret}",
        "target_catalog": "my_catalog",
        "target_schema": "staging"
      }
    }
  }]
}
```

---

## Example output

```sql
-- Table: Fact_Sales (NativeQuery)
CREATE OR REPLACE VIEW my_catalog.staging.fact_sales AS
SELECT fiscper, comp_code, SUM(amount) AS amount
FROM my_catalog.raw.sales_source
GROUP BY fiscper, comp_code;

-- Table: Dim_Customer (DatabricksMultiCloud)
CREATE OR REPLACE VIEW my_catalog.staging.dim_customer AS
SELECT * FROM my_catalog.raw.dim_customer;
```

---

## In the UCMV pipeline

Tool 74 is Phase 1 of the migration alongside Tool 73. Its output (`mquery_json`) feeds:
- **Tool 86** (UC Metric View Generator) - source SQL for each fact table
- **Tool 87** (Measure Allocator) - fact table identification
- **Tool 89/90** (Config Generators) - auto-propose join keys

---

## Notes

- **Admin API is required** - this cannot be done with a non-admin SP
- If the Admin Portal settings haven't propagated yet (wait 15 min), you will get `401 Unauthorized`
- Very complex M-Query transformations (custom functions, nested let expressions) benefit from enabling LLM fallback (`use_llm: true`)

## See also

- [Power BI integration hub](./README.md)
- [Authentication and service principal setup](./01-authentication-setup.md)
- [Tool 73 - measure conversion pipeline](./tool-73-measure-conversion.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
