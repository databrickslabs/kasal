# Tool 88 - Metric View Deployer

**What it is:** Validates or deploys the UC Metric View YAML + SQL output from Tool 86 to a Databricks workspace via the UC REST API. Dry-run is the default - no deployment happens unless explicitly requested.

---

## Why It Exists

Tool 86 generates the YAML and SQL. Tool 88 is the "last mile" - it takes that output and either validates it (dry-run) or executes the `CREATE METRIC VIEW` statements in Databricks. The dry-run default gives the SA and customer an opportunity to review before anything is deployed.

## What Problem It Solves

- **Safe deployment:** Dry-run by default means no accidental production deployments
- **YAML validation:** Checks structure, SQL syntax, and naming conventions before touching Databricks
- **Incremental updates:** Handles `409 Conflict` (metric view already exists) with an update flow
- **Per-view status:** Reports `validated`, `deployed`, `updated`, or `error` per metric view

---

## How It Works

```
Input: yaml_specs_json + sql_specs_json (from Tool 86 output)
    ↓
If dry_run=true:
  Validate YAML structure, SQL syntax, dangerous pattern detection
  Return: validation report per metric view (no Databricks API calls)
    ↓
If dry_run=false:
  Execute CREATE METRIC VIEW SQL via Databricks SQL Statement API
  Handle 409 Conflict → attempt UPDATE METRIC VIEW
  Return: deployment status per metric view
```

---

## Security

- **SSRF prevention:** `databricks_host` must end in `.cloud.databricks.com`, `.azuredatabricks.net`, `.gcp.databricks.com`, `.databricksapps.com`, or `.databricks.azure.cn`
- **Dangerous SQL detection:** Scans YAML for `DROP`, `TRUNCATE`, `DELETE` patterns before executing
- **Dry-run default:** `dry_run: true` is intentional - require explicit opt-in to deploy

---

## Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `yaml_specs_json` | Yes | - | YAML specs from Tool 86 output |
| `sql_specs_json` | Yes | - | SQL specs from Tool 86 output |
| `catalog` | No | `main` | Target catalog |
| `schema_name` | No | `default` | Target schema |
| `dry_run` | No | `true` | Validate only; set `false` to deploy |
| `databricks_host` | Deploy only | - | Workspace URL (e.g. `https://xyz.cloud.databricks.com`) |
| `databricks_token` | Deploy only | - | PAT token for deployment |
| `warehouse_id` | Deploy only | - | SQL warehouse ID |

---

## Example Crew

```json
{
  "name": "UCMV Validation and Deploy",
  "tasks": [
    {
      "name": "Validate metric views (dry run)",
      "description": "Validate the generated UC Metric View YAML and SQL without deploying",
      "tool_ids": [88],
      "tool_config": {
        "88": {
          "dry_run": true,
          "catalog": "my_catalog",
          "schema_name": "metrics"
        }
      }
    }
  ]
}
```

For live deployment (after customer approval):

```json
{
  "tool_config": {
    "88": {
      "dry_run": false,
      "catalog": "my_catalog",
      "schema_name": "metrics",
      "databricks_host": "https://xyz.cloud.databricks.com",
      "databricks_token": "{databricks_pat}",
      "warehouse_id": "{warehouse_id}"
    }
  }
}
```

---

## Example Output (Dry Run)

```json
{
  "deployment_results": {
    "fact_sales": {
      "status": "validated",
      "view_name": "my_catalog.metrics.fact_sales_uc_metric_view",
      "yaml_lines": 42,
      "sql_lines": 38,
      "response": null
    },
    "fact_hr": {
      "status": "validated",
      "view_name": "my_catalog.metrics.fact_hr_uc_metric_view",
      "yaml_lines": 31,
      "sql_lines": 27,
      "response": null
    }
  },
  "summary": {
    "total": 2,
    "validated": 2,
    "deployed": 0,
    "updated": 0,
    "errors": 0,
    "dry_run": true
  }
}
```

---

## After Deployment

Verify the deployed metric views with:
```sql
SHOW METRIC VIEWS IN my_catalog.metrics;
SELECT MEASURE(Total_Revenue) FROM my_catalog.metrics.fact_sales_uc_metric_view GROUP BY Region;
```

Or use the `deploy_test.py` script from the examples directory for smoke testing.

---

## Notes

- Always run dry-run first and share the validation report with the customer before deploying
- `409 Conflict` responses are handled automatically with an update attempt - safe to re-deploy after config changes
- If a metric view fails to deploy, the others continue - per-view error reporting lets you fix and re-run specific views
