# Tool 75 - Power BI relationships tool

**What it is:** Extracts the relationship graph from a Power BI semantic model and generates Unity Catalog `FOREIGN KEY` constraint DDL statements.

---

## Why it exists

Power BI models define star/snowflake schema relationships between fact and dimension tables. These relationships are implicit in the model but need to be explicitly declared in Unity Catalog for lineage tracking, documentation, and to enable auto-join detection in Tool 86 (UC Metric View Generator).

## What problem it solves

- **Join detection for UCMV:** Tool 86 uses relationships to automatically discover which dimension tables to join to each fact table. Without Tool 75, the SA has to specify joins manually in the config
- **Schema documentation:** Foreign key constraints in UC (even `NOT ENFORCED`) appear in Unity Catalog lineage and serve as self-documenting schema
- **Migration completeness:** A complete migration includes not just measures and sources but also the relational structure

---

## How it works

```text
Connect to PBI Execute Queries API
  |
  v
Execute: EVALUATE INFO.VIEW.RELATIONSHIPS()
  |
  v
Parse results: from-table, from-column, to-table, to-column, cardinality, direction
  |
  v
Generate: ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... NOT ENFORCED
```

---

## Microsoft API reference

Uses: `POST /groups/{groupId}/datasets/{datasetId}/executeQueries`
DAX: `EVALUATE INFO.VIEW.RELATIONSHIPS()`
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
| `target_catalog` | No | Target UC catalog (default: `main`) |
| `target_schema` | No | Target UC schema (default: `default`) |
| `include_inactive` | No | Include inactive/hidden relationships (default: `false`) |
| `skip_system_tables` | No | Skip `LocalDateTable_*` system tables (default: `true`) |

---

## Example crew

```json
{
  "name": "PBI Relationship Extraction",
  "tasks": [{
    "name": "Extract relationships as FK constraints",
    "description": "Extract all table relationships from the Power BI model and generate Unity Catalog foreign key DDL",
    "tool_ids": [75],
    "tool_config": {
      "75": {
        "workspace_id": "{workspace_id}",
        "dataset_id": "{dataset_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "target_catalog": "my_catalog",
        "target_schema": "metrics"
      }
    }
  }]
}
```

---

## Example output

```sql
-- Relationship: Sales[CustomerID] to Customer[CustomerID]
ALTER TABLE my_catalog.metrics.fact_sales
ADD CONSTRAINT fk_fact_sales_customer_id_dim_customer
FOREIGN KEY (customer_id)
REFERENCES my_catalog.metrics.dim_customer(customer_id)
NOT ENFORCED;

-- Relationship: Sales[ProductKey] to Product[ProductKey]
ALTER TABLE my_catalog.metrics.fact_sales
ADD CONSTRAINT fk_fact_sales_product_key_dim_product
FOREIGN KEY (product_key)
REFERENCES my_catalog.metrics.dim_product(product_key)
NOT ENFORCED;
```

---

## In the UCMV pipeline

Tool 75 output (`relationships_json`) is **optional but highly recommended** for Tool 86. When provided:
- Tool 86 automatically detects `enrichment_joins` (LEFT JOINs to dimension tables)
- The `join_key_map` in `pipeline_config.json` gets auto-populated
- Reduces manual config work by roughly 20 to 30 percent

---

## Notes

- FK constraints in Unity Catalog are `NOT ENFORCED`; they don't prevent bad data but provide metadata and enable lineage
- System tables like `LocalDateTable_*` are skipped by default; they are PBI internal tables with no equivalent in Databricks
- Inactive relationships (used in specific CALCULATE/USERELATIONSHIP DAX) are excluded by default; enable `include_inactive: true` if needed

## See also

- [Power BI integration hub](./README.md)
- [Authentication and service principal setup](./01-authentication-setup.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
