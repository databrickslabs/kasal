# Tool 76 - Power BI hierarchies tool

**What it is:** Extracts hierarchy definitions from a Microsoft Fabric semantic model and generates Unity Catalog dimension views with `hierarchy_path` columns.

---

## Why it exists

Hierarchies (e.g. Country to Region to City, or Year to Quarter to Month) are a core Power BI UX feature that enables drill-down in reports. When migrating, these hierarchies need to be represented in Unity Catalog so downstream BI tools can reproduce the same drill-down behavior.

## What problem it solves

- **Drill-down preservation:** Converts PBI hierarchy definitions into SQL views with concatenated `hierarchy_path` columns that downstream tools can filter on
- **Documentation:** Generates `_metadata_hierarchies` tables documenting all hierarchy levels
- **Migration completeness:** Hierarchies are often overlooked in manual migrations

---

## Fabric-only requirement

This tool uses the Fabric API `getDefinition` endpoint which returns semantic model definitions in TMDL (Tabular Model Definition Language) format. **This only works with Microsoft Fabric workspaces**, not classic Power BI Service workspaces.

If your customer is on legacy PBI Service (not Fabric), this tool will not work for them.

---

## Microsoft API reference

Uses: `GET /groups/{groupId}/items/{itemId}/getDefinition` (Fabric REST API)
Docs: [Fabric - Get Item Definition](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition)

---

## Authentication

**Non-Admin SP** with `SemanticModel.ReadWrite.All` permission (Fabric-specific).
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
| `include_hidden` | No | Include hidden hierarchy levels (default: `false`) |
| `skip_system_tables` | No | Skip system tables (default: `true`) |

---

## Example crew

```json
{
  "name": "PBI Hierarchy Migration",
  "tasks": [{
    "name": "Extract hierarchies and generate dimension views",
    "description": "Extract all hierarchy definitions from the Fabric semantic model and generate UC dimension views",
    "tool_ids": [76],
    "tool_config": {
      "76": {
        "workspace_id": "{fabric_workspace_id}",
        "dataset_id": "{semantic_model_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "target_catalog": "my_catalog",
        "target_schema": "dimensions"
      }
    }
  }]
}
```

---

## Example output

```sql
-- Dimension View: Geography hierarchy
CREATE OR REPLACE VIEW my_catalog.dimensions.dim_geography_hierarchy AS
SELECT DISTINCT
  Country,
  Region,
  City,
  CONCAT(Country, ' > ', Region, ' > ', City) AS hierarchy_path
FROM my_catalog.raw.dim_geography
ORDER BY Country, Region, City;

-- Metadata table
INSERT INTO my_catalog.dimensions._metadata_hierarchies VALUES
  ('Geography', 'dim_geography', 0, 'Country', 'Country', false),
  ('Geography', 'dim_geography', 1, 'Region', 'Region', false),
  ('Geography', 'dim_geography', 2, 'City', 'City', false);
```

---

## Notes

- Not in the standard UCMV pipeline (Tools 73 to 86); run this separately if the customer needs drill-down hierarchies in UC
- The `_metadata_hierarchies` table is useful for BI tools that can read hierarchy metadata programmatically
- Hidden levels (used in PBI for internal calculations) are excluded by default

## See also

- [Power BI integration hub](./README.md)
- [Authentication and service principal setup](./01-authentication-setup.md)
- [Tool 77 - field parameters and calculation groups](./tool-77-field-parameters.md)
- [Tool 78 - report references tool](./tool-78-report-references.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
