# Power BI Hierarchies Tool

**Tool ID: 76** | **SVP Required: Non-Admin API** | **Fabric Only**

Extracts hierarchies from Microsoft Fabric semantic models and generates Unity Catalog dimension views with hierarchy paths.

---

## Overview

The Power BI Hierarchies Tool uses the **Fabric API** `getDefinition` endpoint to extract hierarchy definitions in TMDL format, then generates:

- **Dimension views** with `hierarchy_path` column for drill-down
- **Metadata table** (`_metadata_hierarchies`) with hierarchy definitions

### Important Limitation

This tool works with **Microsoft Fabric workspaces only**. It does not support legacy Power BI Service workspaces that haven't been migrated to Fabric.

### Output

| Output Type | Description |
|-------------|-------------|
| Dimension Views | `CREATE VIEW` with hierarchy columns and path |
| Metadata Table | `CREATE TABLE _metadata_hierarchies` with definitions |

### What Gets Extracted

| Fabric Element | Output |
|----------------|--------|
| Hierarchy name | View name and metadata |
| Hierarchy levels | Individual columns + path column |
| Level order | ORDER BY clause in view |
| Hidden status | Metadata flag |

### What This Tool Does NOT Extract

- DAX Measures → Use [Measure Conversion Pipeline](./tool-measure-conversion.md)
- M-Query table sources → Use [M-Query Conversion Pipeline](./tool-mquery-conversion.md)
- Relationships → Use [Relationships Tool](./tool-relationships-conversion.md)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Power BI Hierarchies Tool                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │  Fabric API  │    │    TMDL      │    │  Dimension View  │ │
│  │ getDefinition│ ─→ │   Parser     │ ─→ │   Generator      │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│        │                    │                    │             │
│        ▼                    ▼                    ▼             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │    TMDL      │    │  Hierarchy   │    │  CREATE VIEW     │ │
│  │   Format     │    │  Extraction  │    │  + Metadata      │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| PowerBIHierarchiesTool | `src/tools/powerbi_hierarchies_tool.py` | Main tool implementation |
| TMDLParser | (inline) | Parses TMDL hierarchy definitions |
| AadService | `src/converters/services/powerbi/authentication.py` | OAuth authentication |

### Data Flow

1. **Authenticate**: Service Principal → Get access token
2. **Fetch**: Fabric API `getDefinition` → Get TMDL content
3. **Parse**: Extract hierarchy blocks → Get levels and columns
4. **Generate**: Create dimension views and metadata DDL

---

## API Integration

### CrewAI Tool Usage

```python
# In crew configuration
{
    "tools": ["Power BI Hierarchies Tool"],
    "tool_config": {
        "76": {
            "mode": "dynamic",
            "workspace_id": "{workspace_id}",
            "dataset_id": "{dataset_id}"
        }
    }
}
```

### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `workspace_id` | Yes | - | Fabric workspace GUID |
| `dataset_id` | Yes | - | Semantic model GUID |
| `tenant_id` | Yes | - | Azure AD tenant GUID |
| `client_id` | Yes | - | Service Principal client ID |
| `client_secret` | Yes | - | Service Principal secret |

#### Output Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_catalog` | "main" | Unity Catalog name |
| `target_schema` | "default" | Schema name |
| `skip_system_tables` | true | Skip system-generated hierarchies |
| `include_hidden` | false | Include hidden hierarchies |

### Example Configuration

```json
{
  "workspace_id": "fabric-workspace-guid",
  "dataset_id": "semantic-model-guid",
  "tenant_id": "your-tenant-id",
  "client_id": "your-sp-client-id",
  "client_secret": "your-sp-secret",

  "target_catalog": "main",
  "target_schema": "default",
  "include_hidden": false
}
```

### Example Output

**Power BI Hierarchy (TMDL):**
```
hierarchy 'Geography Hierarchy'
    level Country
        column: Country
    level City
        column: City
    level PostalCode
        column: PostalCode
```

**Output: Dimension View**
```sql
-- Dimension View for hierarchy: Geography Hierarchy
-- Source table: Customer
CREATE OR REPLACE VIEW main.default.dim_customer_geography AS
SELECT DISTINCT
    Country,
    City,
    PostalCode,
    CONCAT(Country, ' > ', City, ' > ', PostalCode) AS hierarchy_path
FROM main.default.customer
ORDER BY Country, City, PostalCode;
```

**Output: Metadata Table**
```sql
CREATE TABLE IF NOT EXISTS main.default._metadata_hierarchies (
    hierarchy_name STRING,
    table_name STRING,
    level_ordinal INT,
    level_name STRING,
    column_name STRING,
    is_hidden BOOLEAN
);

INSERT INTO main.default._metadata_hierarchies VALUES
('Geography Hierarchy', 'Customer', 0, 'Country', 'Country', false),
('Geography Hierarchy', 'Customer', 1, 'City', 'City', false),
('Geography Hierarchy', 'Customer', 2, 'PostalCode', 'PostalCode', false);
```

---

## Example Crews

Reference crews are available in the examples directory:

| File | Description |
|------|-------------|
| `crew_hierarchyconverter_static.json` | Static mode - credentials in UI |
| `crew_hierarchyconverter_dynamic.json` | Dynamic mode - credentials at runtime |

### Static Mode

Credentials configured in tool settings. Best for:
- Single semantic model migrations
- Testing and development

### Dynamic Mode

Credentials provided at execution. Best for:
- Multi-model migrations
- Production pipelines
- Reusable templates

**Dynamic Mode Input Example:**
```json
{
  "execution_inputs": {
    "workspace_id": "fabric-workspace-guid",
    "dataset_id": "semantic-model-guid",
    "tenant_id": "tenant-guid",
    "client_id": "sp-client-id",
    "client_secret": "sp-secret"
  }
}
```

---

## Using Dimension Views in BI Tools

### In Databricks SQL

```sql
-- Query with hierarchy path for drill-down
SELECT
    hierarchy_path,
    COUNT(*) as count
FROM main.default.dim_customer_geography
GROUP BY hierarchy_path
ORDER BY hierarchy_path;
```

### In Tableau/Power BI

The `hierarchy_path` column can be used to create drill-down visualizations:
1. Split by delimiter (`>`)
2. Create calculated hierarchy from path components
3. Use for tree/hierarchy visualizations

---

## Troubleshooting

### "Fabric API Not Available" / "getDefinition Failed"
- This tool requires **Microsoft Fabric** workspace
- Legacy Power BI Service workspaces are not supported
- Verify workspace has been migrated to Fabric

### "Unauthorized" Error
- Service Principal needs `SemanticModel.ReadWrite.All` permission
- Check Azure AD app registration permissions
- Ensure admin consent granted

### "No Hierarchies Found"
- Verify semantic model has hierarchies defined
- Check if hierarchies are hidden (set `include_hidden: true`)
- System-generated hierarchies are skipped by default

### TMDL Parsing Errors
- Some TMDL features may not be fully supported
- Review raw TMDL in logs for debugging
- Complex expressions may need manual review

---

## Related Documentation

- [Power BI Tools Guide](./powerbi-tools-guide.md) - SVP setup and overview
- [Measure Conversion Pipeline](./tool-measure-conversion.md) - DAX measure conversion
- [M-Query Conversion Pipeline](./tool-mquery-conversion.md) - Table source conversion
- [Relationships Tool](./tool-relationships-conversion.md) - FK constraints
