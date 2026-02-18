# Power BI Relationships Tool

**Tool ID: 75** | **SVP Required: Non-Admin API**

Extracts relationships from Power BI semantic models and generates Unity Catalog Foreign Key constraint statements.

---

## Overview

The Power BI Relationships Tool uses the **Execute Queries API** with the `INFO.VIEW.RELATIONSHIPS()` DAX function to extract all relationships defined in a Power BI semantic model, then generates `ALTER TABLE ... ADD CONSTRAINT` statements for Unity Catalog.

### Output

- **Foreign Key constraints** (NOT ENFORCED) for Unity Catalog tables
- **Relationship metadata** including cardinality and cross-filtering behavior
- **SQL DDL** ready to execute in Databricks

### What Gets Extracted

| Power BI Element | Output |
|------------------|--------|
| One-to-Many relationships | FK constraint (NOT ENFORCED) |
| Many-to-One relationships | FK constraint (NOT ENFORCED) |
| Many-to-Many relationships | FK constraint (NOT ENFORCED) |
| Cross-filtering direction | SQL comment |
| Active/Inactive status | SQL comment |

### What This Tool Does NOT Extract

- DAX Measures → Use [Measure Conversion Pipeline](./tool-measure-conversion.md)
- M-Query table sources → Use [M-Query Conversion Pipeline](./tool-mquery-conversion.md)
- Hierarchies → Use [Hierarchies Tool](./tool-hierarchies-conversion.md)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  Power BI Relationships Tool                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │ Execute      │    │ Relationship │    │ FK Constraint    │ │
│  │ Queries API  │ ─→ │   Parser     │ ─→ │   Generator      │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│        │                    │                    │             │
│        ▼                    ▼                    ▼             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │ INFO.VIEW.   │    │  Cardinality │    │  ALTER TABLE     │ │
│  │ RELATIONSHIPS│    │  Detection   │    │  ADD CONSTRAINT  │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| PowerBIRelationshipsTool | `src/tools/powerbi_relationships_tool.py` | Main tool implementation |
| AadService | `src/converters/services/powerbi/authentication.py` | OAuth authentication |
| DAXQueryExecutor | (inline) | Executes INFO.VIEW.RELATIONSHIPS() |

### Data Flow

1. **Authenticate**: Service Principal → Get access token
2. **Query**: Execute `EVALUATE INFO.VIEW.RELATIONSHIPS()` DAX
3. **Parse**: Extract table names, columns, cardinality
4. **Generate**: Create ALTER TABLE ADD CONSTRAINT DDL

---

## API Integration

### CrewAI Tool Usage

```python
# In crew configuration
{
    "tools": ["Power BI Relationships Tool"],
    "tool_config": {
        "75": {
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
| `workspace_id` | Yes | - | Power BI workspace GUID |
| `dataset_id` | Yes | - | Semantic model GUID |
| `tenant_id` | Yes | - | Azure AD tenant GUID |
| `client_id` | Yes | - | Service Principal client ID |
| `client_secret` | Yes | - | Service Principal secret |

#### Output Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_catalog` | "main" | Unity Catalog name |
| `target_schema` | "default" | Schema name |
| `include_inactive` | false | Include inactive relationships |
| `skip_system_tables` | true | Skip LocalDateTable etc. |

### Example Configuration

```json
{
  "workspace_id": "workspace-guid",
  "dataset_id": "dataset-guid",
  "tenant_id": "your-tenant-id",
  "client_id": "your-sp-client-id",
  "client_secret": "your-sp-secret",

  "target_catalog": "main",
  "target_schema": "default",
  "include_inactive": false
}
```

### Example Output

**Power BI Relationship:**
```
Sales[CustomerID] → Customer[CustomerID]
Cardinality: Many-to-One
Cross-filtering: Single
Active: Yes
```

**Output SQL:**
```sql
-- Relationship: sales_to_customer
-- Cardinality: Many-to-One
-- Cross-filtering: Single
-- Active: true
ALTER TABLE main.default.sales
ADD CONSTRAINT fk_sales_customer_id_customer
FOREIGN KEY (customer_id)
REFERENCES main.default.customer(customer_id)
NOT ENFORCED;
```

---

## Example Crews

Reference crews are available in the examples directory:

| File | Description |
|------|-------------|
| `crew_relationshipconverter_static.json` | Static mode - credentials in UI |
| `crew_relationshipconverter_dynamic.json` | Dynamic mode - credentials at runtime |

### Static Mode

Credentials configured in tool settings. Best for:
- Single dataset migrations
- Testing and development

### Dynamic Mode

Credentials provided at execution. Best for:
- Multi-dataset migrations
- Production pipelines
- Reusable templates

**Dynamic Mode Input Example:**
```json
{
  "execution_inputs": {
    "workspace_id": "workspace-guid",
    "dataset_id": "dataset-guid",
    "tenant_id": "tenant-guid",
    "client_id": "sp-client-id",
    "client_secret": "sp-secret"
  }
}
```

---

## Unity Catalog FK Constraints

### NOT ENFORCED

Unity Catalog FK constraints are **informational only** (NOT ENFORCED). They:
- Document the relationship between tables
- Enable query optimization hints
- Support BI tool integration
- Do **not** validate data integrity at write time

### Usage in Databricks

```sql
-- View existing FK constraints
SHOW CONSTRAINTS ON main.default.sales;

-- Drop a constraint if needed
ALTER TABLE main.default.sales
DROP CONSTRAINT fk_sales_customer_id_customer;
```

---

## Troubleshooting

### "Unauthorized" Error
- Verify Service Principal is added as **workspace member**
- This tool uses Execute Queries API (not Admin API)
- Check permissions: `Dataset.Read.All` required

### "No Data Returned"
- Verify the semantic model has relationships defined
- Check if RLS is blocking the Service Principal
- Consider using a Service Account if RLS is in place

### "Table Not Found" in Generated DDL
- Table names may differ between Power BI and Unity Catalog
- Review and adjust table names in output before execution

### Inactive Relationships Not Showing
- Set `include_inactive: true` in configuration
- Inactive relationships are skipped by default

---

## Related Documentation

- [Power BI Tools Guide](./powerbi-tools-guide.md) - SVP setup and overview
- [Measure Conversion Pipeline](./tool-measure-conversion.md) - DAX measure conversion
- [M-Query Conversion Pipeline](./tool-mquery-conversion.md) - Table source conversion
- [Hierarchies Tool](./tool-hierarchies-conversion.md) - Dimension views
