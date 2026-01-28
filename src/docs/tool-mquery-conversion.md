# M-Query Conversion Pipeline

**Tool ID: 74** | **SVP Required: Admin API**

Extracts M-Query (Power Query) table source expressions from Power BI semantic models and converts them to Databricks SQL views.

---

## Overview

The M-Query Conversion Pipeline scans Power BI workspaces using the **Admin API** to extract table definitions and their M-Query source expressions, then converts them to `CREATE VIEW` statements for Unity Catalog.

### Supported Expression Types

| M-Query Pattern | Conversion Method | Output |
|-----------------|-------------------|--------|
| `Value.NativeQuery` | Direct SQL extraction | CREATE VIEW with embedded SQL |
| `DatabricksMultiCloud.Catalogs` | Direct mapping | CREATE VIEW referencing catalog |
| `Sql.Database` | LLM conversion | CREATE VIEW with transformed SQL |
| `Table.FromRows` | Rule-based | CREATE VIEW with VALUES clause |
| `Odbc.Query` | LLM conversion | Best-effort SQL |
| `Oracle.Database` | LLM conversion | Best-effort SQL |
| `Snowflake.Databases` | LLM conversion | Best-effort SQL |
| Other expressions | LLM conversion | Best-effort SQL |

### What This Tool Does NOT Convert

- DAX Measures → Use [Measure Conversion Pipeline](./tool-measure-conversion.md)
- Relationships → Use [Relationships Tool](./tool-relationships-conversion.md)
- Hierarchies → Use [Hierarchies Tool](./tool-hierarchies-conversion.md)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   M-Query Conversion Pipeline                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │  Admin API   │    │   MQuery     │    │   SQL Views      │ │
│  │   Scanner    │ ─→ │   Parser     │ ─→ │   Generator      │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│        │                    │                    │             │
│        ▼                    ▼                    ▼             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │  Workspace   │    │  Expression  │    │  CREATE VIEW     │ │
│  │    Scan      │    │   Detection  │    │  Statements      │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│                             │                                  │
│                             ▼                                  │
│                      ┌──────────────┐                         │
│                      │ LLM Converter │ (for complex M-Query)  │
│                      └──────────────┘                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| PowerBIAdminScanner | `src/converters/services/mquery/scanner.py` | Scans workspace via Admin API |
| MQueryParser | `src/converters/services/mquery/parser.py` | Detects expression types |
| TableFromRowsConverter | `src/converters/services/mquery/parser.py` | Converts static tables |
| MQueryLLMConverter | `src/converters/services/mquery/llm_converter.py` | LLM-powered conversion |
| MQueryConnector | `src/converters/services/mquery/connector.py` | Main orchestration |

### Data Flow

1. **Scan**: Admin API → Get workspace scan → Extract table expressions
2. **Parse**: Detect expression type → Extract connection metadata
3. **Convert**:
   - Simple patterns → Rule-based conversion
   - Complex M-Query → LLM conversion
4. **Output**: Generate CREATE VIEW DDL for Unity Catalog

---

## API Integration

### CrewAI Tool Usage

```python
# In crew configuration
{
    "tools": ["M-Query Conversion Pipeline"],
    "tool_config": {
        "74": {
            "mode": "dynamic",
            "workspace_id": "{workspace_id}",
            "dataset_id": "{dataset_id}",
            "use_llm": true
        }
    }
}
```

### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `workspace_id` | Yes | - | Power BI workspace GUID |
| `dataset_id` | No | - | Specific dataset GUID (scans all if empty) |
| `tenant_id` | Yes | - | Azure AD tenant GUID |
| `client_id` | Yes | - | Admin API Service Principal client ID |
| `client_secret` | Yes | - | Admin API Service Principal secret |

#### LLM Configuration (Optional)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `llm_workspace_url` | - | Databricks workspace URL |
| `llm_token` | - | Databricks access token |
| `llm_model` | "databricks-claude-sonnet-4" | Model for complex conversions |
| `use_llm` | true | Enable LLM for complex expressions |

#### Scan Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `include_lineage` | true | Include lineage info in scan |
| `include_datasource_details` | true | Include data source details |
| `include_dataset_schema` | true | Include schema in scan |
| `include_dataset_expressions` | true | Include M-Query expressions |
| `include_hidden_tables` | false | Include hidden tables |

#### Output Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_catalog` | "main" | Unity Catalog name |
| `target_schema` | "default" | Schema name |
| `include_summary` | true | Include conversion summary |

### Example Configuration

```json
{
  "workspace_id": "workspace-guid",
  "dataset_id": "dataset-guid",
  "tenant_id": "your-tenant-id",
  "client_id": "your-admin-sp-client-id",
  "client_secret": "your-admin-sp-secret",

  "llm_workspace_url": "https://your-workspace.databricks.com",
  "llm_token": "your-databricks-token",
  "use_llm": true,

  "target_catalog": "main",
  "target_schema": "default"
}
```

### Example Output

**Input M-Query (Value.NativeQuery):**
```powerquery
let
    Source = Sql.Database("server.database.windows.net", "mydb"),
    Query = Value.NativeQuery(Source, "SELECT * FROM sales WHERE year = 2024")
in
    Query
```

**Output SQL:**
```sql
CREATE OR REPLACE VIEW main.default.sales_2024 AS
SELECT * FROM sales WHERE year = 2024;
```

**Input M-Query (Table.FromRows):**
```powerquery
let
    Source = Table.FromRows({
        {"North", "USA"},
        {"South", "USA"},
        {"West", "Europe"}
    }, type table [Region = text, Country = text])
in
    Source
```

**Output SQL:**
```sql
CREATE OR REPLACE VIEW main.default.regions AS
SELECT * FROM VALUES
  ('North', 'USA'),
  ('South', 'USA'),
  ('West', 'Europe')
AS t(Region, Country);
```

---

## Example Crews

Reference crews are available in the examples directory:

| File | Description |
|------|-------------|
| `crew_mqueryconverter_static.json` | Static mode - credentials in UI |
| `crew_mqueryconverter_dynamic.json` | Dynamic mode - credentials at runtime |

### Static Mode

Credentials configured in tool settings. Best for:
- Single workspace migrations
- Testing and development

### Dynamic Mode

Credentials provided at execution. Best for:
- Multi-workspace migrations
- Production pipelines
- Reusable templates

**Dynamic Mode Input Example:**
```json
{
  "execution_inputs": {
    "workspace_id": "workspace-guid",
    "dataset_id": "dataset-guid",
    "tenant_id": "tenant-guid",
    "client_id": "admin-sp-client-id",
    "client_secret": "admin-sp-secret"
  }
}
```

---

## Troubleshooting

### "Unauthorized" or "Admin API Access Denied"
- This tool requires **Admin API SVP** (not regular workspace member)
- Verify Service Principal has `Dataset.ReadWrite.All` permission
- Check Power BI Admin Portal → Tenant Settings → "Allow service principals to use Power BI APIs" is enabled
- Ensure SP is in the allowed security group

### Empty Results / No Tables Found
- Verify dataset has tables with M-Query expressions
- Check `include_hidden_tables` if tables are hidden
- Ensure the scan completed successfully (check logs)

### LLM Conversion Failures
- Verify Databricks workspace URL and token
- Check model availability (`databricks-claude-sonnet-4`)
- For complex M-Query, manual review may be needed

### Table.FromRows Not Converting
- Verify expression type is detected correctly
- Check table has column schema defined
- Review parser output in logs

---

## Related Documentation

- [Power BI Tools Guide](./powerbi-tools-guide.md) - SVP setup (Admin vs Non-Admin)
- [Measure Conversion Pipeline](./tool-measure-conversion.md) - DAX measure conversion
- [Relationships Tool](./tool-relationships-conversion.md) - FK constraints
- [Hierarchies Tool](./tool-hierarchies-conversion.md) - Dimension views
