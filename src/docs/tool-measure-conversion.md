# Measure Conversion Pipeline

**Tool ID: 73** | **SVP Required: Non-Admin API**

Converts DAX measures (business metrics) from Power BI semantic models to multiple output formats.

---

## Overview

The Measure Conversion Pipeline extracts **DAX measures** (calculated KPIs) from Power BI datasets using the Execute Queries API and converts them to:

- **SQL** (Databricks, PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery)
- **Unity Catalog Metrics** (UC Metrics Store YAML)
- **DAX** (reformatted/documented)
- **YAML** (portable KPI definitions)

### What Gets Converted

| Power BI Element | Output |
|------------------|--------|
| DAX Measures | SQL aggregations, UC Metrics definitions |
| Time Intelligence | Date functions for target dialect |
| Filter Contexts | WHERE clauses, filter parameters |
| Calculated Fields | SQL expressions |

### What This Tool Does NOT Convert

- M-Query table sources → Use [M-Query Conversion Pipeline](./tool-mquery-conversion.md)
- Relationships → Use [Relationships Tool](./tool-relationships-conversion.md)
- Hierarchies → Use [Hierarchies Tool](./tool-hierarchies-conversion.md)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Measure Conversion Pipeline                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │
│  │   Inbound    │    │ KPIDefinition │    │    Outbound      │ │
│  │  Connector   │ ─→ │  (Internal)   │ ─→ │    Generator     │ │
│  └──────────────┘    └──────────────┘    └──────────────────┘ │
│        │                                          │             │
│        ▼                                          ▼             │
│  ┌──────────────┐                        ┌──────────────────┐ │
│  │  Power BI    │                        │  DAX / SQL /     │ │
│  │  YAML        │                        │  UC Metrics      │ │
│  └──────────────┘                        └──────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| PowerBIConnector | `src/converters/services/powerbi/` | Extracts DAX measures via Execute Queries API |
| YAMLConnector | `src/converters/inbound/yaml/` | Loads measures from YAML files |
| DAXGenerator | `src/converters/outbound/dax/` | Generates DAX output |
| SQLGenerator | `src/converters/outbound/sql/` | Generates SQL for 7 dialects |
| UCMetricsGenerator | `src/converters/outbound/uc_metrics/` | Generates UC Metrics YAML |

### Data Flow

1. **Extract**: Connect to Power BI → Execute `INFO.MEASURES()` DAX query → Get measure definitions
2. **Transform**: Parse DAX expressions → Identify aggregations, filters, time intelligence
3. **Generate**: Convert to target format → Apply dialect-specific syntax

---

## API Integration

### CrewAI Tool Usage

```python
# In crew configuration
{
    "tools": ["Measure Conversion Pipeline"],
    "tool_config": {
        "73": {
            "mode": "dynamic",  # or "static"
            "inbound_connector": "powerbi",
            "outbound_format": "sql",
            "sql_dialect": "databricks"
        }
    }
}
```

### Configuration Parameters

#### Inbound: Power BI

| Parameter | Required | Description |
|-----------|----------|-------------|
| `powerbi_semantic_model_id` | Yes | Dataset/semantic model GUID |
| `powerbi_group_id` | Yes | Workspace GUID |
| `powerbi_tenant_id` | Yes | Azure AD tenant GUID |
| `powerbi_client_id` | Yes | Service Principal client ID |
| `powerbi_client_secret` | Yes | Service Principal secret |
| `powerbi_include_hidden` | No | Include hidden measures (default: false) |
| `powerbi_filter_pattern` | No | Regex to filter measure names |

#### Inbound: YAML

| Parameter | Required | Description |
|-----------|----------|-------------|
| `yaml_content` | Yes* | YAML content as string |
| `yaml_file_path` | Yes* | Path to YAML file |

*One of `yaml_content` or `yaml_file_path` required

#### Outbound: SQL

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sql_dialect` | "databricks" | Target SQL dialect |
| `sql_include_comments` | true | Include descriptive comments |
| `sql_process_structures` | true | Process time intelligence |

#### Outbound: UC Metrics

| Parameter | Default | Description |
|-----------|---------|-------------|
| `uc_catalog` | "main" | Unity Catalog name |
| `uc_schema` | "default" | Schema name |
| `uc_process_structures` | true | Process time intelligence |

### Example: Power BI to Databricks SQL

```json
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "abc-123-def",
  "powerbi_group_id": "workspace-456",
  "powerbi_tenant_id": "your-tenant-id",
  "powerbi_client_id": "your-client-id",
  "powerbi_client_secret": "your-secret",

  "outbound_format": "sql",
  "sql_dialect": "databricks",
  "sql_include_comments": true
}
```

### Example Output

**Input DAX:**
```dax
Total Sales = SUM(Sales[Amount])
```

**Output SQL (Databricks):**
```sql
-- Measure: Total Sales
-- Original DAX: SUM(Sales[Amount])
SELECT SUM(Amount) AS total_sales
FROM catalog.schema.sales
```

---

## Example Crews

Reference crews are available in the examples directory:

| File | Description |
|------|-------------|
| `crew_measureconverter_static.json` | Static mode - credentials in UI |
| `crew_measureconverter_dynamic.json` | Dynamic mode - credentials at runtime |

### Static Mode

Credentials are configured in the tool settings UI. Best for:
- Single workspace/dataset migrations
- Testing and development
- Simple one-off conversions

### Dynamic Mode

Credentials are provided as execution inputs. Best for:
- Multi-workspace migrations
- Production pipelines
- Reusable crew templates

**Dynamic Mode Example:**
```json
{
  "execution_inputs": {
    "workspace_id": "workspace-guid",
    "dataset_id": "dataset-guid",
    "tenant_id": "tenant-guid",
    "client_id": "client-guid",
    "client_secret": "secret-value"
  }
}
```

---

## Troubleshooting

### "Unauthorized" Error
- Verify Service Principal is added as workspace member
- Check API permissions: `Dataset.Read.All` required
- Ensure admin consent granted for tenant permissions

### Empty Results
- Check if dataset contains measures (not just columns)
- Try with `powerbi_include_hidden: true`
- Verify `powerbi_filter_pattern` isn't too restrictive

### Time Intelligence Not Converting
- Ensure `sql_process_structures: true`
- Check DAX uses standard time intelligence patterns

---

## Related Documentation

- [Power BI Tools Guide](./powerbi-tools-guide.md) - SVP setup and overview
- [M-Query Conversion Pipeline](./tool-mquery-conversion.md) - Table source conversion
- [Relationships Tool](./tool-relationships-conversion.md) - FK constraints
- [Hierarchies Tool](./tool-hierarchies-conversion.md) - Dimension views
