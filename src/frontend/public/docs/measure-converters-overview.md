# Measure Converters - Overview

## Introduction

The Kasal Measure Conversion system enables seamless migration and transformation of business metrics between different BI platforms and formats. This system provides both **specialized converters** for specific workflows and a **universal pipeline** for flexible conversions.

## Architecture

### Three-Layer Design

```
┌──────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                          │
│  CrewAI Tools: Universal Pipeline, Specialized Converters    │
└────────────────────┬─────────────────────────────────────────┘
                     │
┌────────────────────┼─────────────────────────────────────────┐
│                    ↓           PIPELINE LAYER                 │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  Inbound Connectors  →  KPIDefinition  →  Outbound     │ │
│  │  (Extract)               (Transform)       (Generate)   │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                     │
┌────────────────────┼─────────────────────────────────────────┐
│                    ↓           CONVERTER LAYER                │
│  Inbound:          │          Outbound:                       │
│  • PowerBI         │          • DAX Generator                 │
│  • YAML            │          • SQL Generator (multi-dialect) │
│  • Tableau*        │          • UC Metrics Generator          │
│  • Excel*          │          • YAML Exporter                 │
└──────────────────────────────────────────────────────────────┘

* Coming Soon
```

### Standard Intermediate Format: KPIDefinition

All conversions flow through a standard intermediate representation:

```python
KPIDefinition {
  technical_name: str
  description: str
  kpis: List[KPI]           # List of measures/metrics
  filters: List[Filter]      # Global filters
  query_filters: List[QueryFilter]  # Query-level filters
  default_variables: Dict    # Variable definitions
  structures: Dict           # Time intelligence structures
}
```

This design enables:
- **Extensibility**: Add new sources/targets without changing existing code
- **Consistency**: All converters use the same intermediate format
- **Flexibility**: Mix and match any inbound/outbound combination

## Available Tools

### 1. Universal Measure Conversion Pipeline (ID: 74)

**Best For**: Flexible conversions between any supported formats

**Capabilities**:
- **Inbound**: Power BI, YAML (Tableau, Excel coming soon)
- **Outbound**: DAX, SQL (7 dialects), UC Metrics, YAML

**Use When**:
- You need to convert between different platforms
- You want a single tool for all conversion needs
- You need flexibility in source/target selection

**See**: [Measure Conversion Pipeline Guide](./measure-conversion-pipeline-guide.md)

### 2. Specialized Converters

#### YAMLToDAXTool (ID: 71)

**Best For**: Generating Power BI measures from YAML definitions

**Input**: YAML KPI definition file
**Output**: DAX measures with time intelligence

**Use When**:
- You have standardized YAML metric definitions
- You want to automate Power BI measure creation
- You need consistent DAX patterns across models

#### YAMLToSQLTool (ID: 72)

**Best For**: Generating SQL queries from business logic definitions

**Input**: YAML KPI definition file
**Output**: SQL queries (7 dialect support)

**Supported Dialects**:
- Databricks
- PostgreSQL
- MySQL
- SQL Server
- Snowflake
- BigQuery
- Standard SQL

**Use When**:
- You want to maintain business logic as code (YAML)
- You need SQL queries for multiple database platforms
- You're building a metrics layer

#### YAMLToUCMetricsTool (ID: 73)

**Best For**: Deploying metrics to Databricks Unity Catalog

**Input**: YAML KPI definition file
**Output**: Unity Catalog Metrics Store definition

**Use When**:
- You're using Databricks Unity Catalog
- You want centralized metric governance
- You need lineage tracking for business metrics

#### PowerBIConnectorTool

**Best For**: Extracting measures from Power BI datasets

**Input**: Power BI connection details (dataset ID, workspace ID, access token)
**Output**: Measures in DAX, SQL, UC Metrics, or YAML format

**Use When**:
- You need to document existing Power BI measures
- You're migrating from Power BI to another platform
- You want to export Power BI logic for reuse

## Comparison Matrix

| Tool | Inbound | Outbound | Best Use Case |
|------|---------|----------|---------------|
| **Universal Pipeline** | Power BI, YAML | DAX, SQL, UC Metrics, YAML | Flexible conversions |
| **YAMLToDAXTool** | YAML only | DAX only | YAML → Power BI workflow |
| **YAMLToSQLTool** | YAML only | SQL only | YAML → SQL databases |
| **YAMLToUCMetricsTool** | YAML only | UC Metrics only | YAML → Databricks governance |
| **PowerBIConnectorTool** | Power BI only | All formats | Power BI extraction |

## Common Workflows

### 1. Power BI → Databricks Migration

**Scenario**: Migrate Power BI semantic model to Databricks SQL

**Tool**: Universal Pipeline or PowerBIConnectorTool

**Steps**:
1. Extract measures from Power BI dataset
2. Convert to Databricks SQL dialect
3. Review and deploy SQL queries

**Configuration**:
```json
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "dataset-id",
  "powerbi_group_id": "workspace-id",
  "powerbi_access_token": "token",

  "outbound_format": "sql",
  "sql_dialect": "databricks"
}
```

### 2. YAML-Driven Metric Definitions

**Scenario**: Maintain metrics as YAML, generate for multiple platforms

**Tools**: YAMLToDAXTool, YAMLToSQLTool, YAMLToUCMetricsTool

**Steps**:
1. Define metrics in YAML (source of truth)
2. Generate DAX for Power BI
3. Generate SQL for data warehouse
4. Generate UC Metrics for Databricks governance

**Benefits**:
- Single source of truth for business logic
- Version control for metrics (Git)
- Consistent definitions across platforms
- Automated generation reduces errors

### 3. Multi-Platform Analytics

**Scenario**: Support metrics across Power BI, Tableau, and Databricks

**Tool**: Universal Pipeline

**Steps**:
1. Extract from any source (Power BI, YAML)
2. Convert to intermediate YAML format (documentation)
3. Generate platform-specific outputs (DAX, SQL)
4. Maintain YAML as canonical reference

### 4. Databricks Unity Catalog Governance

**Scenario**: Centralize metric definitions in Unity Catalog

**Tool**: YAMLToUCMetricsTool or Universal Pipeline

**Steps**:
1. Define or extract metrics
2. Generate UC Metrics definitions
3. Deploy to Unity Catalog
4. Enable lineage tracking and governance

## Technical Details

### Supported SQL Dialects

| Dialect | Platform | Notes |
|---------|----------|-------|
| `databricks` | Databricks SQL | Optimized for Databricks |
| `postgresql` | PostgreSQL | Standard PostgreSQL syntax |
| `mysql` | MySQL | MySQL-specific functions |
| `sqlserver` | SQL Server | T-SQL compatibility |
| `snowflake` | Snowflake | Snowflake SQL syntax |
| `bigquery` | Google BigQuery | BigQuery Standard SQL |
| `standard` | Generic SQL | ANSI SQL standard |

### Time Intelligence Support

All converters support time intelligence structures:

- **Year-to-Date (YTD)**
- **Quarter-to-Date (QTD)**
- **Month-to-Date (MTD)**
- **Rolling Periods** (12-month, 90-day, etc.)
- **Prior Period Comparisons** (YoY, MoM, etc.)

### DAX Expression Parsing

Power BI connector includes sophisticated DAX parser:

- Extracts aggregation functions (SUM, AVERAGE, COUNT, etc.)
- Identifies filter contexts (CALCULATE, FILTER)
- Parses time intelligence functions
- Handles nested expressions
- Resolves table and column references

### Authentication

#### Power BI
- **OAuth 2.0 Access Token** (required)
- Supports service principal and user-based authentication
- Token must have read permissions on dataset

#### Databricks (UC Metrics)
- Uses workspace default authentication
- Requires Unity Catalog access
- Honors catalog/schema permissions

## Best Practices

### 1. Use YAML as Source of Truth

**Recommendation**: Maintain business metric definitions in YAML

**Benefits**:
- Version control with Git
- Code review process for metrics
- Documentation embedded in code
- Platform-agnostic definitions
- Easy to test and validate

### 2. Standardize Naming Conventions

**Recommendation**: Use consistent naming across platforms

**Example**:
```yaml
kpis:
  - technical_name: total_revenue
    display_name: "Total Revenue"
    # Same name used in DAX, SQL, UC Metrics
```

### 3. Document Business Logic

**Recommendation**: Include descriptions and metadata

**Example**:
```yaml
kpis:
  - technical_name: customer_lifetime_value
    display_name: "Customer Lifetime Value"
    description: "Average revenue per customer over their entire relationship"
    business_owner: "Sales Analytics Team"
    update_frequency: "Daily"
```

### 4. Test Conversions

**Recommendation**: Validate generated output before deployment

- Compare results between platforms
- Test with sample data
- Review generated SQL/DAX for correctness
- Use version control for generated outputs

### 5. Leverage Time Intelligence

**Recommendation**: Use built-in time intelligence processing

- Enable structure processing (`process_structures: true`)
- Define time intelligence patterns in YAML
- Let converters generate platform-specific time logic
- Reduces manual coding errors

## Extending the System

### Adding New Inbound Connectors

1. Create connector class inheriting from `BaseInboundConnector`
2. Implement `connect()`, `extract_measures()`, `disconnect()`
3. Return standardized `KPIDefinition`
4. Register in `ConnectorType` enum
5. Add to pipeline factory

### Adding New Outbound Formats

1. Create generator class
2. Accept `KPIDefinition` as input
3. Generate target format output
4. Add to `OutboundFormat` enum
5. Add to pipeline converter selection

## Related Documentation

- [Measure Conversion Pipeline Guide](./measure-conversion-pipeline-guide.md) - Detailed guide for Universal Pipeline
- [YAML KPI Schema](./yaml-kpi-schema.md) - YAML format specification
- [Power BI Integration](./powerbi-integration.md) - Power BI connector details
- [SQL Generator](./sql-generator.md) - SQL conversion details

## Support

For issues or questions:
- Check the documentation above
- Review error messages and troubleshooting sections
- Consult the converter-specific guides
- Review example configurations in the guides
