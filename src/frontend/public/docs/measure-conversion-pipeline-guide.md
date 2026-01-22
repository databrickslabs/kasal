# Measure Conversion Pipeline - User Guide

## Overview

The **Measure Conversion Pipeline** is a universal converter that transforms business metrics and measures between different BI platforms and formats. It provides a simple dropdown-based UX where you select:

- **FROM** (Inbound Connector): Source system or format
- **TO** (Outbound Format): Target format or platform

## Quick Start

### Basic Workflow

1. **Select Inbound Connector** (`inbound_connector`): Choose your source
   - `powerbi` - Extract from Power BI datasets via REST API
   - `yaml` - Load from YAML definition files
   - *Coming Soon*: `tableau`, `excel`, `looker`

2. **Select Outbound Format** (`outbound_format`): Choose your target
   - `dax` - Power BI / Analysis Services measures
   - `sql` - SQL queries (multiple dialects supported)
   - `uc_metrics` - Databricks Unity Catalog Metrics Store
   - `yaml` - Portable YAML definition format

3. **Configure Source-Specific Parameters**: Provide authentication and connection details

4. **Configure Target-Specific Parameters**: Set output preferences (dialect, catalog, etc.)

5. **Execute**: Run the conversion pipeline

## Inbound Connectors (FROM)

### Power BI (`powerbi`)

Extract measures from Power BI datasets using the REST API.

**Required Parameters:**
- `powerbi_semantic_model_id` - Dataset/semantic model ID
- `powerbi_group_id` - Workspace ID
- `powerbi_tenant_id` - Azure AD tenant ID
- `powerbi_client_id` - Application/Client ID
- `powerbi_client_secret` - Client secret

**Optional Parameters:**
- `powerbi_info_table_name` - Name of Info Measures table (default: "Info Measures")
- `powerbi_include_hidden` - Include hidden measures (default: false)
- `powerbi_filter_pattern` - Regex pattern to filter measure names

**Example:**
```json
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "abc-123-def",
  "powerbi_group_id": "workspace-456",
  "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
  "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
  "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
  "powerbi_include_hidden": false
}
```

### YAML (`yaml`)

Load measures from YAML KPI definition files.

**Required Parameters:**
- `yaml_content` - YAML content as string, OR
- `yaml_file_path` - Path to YAML file

**Example:**
```json
{
  "inbound_connector": "yaml",
  "yaml_file_path": "/path/to/kpis.yaml"
}
```

## Outbound Formats (TO)

### DAX (`dax`)

Generate Power BI / Analysis Services measures with DAX formulas.

**Optional Parameters:**
- `dax_process_structures` - Process time intelligence structures (default: true)

**Output:** List of DAX measures with names, expressions, and descriptions

**Example:**
```json
{
  "outbound_format": "dax",
  "dax_process_structures": true
}
```

### SQL (`sql`)

Generate SQL queries compatible with multiple database platforms.

**Optional Parameters:**
- `sql_dialect` - SQL dialect (default: "databricks")
  - Supported: `databricks`, `postgresql`, `mysql`, `sqlserver`, `snowflake`, `bigquery`, `standard`
- `sql_include_comments` - Include descriptive comments (default: true)
- `sql_process_structures` - Process time intelligence structures (default: true)

**Output:** Optimized SQL query for the specified dialect

**Example:**
```json
{
  "outbound_format": "sql",
  "sql_dialect": "databricks",
  "sql_include_comments": true
}
```

### UC Metrics (`uc_metrics`)

Generate Databricks Unity Catalog Metrics Store definitions.

**Optional Parameters:**
- `uc_catalog` - Unity Catalog catalog name (default: "main")
- `uc_schema` - Unity Catalog schema name (default: "default")
- `uc_process_structures` - Process time intelligence structures (default: true)

**Output:** Unity Catalog Metrics YAML definition

**Example:**
```json
{
  "outbound_format": "uc_metrics",
  "uc_catalog": "production",
  "uc_schema": "metrics"
}
```

### YAML (`yaml`)

Export to portable YAML KPI definition format.

**Output:** Structured YAML definition

**Example:**
```json
{
  "outbound_format": "yaml"
}
```

## Common Use Cases

### 1. Migrate Power BI to Databricks SQL

Convert Power BI measures to Databricks SQL queries.

```json
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "my-dataset",
  "powerbi_group_id": "my-workspace",
  "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
  "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
  "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",

  "outbound_format": "sql",
  "sql_dialect": "databricks",
  "sql_include_comments": true
}
```

### 2. Generate Power BI Measures from YAML

Create DAX measures from YAML business logic definitions.

```json
{
  "inbound_connector": "yaml",
  "yaml_file_path": "/path/to/business-metrics.yaml",

  "outbound_format": "dax",
  "dax_process_structures": true
}
```

### 3. Export to Unity Catalog Metrics Store

Move Power BI measures to Databricks Metrics Store for governance.

```json
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "my-dataset",
  "powerbi_group_id": "my-workspace",
  "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
  "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
  "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",

  "outbound_format": "uc_metrics",
  "uc_catalog": "production",
  "uc_schema": "business_metrics"
}
```

### 4. Document Existing Measures as YAML

Export Power BI measures to portable YAML format for documentation.

```json
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "my-dataset",
  "powerbi_group_id": "my-workspace",
  "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
  "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
  "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",

  "outbound_format": "yaml"
}
```

### 5. Multi-Platform Support

Convert YAML to SQL for multiple database platforms.

```json
{
  "inbound_connector": "yaml",
  "yaml_content": "...",

  "outbound_format": "sql",
  "sql_dialect": "postgresql"
}
```

## Advanced Features

### Time Intelligence Processing

The pipeline can process time intelligence structures (YTD, QTD, MTD, rolling periods):

- **DAX**: `dax_process_structures` (default: true)
- **SQL**: `sql_process_structures` (default: true)
- **UC Metrics**: `uc_process_structures` (default: true)

### Measure Filtering

When extracting from Power BI, you can filter measures:

- **Include Hidden**: `powerbi_include_hidden` (default: false)
- **Regex Pattern**: `powerbi_filter_pattern` (e.g., "^Sales.*" for all measures starting with "Sales")

### Custom Definition Names

Specify a custom name for the generated KPI definition:

```json
{
  "definition_name": "Q1_2024_Metrics"
}
```

## API Reference

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `inbound_connector` | string | Yes | "powerbi" | Source connector type |
| `outbound_format` | string | Yes | "dax" | Target output format |
| `definition_name` | string | No | auto-generated | Name for KPI definition |

### Power BI Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `powerbi_semantic_model_id` | string | Yes* | - | Dataset/semantic model ID |
| `powerbi_group_id` | string | Yes* | - | Workspace ID |
| `powerbi_tenant_id` | string | Yes* | - | Azure AD tenant ID |
| `powerbi_client_id` | string | Yes* | - | Application/Client ID |
| `powerbi_client_secret` | string | Yes* | - | Client secret |
| `powerbi_info_table_name` | string | No | "Info Measures" | Info Measures table name |
| `powerbi_include_hidden` | boolean | No | false | Include hidden measures |
| `powerbi_filter_pattern` | string | No | - | Regex filter for measure names |

*Required only when `inbound_connector="powerbi"`

### YAML Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `yaml_content` | string | Yes* | - | YAML content as string |
| `yaml_file_path` | string | Yes* | - | Path to YAML file |

*One of `yaml_content` or `yaml_file_path` required when `inbound_connector="yaml"`

### SQL Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `sql_dialect` | string | No | "databricks" | SQL dialect for output |
| `sql_include_comments` | boolean | No | true | Include comments in SQL |
| `sql_process_structures` | boolean | No | true | Process time intelligence |

### UC Metrics Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `uc_catalog` | string | No | "main" | Unity Catalog catalog name |
| `uc_schema` | string | No | "default" | Unity Catalog schema name |
| `uc_process_structures` | boolean | No | true | Process time intelligence |

### DAX Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `dax_process_structures` | boolean | No | true | Process time intelligence |

## Troubleshooting

### Authentication Issues

**Problem**: "Error: Missing required parameters"
**Solution**: Ensure you provide all required parameters for your inbound connector:
- Power BI requires: `semantic_model_id`, `group_id`, `access_token`
- YAML requires: `yaml_content` OR `yaml_file_path`

### Invalid Format Errors

**Problem**: "Error: Invalid outbound_format"
**Solution**: Use only supported formats: `dax`, `sql`, `uc_metrics`, `yaml`

**Problem**: "Error: Unsupported inbound_connector"
**Solution**: Use only supported connectors: `powerbi`, `yaml`

### SQL Dialect Issues

**Problem**: Generated SQL doesn't work in my database
**Solution**: Verify you're using the correct `sql_dialect` for your database platform

### Empty Results

**Problem**: No measures extracted from Power BI
**Solution**:
- Check that the Info Measures table exists in your dataset
- Verify your access token has permission to read the dataset
- Check if `powerbi_filter_pattern` is too restrictive

## Architecture

The Measure Conversion Pipeline uses a clean architecture pattern:

```
┌─────────────────┐
│ Inbound         │
│ Connector       │  Extract → KPIDefinition (Standard Format)
│ (Power BI/YAML) │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ KPIDefinition   │  Universal intermediate representation
│ (Standard       │  - KPIs with metadata
│  Format)        │  - Filters & variables
└────────┬────────┘  - Time intelligence structures
         │
         ↓
┌─────────────────┐
│ Outbound        │
│ Converter       │  Generate → Target Format
│ (DAX/SQL/UC)    │
└─────────────────┘
```

## Future Enhancements

- **Tableau Connector**: Extract from Tableau workbooks
- **Excel Connector**: Import from Excel-based KPI definitions
- **Looker Connector**: Extract LookML measures
- **BigQuery ML**: Generate BigQuery ML model definitions
- **dbt Integration**: Export to dbt metrics YAML

## Related Tools

- **YAMLToDAXTool** (ID: 71): Dedicated YAML → DAX converter
- **YAMLToSQLTool** (ID: 72): Dedicated YAML → SQL converter
- **YAMLToUCMetricsTool** (ID: 73): Dedicated YAML → UC Metrics converter
- **PowerBIConnectorTool**: Standalone Power BI extraction tool

The Measure Conversion Pipeline combines all these capabilities into a single, unified interface.
