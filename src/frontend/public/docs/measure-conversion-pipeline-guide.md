# Measure Conversion Pipeline - User Guide

## Overview

The **Measure Conversion Pipeline** is a universal converter that transforms business metrics and measures between different BI platforms and formats. It provides a simple dropdown-based UX where you select:

- **FROM** (Inbound Connector): Source system or format
- **TO** (Outbound Format): Target format or platform

## Prerequisites - Service Principal Setup

> **Important**: This setup is required before using the Power BI connector. Without proper Service Principal configuration, the application cannot access Power BI workspaces and metadata.

### Why Service Principal Authentication is Required

The Measure Conversion Pipeline uses Microsoft's Power BI REST APIs to programmatically access dataset metadata, measures, and model information. Service Principal authentication provides:

- **Secure API Access**: Application-level credentials without user interaction
- **Tenant-Wide Access**: Ability to access all workspaces (with proper permissions)
- **Automation Support**: No manual login required for scheduled conversions

### Step-by-Step: Service Principal Configuration

#### Step 1: Create Azure AD App Registration

In the **Azure Portal**:

1. Navigate to **Azure Active Directory** → **App registrations**
2. Click **New registration**
3. Enter an application name (e.g., "Measure-Converter-ServicePrincipal")
4. Select **Accounts in this organizational directory only**
5. Click **Register**
6. **Save the following values** - you'll need them later:
   - **Application (client) ID** - This is your `client_id`
   - **Directory (tenant) ID** - This is your `tenant_id`

#### Step 2: Configure API Permissions

In your App Registration:

1. Go to **API permissions**
2. Click **Add a permission**
3. Select **Power BI Service**
4. Choose **Delegated permissions**
5. Add the following permissions:

| Permission | Type | Admin Consent | Purpose |
|------------|------|---------------|---------|
| `Dataset.Read.All` | Delegated | Required | Read dataset metadata and measures |
| `Workspace.Read.All` | Delegated | Optional | List accessible workspaces |

6. Click **Grant admin consent for [Your Organization]**

> **Note**: Admin consent is required for these permissions. You may need to request this from your Azure AD administrator if you don't have admin rights.

#### Step 3: Create Client Secret

Generate authentication credentials:

1. In your App Registration, go to **Certificates & secrets**
2. Click **New client secret**
3. Enter a description (e.g., "Measure Converter Production")
4. Select expiration period (recommended: 24 months)
5. Click **Add**
6. **IMMEDIATELY COPY THE SECRET VALUE** - you cannot retrieve it later!

> **Security Warning**: Store the client secret securely. Never commit it to source control or share it publicly. Treat it like a password.

#### Step 4: Enable Service Principal in Power BI

Power BI Admin Portal configuration:

1. Go to **Power BI Admin Portal** (app.powerbi.com → Settings → Admin portal)
2. Navigate to **Tenant settings**
3. Scroll to **Developer settings**
4. Enable **"Allow service principals to use Power BI APIs"**
5. Add your service principal to the allowed list:
   - Either by **Application ID**, or
   - By adding it to a **Security Group** that's allowed
6. Click **Apply**

> **Note**: Changes to Power BI tenant settings may take up to 15 minutes to propagate.

#### Step 5: Grant Workspace Access

For each workspace you want to access:

1. Open the workspace in Power BI
2. Click **Manage access** (or Access in older versions)
3. Add your Service Principal (search by Application ID or name)
4. Assign at least **Viewer** role (or higher for write operations)

### Required Credentials Summary

| Credential | Where to Find | Example Format |
|------------|---------------|----------------|
| **Tenant ID** | Azure Portal → Azure AD → Overview | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| **Client ID** | Azure Portal → App Registration → Overview | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| **Client Secret** | Azure Portal → App Registration → Certificates & secrets | `abc123...` (long string) |
| **Workspace ID** | Power BI URL: `app.powerbi.com/groups/{workspace_id}/...` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| **Dataset ID** | Power BI URL: `app.powerbi.com/.../datasets/{dataset_id}/...` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |

---

## Quick Start

### Basic Workflow

1. **Select Inbound Connector** (`inbound_connector`): Choose your source
   - `powerbi` - Extract from Power BI datasets via REST API
   - `yaml` - Load from YAML definition files
   - *Coming Soon*: `tableau`, `excel`, `looker`

2. **Select Outbound Format** (`outbound_format`): Choose your target
   - `dax` - Power BI / Analysis Services measures
   - `sql` - SQL queries (multiple dialects supported)
   - `uc_metrics` - Databricks Unity Catalog Metrics Store (downloadable YAML)

3. **Configure Source-Specific Parameters**: Provide authentication and connection details

4. **Configure Target-Specific Parameters**: Set output preferences (dialect, etc.)

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

**Output:** Downloadable Unity Catalog Metrics YAML definition file

**Example:**
```json
{
  "outbound_format": "uc_metrics"
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

  "outbound_format": "uc_metrics"
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

### DAX Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `dax_process_structures` | boolean | No | true | Process time intelligence |

## Troubleshooting

### Service Principal Authentication Issues

**Problem**: `AADSTS700016: Application with identifier 'xxx' was not found in the directory`

**Solutions**:
- Verify the `tenant_id` matches the Azure AD tenant where the app is registered
- Confirm the `client_id` is correct (Application ID from Azure Portal)
- Ensure the app registration exists and hasn't been deleted

**Problem**: `Insufficient privileges to complete the operation`

**Solutions**:
- Verify admin consent was granted for API permissions
- Ensure the Service Principal is enabled in Power BI Admin Portal
- Confirm the Service Principal has been granted access to the workspace
- Wait 15 minutes after making changes for them to propagate

**Problem**: `Invalid client secret`

**Solutions**:
- Regenerate the client secret in Azure Portal
- Ensure there are no leading/trailing spaces when copying the secret
- Check that the secret hasn't expired

### Connection Test Fails

**Problem**: Cannot connect to Power BI

**Solutions**:
- Verify all credentials are correct (no extra spaces)
- Check if firewall or network policies are blocking Power BI API access
- Ensure the Power BI service is accessible from your network

### Empty Results

**Problem**: No measures extracted from Power BI

**Solutions**:
- Verify the dataset contains measures (not just columns)
- Check if `powerbi_filter_pattern` is too restrictive
- Ensure the Service Principal has at least Viewer access to the workspace
- Try with `powerbi_include_hidden: true` to include hidden measures

### Invalid Format Errors

**Problem**: "Error: Invalid outbound_format"
**Solution**: Use only supported formats: `dax`, `sql`, `uc_metrics`

**Problem**: "Error: Unsupported inbound_connector"
**Solution**: Use only supported connectors: `powerbi`, `yaml`

### SQL Dialect Issues

**Problem**: Generated SQL doesn't work in my database
**Solution**: Verify you're using the correct `sql_dialect` for your database platform

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

## Additional Resources

### Microsoft Documentation

- [Register an application with Microsoft identity platform](https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
- [Power BI REST API - Datasets](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets)
- [Automate workspace and dataset tasks with service principals](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-service-principal)

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
