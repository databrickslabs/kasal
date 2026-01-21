# Measure Conversion Pipeline Integration Guide

Complete guide for integrating the Measure Conversion Pipeline with Kasal AI agents for automated business metrics transformation.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
  - [Kasal Configuration](#1-kasal-configuration)
  - [Power BI Authentication Setup](#2-power-bi-authentication-setup)
  - [Unity Catalog Setup](#3-unity-catalog-setup-optional)
- [Tool Configuration](#tool-configuration)
- [Use Cases](#use-cases)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

---

## Overview

The Measure Conversion Pipeline integration enables Kasal AI agents to automatically convert business metrics and measures between different BI platforms and formats. This provides a production-ready, API-driven converter for metrics transformation within AI workflows.

**Key Features:**
- Universal metrics conversion (Power BI ↔ Databricks ↔ SQL)
- FROM/TO dropdown-based configuration
- Support for DAX, SQL (multiple dialects), Unity Catalog Metrics, YAML
- Task-level configuration for source and target systems
- Multi-tenant isolation with encrypted credential storage
- Time intelligence structure processing

**Use Cases:**
- Migrating Power BI measures to Databricks Unity Catalog
- Converting DAX measures to SQL for data warehouse integration
- Extracting Power BI semantic model metadata around measures
- Generating portable YAML definitions from existing BI assets
- Automating measure standardization across platforms

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────┐
│         Kasal AI Agent                      │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │  Measure Conversion Pipeline Tool   │   │
│  └───────────────┬─────────────────────┘   │
└──────────────────┼─────────────────────────┘
                   │
                   ├─ Inbound Connectors (FROM)
                   │  ├─> Power BI REST API
                   │  │   └─ Extract measures & metadata
                   │  └─> YAML File Parser
                   │      └─ Load KPI definitions
                   │
                   └─ Outbound Formats (TO)
                      ├─> DAX Generator
                      │   └─ Power BI measures
                      ├─> SQL Generator
                      │   └─ Multiple dialects
                      └─> UC Metrics Creator
                          └─ Databricks Metrics Store
```

### Backend Components

1. **Measure Converter Tool** (`engines/crewai/tools/custom/measure_converter_tool.py`)
   - CrewAI tool for metrics conversion
   - Handles FROM/TO connector orchestration
   - Validates configuration and credentials

2. **Power BI Inbound Connector** (`engines/crewai/tools/custom/converters/inbound/powerbi_inbound_connector.py`)
   - Connects to Power BI REST API
   - Extracts semantic model metadata
   - Retrieves measure definitions and DAX expressions

3. **Outbound Format Generators**
   - **DAX Generator**: Creates Power BI measure definitions
   - **SQL Generator**: Produces SQL queries for various dialects
   - **UC Metrics Generator**: Creates Unity Catalog Metric definitions
   - **YAML Exporter**: Generates portable KPI definitions

4. **API Keys Service** (`services/api_keys_service.py`)
   - Stores encrypted Power BI credentials
   - Multi-tenant isolation via `group_id`
   - Handles: `POWERBI_CLIENT_SECRET`, `POWERBI_USERNAME`, `POWERBI_PASSWORD`, `DATABRICKS_API_KEY`

### Frontend Components

1. **MeasureConverterConfigSelector** (`components/Common/MeasureConverterConfigSelector.tsx`)
   - Task-level converter configuration UI
   - FROM/TO dropdown selection
   - Dynamic configuration fields based on selection
   - Appears when "Measure Conversion Pipeline" tool is selected

2. **TaskForm** (`components/Tasks/TaskForm.tsx`)
   - Integrates MeasureConverterConfigSelector
   - Stores configuration in `tool_configs['Measure Conversion Pipeline']`

---

## Prerequisites

### Required Components

1. **Kasal Installation**
   - Backend API running
   - Frontend UI accessible
   - Database configured (SQLite or PostgreSQL)

2. **API Keys** (depends on source/target)
   - Power BI: Service Principal credentials or Device Code Flow
   - Databricks: API token or OAuth credentials
   - Stored in Kasal Settings → API Keys

### Optional Components

- **Power BI Workspace Access** (for Power BI inbound connector)
- **Databricks Workspace** (for Unity Catalog Metrics target)
- **Unity Catalog Metrics Store** (for UC Metrics output)

---

## Setup Guide

### 1. Kasal Configuration

#### Configure API Keys

Navigate to **Settings → API Keys** and add the following keys based on your use case:

**For Power BI Source:**
```
Key Name: POWERBI_CLIENT_SECRET
Value: <your-azure-ad-client-secret>
Description: Azure AD Service Principal Secret

Key Name: POWERBI_USERNAME
Value: <your-powerbi-username>
Description: Power BI user account (for device code flow)

Key Name: POWERBI_PASSWORD
Value: <your-powerbi-password>
Description: Power BI password (for device code flow)
```

**For Databricks / Unity Catalog:**
```
Key Name: DATABRICKS_API_KEY
Value: <your-databricks-pat>
Description: Databricks Personal Access Token
```

### 2. Power BI Authentication Setup

#### Option A: Service Principal (Recommended for Production)

1. **Register Azure AD Application**
   - Go to Azure Portal → Azure Active Directory → App Registrations
   - Create new registration: "Kasal-Measure-Converter"
   - Note the **Application (client) ID** and **Directory (tenant) ID**

2. **Create Client Secret**
   - Go to Certificates & secrets → New client secret
   - Copy the secret value (store as `POWERBI_CLIENT_SECRET`)

3. **Grant Power BI Permissions**
   - Go to API permissions → Add permission → Power BI Service
   - Add: `Dataset.Read.All`, `Workspace.Read.All`
   - Grant admin consent

4. **Enable Power BI Service Principal**
   - Go to Power BI Admin Portal → Tenant Settings
   - Enable "Service principals can access Power BI APIs"
   - Add your app to the security group

5. **Grant Workspace Access**
   - In Power BI workspace settings, add the service principal with "Viewer" role

#### Option B: Device Code Flow (Development/Testing)

1. Store your Power BI credentials in API Keys:
   - `POWERBI_USERNAME`: Your Power BI account email
   - `POWERBI_PASSWORD`: Your Power BI password

2. This method will prompt for interactive authentication during first use

### 3. Unity Catalog Setup (Optional)

If using Unity Catalog Metrics as target format:

1. **Enable UC Metrics Store**
   ```sql
   CREATE CATALOG IF NOT EXISTS <catalog_name>;
   CREATE SCHEMA IF NOT EXISTS <catalog_name>.<schema_name>;
   ```

2. **Grant Permissions**
   ```sql
   GRANT CREATE ON SCHEMA <catalog_name>.<schema_name> TO `<principal>`;
   GRANT USAGE ON CATALOG <catalog_name> TO `<principal>`;
   ```

3. **Configure Databricks Token**
   - Store `DATABRICKS_API_KEY` in Kasal API Keys

---

## Tool Configuration

### Adding the Tool to a Task

1. **Create or Edit a Task**
   - Navigate to Workflow Designer
   - Create a new task or edit an existing one

2. **Select the Tool**
   - In the "Tools" dropdown, select **"Measure Conversion Pipeline"**

3. **Configure Source (FROM)**
   - Select inbound connector (e.g., "Power BI")
   - Provide required authentication fields:
     - **Tenant ID**: Azure AD tenant ID
     - **Client ID**: Application (client) ID
     - **Semantic Model ID**: Power BI dataset ID
     - **Group ID**: Power BI workspace ID
   - Set optional parameters (filter patterns, include hidden measures, etc.)

4. **Configure Target (TO)**
   - Select outbound format (e.g., "Unity Catalog Metrics")
   - Provide target-specific configuration:
     - **Catalog**: Unity Catalog name
     - **Schema**: Schema name for metrics
     - **SQL Dialect**: If using SQL output
   - Enable/disable structure processing

### Configuration Schema

```json
{
  "inbound_connector": "powerbi",
  "outbound_format": "uc_metrics",

  // Power BI inbound config
  "powerbi_semantic_model_id": "abc-123-def",
  "powerbi_group_id": "workspace-456",
  "powerbi_tenant_id": "tenant-789",
  "powerbi_client_id": "client-012",
  "powerbi_use_device_code": false,
  "powerbi_include_hidden": false,
  "powerbi_filter_pattern": "Sales.*",

  // Unity Catalog outbound config
  "uc_catalog": "analytics",
  "uc_schema": "metrics",
  "uc_process_structures": true,

  // General config
  "definition_name": "Sales Metrics Migration"
}
```

### Task Description Example

```
Extract all sales-related measures from the Power BI semantic model
and convert them to Unity Catalog Metrics format. Filter to only
include measures that start with "Sales" and process all time
intelligence structures.
```

---

## Use Cases

### Use Case 1: Power BI to Unity Catalog Migration

**Scenario:** Migrate all measures from a Power BI dataset to Databricks Unity Catalog Metrics Store.

**Configuration:**
- **FROM**: Power BI
- **TO**: Unity Catalog Metrics
- **Task Description**: "Extract all measures from the Financial Reporting dataset and create corresponding Unity Catalog metrics in the analytics.financial_metrics schema."

**Expected Output:**
- Unity Catalog metrics created
- Measure definitions with metadata
- DAX expressions preserved in descriptions

### Use Case 2: Generate SQL from Power BI

**Scenario:** Convert Power BI DAX measures to SQL queries for a data warehouse.

**Configuration:**
- **FROM**: Power BI
- **TO**: SQL
- **SQL Dialect**: Snowflake
- **Task Description**: "Convert all revenue-related measures from Power BI to Snowflake SQL queries."

**Expected Output:**
- SQL query definitions
- Comments with original measure descriptions
- Compatible with Snowflake syntax

### Use Case 3: Extract Portable YAML Definitions

**Scenario:** Create a portable, version-controlled definition of all business metrics.

**Configuration:**
- **FROM**: Power BI
- **TO**: YAML
- **Task Description**: "Extract all KPIs from the Executive Dashboard and generate a YAML definition file."

**Expected Output:**
- YAML file with all measure definitions
- Includes DAX expressions, descriptions, and metadata
- Can be versioned in Git

### Use Case 4: Standardize Measures Across Teams

**Scenario:** Extract measures from multiple Power BI reports and standardize naming conventions.

**Configuration:**
- **FROM**: Power BI
- **TO**: YAML
- **Filter Pattern**: "(Revenue|Sales|Profit).*"
- **Agent Task**: "Extract measures, standardize naming conventions, then convert to Unity Catalog format."

**Expected Output:**
- Standardized measure definitions
- Consistent naming across datasets
- Ready for deployment to UC

---

## Testing

### Manual Testing

1. **Create a Test Task**
   - Create a simple task with Measure Conversion Pipeline tool
   - Use a small Power BI dataset for testing

2. **Test FROM Connector**
   ```
   Task: "List all measures from the Power BI semantic model"
   FROM: Power BI
   TO: YAML
   ```

3. **Test TO Generator**
   ```
   Task: "Convert the extracted measures to SQL format"
   FROM: YAML (using output from previous task)
   TO: SQL (Databricks dialect)
   ```

4. **Verify Output**
   - Check task execution results
   - Validate generated code/definitions
   - Test generated SQL/DAX in target system

### Integration Testing

Run the measure converter in a complete workflow:

```
Agent: "BI Migration Specialist"
Task 1: "Extract measures from Power BI dataset X"
  - Tool: Measure Conversion Pipeline
  - FROM: Power BI
  - TO: YAML

Task 2: "Review and validate measure definitions"
  - Tool: None (LLM analysis)
  - Context: Output from Task 1

Task 3: "Create Unity Catalog metrics"
  - Tool: Measure Conversion Pipeline
  - FROM: YAML (from Task 1)
  - TO: Unity Catalog Metrics
```

---

## Troubleshooting

### Common Issues

#### 1. Authentication Errors

**Error:** `"Power BI authentication failed"`

**Solutions:**
- Verify API keys are correctly stored in Settings → API Keys
- Check Service Principal has correct permissions
- Ensure workspace access is granted
- Verify tenant ID and client ID are correct

#### 2. Semantic Model Not Found

**Error:** `"Semantic model ID not found"`

**Solutions:**
- Verify the semantic model ID is correct (copy from Power BI workspace URL)
- Check the service principal has access to the workspace
- Ensure the dataset is published and not in development mode

#### 3. Empty Results

**Error:** `"No measures found in semantic model"`

**Solutions:**
- Check if filter pattern is too restrictive
- Verify `include_hidden` setting
- Ensure the dataset actually contains measures (not just columns)

#### 4. Unity Catalog Permission Errors

**Error:** `"Permission denied creating metrics in UC"`

**Solutions:**
- Verify Databricks token has correct permissions
- Check catalog and schema exist
- Ensure principal has CREATE privileges on schema

#### 5. Invalid Configuration

**Error:** `"Required configuration parameter missing"`

**Solutions:**
- Review configuration requirements for selected FROM/TO combination
- Ensure all required fields are filled in task configuration
- Check API keys are configured correctly

### Debug Mode

Enable detailed logging by setting environment variable:
```bash
export LOG_LEVEL=DEBUG
```

---

## Best Practices

### Security

1. **Use Service Principals** for production deployments instead of user credentials
2. **Store credentials in API Keys** - never hardcode in task descriptions
3. **Apply least privilege** - grant minimal permissions required
4. **Rotate secrets regularly** - update client secrets and tokens periodically

### Performance

1. **Use filter patterns** to limit the number of measures extracted
2. **Process in batches** for large semantic models
3. **Cache YAML outputs** to avoid repeated Power BI API calls
4. **Set appropriate timeouts** for large conversion operations

### Workflow Design

1. **Separate extraction and transformation** into different tasks for better debugging
2. **Use YAML as intermediate format** for complex multi-stage conversions
3. **Validate outputs** with a review task before deploying to production
4. **Version control YAML definitions** in Git for audit trail

### Configuration Management

1. **Use consistent naming conventions** for definition names
2. **Document filter patterns** in task descriptions
3. **Test configurations** in development workspace before production
4. **Store workspace/model IDs** as crew-level variables for reusability

### Error Handling

1. **Enable retry on failure** in advanced task configuration
2. **Set appropriate max retries** (3-5 recommended)
3. **Use guardrails** to validate output structure
4. **Add validation tasks** after conversion to check output quality

---

## API Reference

### Tool Configuration Parameters

#### Inbound Connectors

**Power BI:**
```typescript
{
  inbound_connector: "powerbi",
  powerbi_semantic_model_id: string,  // Required
  powerbi_group_id: string,           // Required
  powerbi_tenant_id?: string,         // Optional
  powerbi_client_id?: string,         // Optional
  powerbi_access_token?: string,      // Optional (alternative to client credentials)
  powerbi_use_device_code?: boolean,  // Optional
  powerbi_info_table_name?: string,   // Optional (default: "Info Measures")
  powerbi_include_hidden?: boolean,   // Optional (default: false)
  powerbi_filter_pattern?: string     // Optional (regex)
}
```

**YAML:**
```typescript
{
  inbound_connector: "yaml",
  yaml_content?: string,    // Provide content directly
  yaml_file_path?: string   // Or file path
}
```

#### Outbound Formats

**DAX:**
```typescript
{
  outbound_format: "dax",
  dax_process_structures?: boolean  // Optional (default: true)
}
```

**SQL:**
```typescript
{
  outbound_format: "sql",
  sql_dialect?: string,           // Optional (default: "databricks")
  sql_include_comments?: boolean, // Optional (default: true)
  sql_process_structures?: boolean // Optional (default: true)
}
```

**Unity Catalog Metrics:**
```typescript
{
  outbound_format: "uc_metrics",
  uc_catalog: string,              // Required
  uc_schema: string,               // Required
  uc_process_structures?: boolean  // Optional (default: true)
}
```

**YAML:**
```typescript
{
  outbound_format: "yaml",
  definition_name?: string  // Optional
}
```

---

## Related Documentation

- [Measure Conversion Pipeline User Guide](measure-conversion-pipeline-guide.md) - Detailed parameter reference
- [Measure Converters Overview](measure-converters-overview.md) - Architecture and design
- [Power BI Integration](powerbi_integration.md) - Power BI authentication setup
- [API Endpoints](api_endpoints.md) - REST API documentation

---

## Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review logs with DEBUG level enabled
3. Verify API key configuration in Settings
4. Test with minimal configuration first

---

**Last Updated:** December 2024
**Version:** 1.0
