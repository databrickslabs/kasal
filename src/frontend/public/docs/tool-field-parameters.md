# Power BI Field Parameters & Calculation Groups Tool

Detailed guide for extracting Field Parameters and Calculation Groups from Power BI/Microsoft Fabric semantic models and converting them to Unity Catalog SQL views.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [API Flow](#api-flow)
- [Output Formats](#output-formats)
- [Use Cases](#use-cases)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

---

## Overview

### What are Field Parameters?

**Field Parameters** in Power BI allow users to dynamically switch between different measures or columns in a visual without creating multiple visuals. They create a "slicer" that lets users choose which metric to display.

**Example**: A sales dashboard where users can switch between "Revenue", "Profit", and "Units Sold" using a single slicer.

### What are Calculation Groups?

**Calculation Groups** provide a way to define reusable calculations that can be applied to multiple measures. They're commonly used for time intelligence patterns (YTD, QTD, YoY, etc.).

**Example**: Instead of creating `Revenue YTD`, `Profit YTD`, `Units YTD` separately, you define a "YTD" calculation once and apply it to any measure.

### Tool Purpose

This tool extracts these constructs from Fabric semantic models and generates SQL UNION views that replicate the behavior in Databricks, enabling:

- Migration of dynamic dashboards to Databricks
- Replication of field parameter switching behavior
- Preservation of calculation group logic in SQL

---

## Architecture

### System Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kasal AI Agent                              │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │     Field Parameters & Calculation Groups Tool (ID: 77)   │ │
│  └─────────────────────────┬─────────────────────────────────┘ │
└────────────────────────────┼────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  1. Authenticate with Azure AD (Service Principal)             │
│     └─> POST https://login.microsoftonline.com/{tenant}/oauth2 │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  2. Fetch Semantic Model Definition (TMDL Format)              │
│     └─> POST /v1/workspaces/{ws}/semanticModels/{sm}/getDefin  │
│         ition                                                  │
│         (Long-running operation: Poll until complete)          │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  3. Parse TMDL Parts                                           │
│     ├─> tables/{name}/table.tmdl      → Find field params      │
│     ├─> tables/{name}/columns.tmdl    → Extract column defs    │
│     └─> calculationGroups/{name}/...  → Extract calc groups    │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  4. Generate SQL Output                                        │
│     ├─> CREATE VIEW vw_field_param_{name} AS ...               │
│     └─> CREATE VIEW vw_calc_group_{name} AS ...                │
└────────────────────────────────────────────────────────────────┘
```

### TMDL Structure

The Fabric API returns the semantic model in **TMDL (Tabular Model Definition Language)** format:

```
definition/
├── model.tmdl                    # Model-level settings
├── tables/
│   ├── Sales/
│   │   ├── table.tmdl           # Table definition
│   │   └── columns.tmdl         # Column definitions
│   └── MeasureSelector/         # Field Parameter table
│       ├── table.tmdl           # Contains isFieldParameter: true
│       └── columns.tmdl         # Parameter options
├── calculationGroups/
│   └── TimeIntelligence/
│       ├── calculationGroup.tmdl
│       └── calculationItems/
│           ├── Current.tmdl
│           ├── YTD.tmdl
│           └── YoY.tmdl
```

---

## Prerequisites

### Azure AD Service Principal

**Required Permissions:**
- `SemanticModel.ReadWrite.All` (Application permission)
- Admin consent granted

**Workspace Access:**
- Service Principal must be added to the Fabric workspace as **Member** or **Contributor**

### Microsoft Fabric Workspace

- This tool **only works with Microsoft Fabric workspaces**
- Legacy Power BI Service workspaces (non-Fabric) are not supported
- The semantic model must be in Fabric format

---

## Configuration

### Static Mode Configuration

Configure values directly in the UI:

```yaml
# Mode
mode: "static"

# Power BI / Fabric Configuration
workspace_id: "bcb084ed-f8c9-422c-b148-29839c0f9227"
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"

# Authentication (Service Principal)
tenant_id: "9f37a392-f0ae-4280-9796-f1864a10effc"
client_id: "7b597aac-de00-44c9-8e2a-3d2c345c36a9"
client_secret: "your-client-secret"

# Output Options
target_catalog: "main"
target_schema: "default"
output_format: "sql"              # "sql", "json", or "markdown"
include_metadata_table: true
skip_system_tables: true
include_hidden: false
```

### Dynamic Mode Configuration

Use placeholders that resolve at runtime:

```yaml
# Mode
mode: "dynamic"

# Placeholders resolved from execution_inputs
workspace_id: "{workspace_id}"
dataset_id: "{dataset_id}"
tenant_id: "{tenant_id}"
client_id: "{client_id}"
client_secret: "{client_secret}"

# Fixed output options
target_catalog: "main"
target_schema: "default"
output_format: "sql"
```

### User OAuth Mode

Alternative to Service Principal - uses user's OAuth token:

```yaml
mode: "static"
auth_method: "user_oauth"

workspace_id: "bcb084ed-f8c9-422c-b148-29839c0f9227"
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"

# OAuth token (obtained via OAuth flow in UI)
access_token: "eyJ0eXAiOiJKV1QiLCJhbGciOi..."

# Output Options
output_format: "sql"
```

---

## API Flow

### Step 1: Authentication

```http
POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials
&client_id={client_id}
&client_secret={client_secret}
&scope=https://api.fabric.microsoft.com/.default
```

### Step 2: Initiate Definition Fetch

```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Response (202 Accepted):**
```http
Location: https://api.fabric.microsoft.com/v1/operations/{operation_id}
```

### Step 3: Poll for Completion

```http
GET https://api.fabric.microsoft.com/v1/operations/{operation_id}
Authorization: Bearer {access_token}
```

**Response (when complete):**
```json
{
  "status": "Succeeded",
  "createdTimeUtc": "2025-01-15T10:00:00Z",
  "lastUpdatedTimeUtc": "2025-01-15T10:00:05Z"
}
```

### Step 4: Get Result

```http
GET https://api.fabric.microsoft.com/v1/operations/{operation_id}/result
Authorization: Bearer {access_token}
```

**Response:**
```json
{
  "definition": {
    "parts": [
      {
        "path": "definition/tables/MeasureSelector/table.tmdl",
        "payload": "base64-encoded-content"
      }
    ]
  }
}
```

---

## Output Formats

### SQL Output (Default)

```sql
-- ============================================================
-- Field Parameters & Calculation Groups - SQL Conversion
-- Generated from Power BI Semantic Model
-- ============================================================

-- ============================================================
-- FIELD PARAMETER: MeasureSelector
-- ============================================================
-- Description: Allows dynamic switching between measures
-- Source Table: MeasureSelector

CREATE OR REPLACE VIEW main.default.vw_field_param_measure_selector AS
SELECT 'Revenue' AS field_name, SUM(amount) AS value, 1 AS ordinal
FROM main.default.sales
UNION ALL
SELECT 'Profit' AS field_name, SUM(profit) AS value, 2 AS ordinal
FROM main.default.sales
UNION ALL
SELECT 'Units Sold' AS field_name, SUM(quantity) AS value, 3 AS ordinal
FROM main.default.sales;

-- ============================================================
-- CALCULATION GROUP: TimeIntelligence
-- ============================================================
-- Description: Time-based calculations
-- Precedence: 1

CREATE OR REPLACE VIEW main.default.vw_calc_group_time_intelligence AS
SELECT 'Current' AS calculation_name, SUM(amount) AS value, 1 AS ordinal
FROM main.default.sales
WHERE date_column >= DATE_TRUNC('YEAR', CURRENT_DATE())
UNION ALL
SELECT 'YTD' AS calculation_name,
       SUM(SUM(amount)) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS value,
       2 AS ordinal
FROM main.default.sales
UNION ALL
SELECT 'YoY' AS calculation_name,
       (SUM(amount) - LAG_VALUE) / LAG_VALUE * 100 AS value,
       3 AS ordinal
FROM (
  SELECT SUM(amount) AS amount,
         LAG(SUM(amount)) OVER (ORDER BY year) AS LAG_VALUE
  FROM main.default.sales
);

-- ============================================================
-- METADATA TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS main.default._metadata_field_params (
  param_name STRING,
  source_table STRING,
  option_name STRING,
  option_ordinal INT,
  original_expression STRING,
  is_hidden BOOLEAN
);

INSERT INTO main.default._metadata_field_params VALUES
  ('MeasureSelector', 'MeasureSelector', 'Revenue', 1, '[Sales].[Total Revenue]', false),
  ('MeasureSelector', 'MeasureSelector', 'Profit', 2, '[Sales].[Total Profit]', false),
  ('MeasureSelector', 'MeasureSelector', 'Units Sold', 3, '[Sales].[Total Units]', false);
```

### JSON Output

```json
{
  "workspace_id": "bcb084ed-f8c9-422c-b148-29839c0f9227",
  "dataset_id": "758560a5-aa3e-4146-b5cc-84966539d169",
  "field_parameters": [
    {
      "name": "MeasureSelector",
      "source_table": "MeasureSelector",
      "is_hidden": false,
      "options": [
        {
          "name": "Revenue",
          "ordinal": 1,
          "expression": "[Sales].[Total Revenue]",
          "data_type": "Decimal"
        },
        {
          "name": "Profit",
          "ordinal": 2,
          "expression": "[Sales].[Total Profit]",
          "data_type": "Decimal"
        }
      ]
    }
  ],
  "calculation_groups": [
    {
      "name": "TimeIntelligence",
      "precedence": 1,
      "calculation_items": [
        {
          "name": "Current",
          "ordinal": 1,
          "expression": "SELECTEDMEASURE()"
        },
        {
          "name": "YTD",
          "ordinal": 2,
          "expression": "CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))"
        }
      ]
    }
  ],
  "summary": {
    "field_parameter_count": 1,
    "calculation_group_count": 1,
    "total_options": 3,
    "total_calculation_items": 2
  }
}
```

### Markdown Output

```markdown
# Field Parameters & Calculation Groups

**Workspace**: `bcb084ed-f8c9-422c-b148-29839c0f9227`
**Dataset**: `758560a5-aa3e-4146-b5cc-84966539d169`

---

## Field Parameters

### MeasureSelector

| # | Option | Expression | Data Type |
|---|--------|------------|-----------|
| 1 | Revenue | [Sales].[Total Revenue] | Decimal |
| 2 | Profit | [Sales].[Total Profit] | Decimal |
| 3 | Units Sold | [Sales].[Total Units] | Integer |

---

## Calculation Groups

### TimeIntelligence

**Precedence**: 1

| # | Calculation | Expression |
|---|-------------|------------|
| 1 | Current | SELECTEDMEASURE() |
| 2 | YTD | CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date])) |
| 3 | YoY | DIVIDE(SELECTEDMEASURE() - [PY Value], [PY Value]) |

---

## Summary

- **Field Parameters**: 1
- **Calculation Groups**: 1
- **Total Options**: 3
- **Total Calculation Items**: 3
```

---

## Use Cases

### 1. Dashboard Migration

Migrate Power BI dashboards with dynamic field selectors to Databricks:

```sql
-- Original Power BI: User selects "Revenue" or "Profit" from slicer
-- Databricks equivalent: Filter the UNION view

SELECT * FROM main.default.vw_field_param_measure_selector
WHERE field_name = 'Revenue';
```

### 2. Time Intelligence Replication

Replicate Power BI's time intelligence calculations:

```sql
-- Apply time intelligence to any measure
SELECT
  m.measure_name,
  t.calculation_name,
  m.value * t.multiplier AS calculated_value
FROM main.default.measures m
CROSS JOIN main.default.vw_calc_group_time_intelligence t;
```

### 3. Documentation Generation

Generate markdown documentation for existing semantic models:

```yaml
output_format: "markdown"
```

---

## Troubleshooting

### Error: "Could not fetch semantic model definition"

**Causes:**
- Workspace is not a Fabric workspace
- Service Principal lacks SemanticModel.ReadWrite.All permission
- Dataset ID is incorrect

**Solutions:**
1. Verify the workspace is Fabric-enabled (not legacy Power BI Service)
2. Check Service Principal permissions in Azure AD
3. Confirm the dataset_id is correct (GUID format)

### Error: "No field parameters or calculation groups found"

**Causes:**
- Semantic model doesn't have these constructs
- Parsing failed to detect the structures

**Solutions:**
1. Verify the semantic model has field parameters or calculation groups in Power BI Desktop
2. Check the output logs for parsing details
3. Try JSON output format to see raw extracted data

### Error: "Authentication failed"

**Causes:**
- Invalid or expired client secret
- Missing API permissions
- Wrong tenant ID

**Solutions:**
1. Regenerate client secret in Azure AD
2. Verify SemanticModel.ReadWrite.All permission is granted
3. Confirm tenant ID matches the Azure AD tenant

---

## Best Practices

1. **Test with JSON first**: Use JSON output to verify extraction before generating SQL
2. **Review generated SQL**: Always review and test generated SQL before deployment
3. **Use dynamic mode for production**: Avoid hardcoding credentials
4. **Monitor for schema changes**: Re-run extraction when semantic model changes
5. **Include metadata tables**: Enable `include_metadata_table: true` for lineage tracking

---

## Related Documentation

- [Power BI Migration Tools Guide](./powerbi-tools-guide.md)
- [Measure Conversion Pipeline](./tool-measure-conversion.md)
- [Report References Tool](./tool-report-references.md)
