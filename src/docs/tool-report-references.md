# Power BI Report References Tool

Detailed guide for extracting visual-to-measure/table mappings from Power BI/Microsoft Fabric reports, enabling impact analysis and report documentation with direct page URLs for navigation.

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

### Purpose

The Report References Tool extracts the mapping between:
- **Measures** and the **report pages/visuals** that use them
- **Tables** and the **report pages/visuals** that reference them

This enables powerful impact analysis: before modifying a measure in your semantic model, you can see exactly which reports and pages will be affected.

### Key Features

- **Dataset-based Discovery**: Provide a `dataset_id` to automatically find ALL reports using that semantic model
- **Page URLs**: Every page includes a direct URL for one-click navigation to the affected report page
- **Multiple Output Formats**: JSON, Markdown, or Matrix view
- **Cross-Reference Analysis**: See which measures are used across multiple reports
- **Visual-Level Detail**: Optionally see exactly which visual uses each measure

---

## Architecture

### System Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kasal AI Agent                              │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │         Report References Tool (ID: 78)                   │ │
│  └─────────────────────────┬─────────────────────────────────┘ │
└────────────────────────────┼────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  1. Authenticate with Azure AD (Service Principal or OAuth)    │
│     └─> POST https://login.microsoftonline.com/{tenant}/oauth2 │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  2. [If dataset_id provided] List Workspace Reports            │
│     └─> GET /v1.0/myorg/groups/{ws}/reports                    │
│         Filter by datasetId to find all reports using dataset  │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  3. Fetch Report Definition (PBIR Format)                      │
│     └─> POST /v1/workspaces/{ws}/reports/{rpt}/getDefinition   │
│         (Long-running operation: Poll until complete)          │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  4. Parse PBIR Structure                                       │
│     ├─> definition/pages/{pageId}/page.json    → Page metadata │
│     └─> definition/pages/{pageId}/visuals/...  → Visual refs   │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│  5. Build Cross-Reference Mappings                             │
│     ├─> Measure → Pages/Reports                                │
│     ├─> Table → Pages/Reports                                  │
│     └─> Generate page URLs for navigation                      │
└────────────────────────────────────────────────────────────────┘
```

### PBIR Structure

The Fabric Report Definition API returns the report in **PBIR (Power BI Report)** format:

```
definition/
├── report.json                       # Report-level settings
├── pages/
│   ├── page1/
│   │   ├── page.json                # Page metadata (name, ordinal)
│   │   └── visuals/
│   │       ├── visual1/
│   │       │   └── visual.json      # Visual configuration & data bindings
│   │       └── visual2/
│   │           └── visual.json
│   └── page2/
│       ├── page.json
│       └── visuals/
│           └── ...
```

### Page URL Format

The tool generates direct page URLs in this format:
```
https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}/ReportSection{page_id}
```

---

## Prerequisites

### Azure AD Service Principal

**Required Permissions:**
- `Report.ReadWrite.All` (Application permission) - for reading report definitions
- Admin consent granted

**Workspace Access:**
- Service Principal must be added to the Fabric workspace as **Member** or **Contributor**

### Microsoft Fabric Workspace

- This tool works best with **Microsoft Fabric reports** (PBIR format)
- Traditional .pbix files uploaded to Power BI Service may not have PBIR format
- The report must be published to a Fabric workspace

---

## Configuration

### Recommended: Dataset-Based Discovery

The recommended approach is to provide `dataset_id` to discover ALL reports using that semantic model:

```yaml
mode: "static"

# Power BI / Fabric Configuration
workspace_id: "bcb084ed-f8c9-422c-b148-29839c0f9227"
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"  # Discovers ALL reports using this dataset

# Authentication (Service Principal)
tenant_id: "9f37a392-f0ae-4280-9796-f1864a10effc"
client_id: "7b597aac-de00-44c9-8e2a-3d2c345c36a9"
client_secret: "your-client-secret"

# Output Options
output_format: "json"              # "json", "markdown", or "matrix"
include_visual_details: true       # Include visual-level breakdown
group_by: "page"                   # "page", "measure", or "table"
```

### Alternative: Single Report Mode

If you only want to analyze a specific report:

```yaml
mode: "static"

workspace_id: "bcb084ed-f8c9-422c-b148-29839c0f9227"
report_id: "f8ebce44-c9cb-4041-88d2-4ca33f314e82"  # Single specific report

# Authentication
tenant_id: "9f37a392-f0ae-4280-9796-f1864a10effc"
client_id: "7b597aac-de00-44c9-8e2a-3d2c345c36a9"
client_secret: "your-client-secret"

output_format: "markdown"
```

### User OAuth Mode

Using user's OAuth token instead of Service Principal:

```yaml
mode: "static"
auth_method: "user_oauth"

workspace_id: "bcb084ed-f8c9-422c-b148-29839c0f9227"
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"

# OAuth token (obtained via OAuth flow in UI)
access_token: "eyJ0eXAiOiJKV1QiLCJhbGciOi..."

output_format: "json"
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

### Step 2: List Workspace Reports (if dataset_id provided)

```http
GET https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports
Authorization: Bearer {access_token}
```

**Response:**
```json
{
  "value": [
    {
      "id": "f8ebce44-c9cb-4041-88d2-4ca33f314e82",
      "name": "Sales Dashboard",
      "datasetId": "758560a5-aa3e-4146-b5cc-84966539d169",
      "webUrl": "https://app.powerbi.com/groups/.../reports/..."
    },
    {
      "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "name": "Finance Overview",
      "datasetId": "758560a5-aa3e-4146-b5cc-84966539d169",
      "webUrl": "https://app.powerbi.com/groups/.../reports/..."
    }
  ]
}
```

The tool filters to find reports where `datasetId` matches the provided `dataset_id`.

### Step 3: Fetch Report Definition

```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Response (202 Accepted):**
```http
Location: https://api.fabric.microsoft.com/v1/operations/{operation_id}
```

### Step 4: Poll for Completion

```http
GET https://api.fabric.microsoft.com/v1/operations/{operation_id}
Authorization: Bearer {access_token}
```

### Step 5: Get Result

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
        "path": "definition/pages/page1/page.json",
        "payload": "base64-encoded-content"
      },
      {
        "path": "definition/pages/page1/visuals/visual1/visual.json",
        "payload": "base64-encoded-content"
      }
    ]
  }
}
```

---

## Output Formats

### JSON Output (Default)

Best for programmatic processing and integration with other tools:

```json
{
  "report_references": [
    {
      "report_id": "f8ebce44-c9cb-4041-88d2-4ca33f314e82",
      "report_name": "Sales Dashboard",
      "report_url": "https://app.powerbi.com/groups/bcb084ed-f8c9-422c-b148-29839c0f9227/reports/f8ebce44-c9cb-4041-88d2-4ca33f314e82",
      "pages": [
        {
          "page_name": "Revenue Overview",
          "page_url": "https://app.powerbi.com/groups/bcb084ed-f8c9-422c-b148-29839c0f9227/reports/f8ebce44-c9cb-4041-88d2-4ca33f314e82/ReportSection1",
          "measures": [
            {
              "measure_name": "Total Revenue",
              "table_name": "Sales"
            },
            {
              "measure_name": "YoY Growth",
              "table_name": "Sales"
            }
          ]
        },
        {
          "page_name": "Regional Analysis",
          "page_url": "https://app.powerbi.com/groups/bcb084ed-f8c9-422c-b148-29839c0f9227/reports/f8ebce44-c9cb-4041-88d2-4ca33f314e82/ReportSection2",
          "measures": [
            {
              "measure_name": "Total Revenue",
              "table_name": "Sales"
            },
            {
              "measure_name": "Units Sold",
              "table_name": "Sales"
            }
          ]
        }
      ]
    }
  ],
  "measure_usage": {
    "Total Revenue": [
      {
        "report_id": "f8ebce44-c9cb-4041-88d2-4ca33f314e82",
        "report_name": "Sales Dashboard",
        "page_name": "Revenue Overview",
        "page_url": "https://app.powerbi.com/.../ReportSection1"
      },
      {
        "report_id": "f8ebce44-c9cb-4041-88d2-4ca33f314e82",
        "report_name": "Sales Dashboard",
        "page_name": "Regional Analysis",
        "page_url": "https://app.powerbi.com/.../ReportSection2"
      }
    ],
    "YoY Growth": [
      {
        "report_id": "f8ebce44-c9cb-4041-88d2-4ca33f314e82",
        "report_name": "Sales Dashboard",
        "page_name": "Revenue Overview",
        "page_url": "https://app.powerbi.com/.../ReportSection1"
      }
    ]
  },
  "summary": {
    "reports_analyzed": 1,
    "total_pages": 2,
    "total_visuals": 8,
    "unique_measures": 3,
    "unique_tables": 1
  }
}
```

### Markdown Output

Best for documentation and human review:

```markdown
# Power BI Report References Extraction Results

**Workspace ID**: `bcb084ed-f8c9-422c-b148-29839c0f9227`
**Dataset ID**: `758560a5-aa3e-4146-b5cc-84966539d169`
**Reports Analyzed**: 2

---

## Reports Overview

| Report Name | Pages | Visuals | Measures | Tables | Link |
|-------------|-------|---------|----------|--------|------|
| Sales Dashboard | 3 | 12 | 5 | 2 | [Open](https://app.powerbi.com/...) |
| Finance Overview | 2 | 8 | 4 | 3 | [Open](https://app.powerbi.com/...) |

---

## Report: Sales Dashboard

**Report URL**: [Sales Dashboard](https://app.powerbi.com/groups/.../reports/...)

### Pages

| # | Page Name | Visuals | Link |
|---|-----------|---------|------|
| 1 | Revenue Overview | 4 | [Open](https://app.powerbi.com/.../ReportSection1) |
| 2 | Regional Analysis | 5 | [Open](https://app.powerbi.com/.../ReportSection2) |
| 3 | Trends | 3 | [Open](https://app.powerbi.com/.../ReportSection3) |

### References by Page

#### [Revenue Overview](https://app.powerbi.com/.../ReportSection1)

**Measures**:
- `Total Revenue`
- `YoY Growth`
- `Profit Margin`

**Tables**:
- `Sales`
- `Date`

---

## Global Cross-Reference Summary

### Measures by Report Usage

| Measure | # Reports | Reports |
|---------|-----------|---------|
| `Total Revenue` | 2 | Sales Dashboard, Finance Overview |
| `YoY Growth` | 1 | Sales Dashboard |
| `Units Sold` | 2 | Sales Dashboard, Finance Overview |
```

### Matrix Output

Best for visual overview of measure/table usage across pages:

```markdown
# Power BI Report References Matrix

**Workspace**: `bcb084ed-f8c9-422c-b148-29839c0f9227`
**Dataset**: `758560a5-aa3e-4146-b5cc-84966539d169`
**Reports**: 2

## Measure Usage Matrix (Measures × Reports)

| Measure | Sales Dashboard | Finance Overview |
|---------|-----------------|------------------|
| `Total Revenue` | ✓ | ✓ |
| `YoY Growth` | ✓ |  |
| `Units Sold` | ✓ | ✓ |
| `Profit Margin` | ✓ |  |
| `Avg Order Value` |  | ✓ |

---

## Report: Sales Dashboard

### Measure × Page Matrix

| Measure | Revenue Overview | Regional Analysis | Trends |
|---------|------------------|-------------------|--------|
| `Total Revenue` | ✓ | ✓ | ✓ |
| `YoY Growth` | ✓ |  | ✓ |
| `Units Sold` |  | ✓ |  |
| `Profit Margin` | ✓ |  |  |
```

---

## Use Cases

### 1. Impact Analysis Before Measure Changes

Before modifying a measure in your semantic model, see all affected reports:

```yaml
# Configuration
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"
output_format: "json"
```

**Output**: List of all reports and pages that use measures from this dataset, with direct URLs to review each page.

### 2. Migration Planning

When migrating from Power BI to Databricks, understand report dependencies:

```yaml
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"
output_format: "markdown"
```

**Output**: Comprehensive documentation of which measures are used where, helping prioritize measure conversion.

### 3. Audit and Compliance

Generate documentation showing measure usage across reports:

```yaml
dataset_id: "758560a5-aa3e-4146-b5cc-84966539d169"
output_format: "matrix"
include_visual_details: true
```

**Output**: Matrix view perfect for compliance documentation.

### 4. Combining with Measure Conversion

Use Report References output to trace converted measures back to reports:

1. Run **Measure Conversion Pipeline** to convert measures
2. Run **Report References Tool** to get measure-to-page mappings
3. Cross-reference to identify which report pages are affected by each converted measure

---

## Troubleshooting

### Error: "No reports found using this dataset"

**Causes:**
- No reports have been created from this dataset yet
- Reports exist but use a different dataset ID
- Service Principal lacks access to view reports

**Solutions:**
1. Verify reports exist in the workspace using Power BI Service UI
2. Check the dataset ID is correct (GUID format)
3. Add Service Principal to workspace with at least Viewer role

### Error: "Could not fetch report definition"

**Causes:**
- Report is not in PBIR format (traditional .pbix upload)
- Service Principal lacks Report.ReadWrite.All permission
- Report ID is incorrect

**Solutions:**
1. Verify the report is a Fabric report (not legacy .pbix)
2. Check Service Principal permissions in Azure AD
3. Confirm the report_id is correct

### Error: "No pages found in report"

**Causes:**
- Report definition returned but PBIR structure is different
- Report is empty or has no visuals
- Parsing failed due to unexpected format

**Solutions:**
1. Check the debug_info in JSON output for actual paths returned
2. Verify the report has pages and visuals in Power BI Service
3. Try with a different report to confirm tool functionality

### Error: "Authentication failed"

**Causes:**
- Invalid or expired client secret
- Missing API permissions
- Wrong tenant ID

**Solutions:**
1. Regenerate client secret in Azure AD
2. Verify Report.ReadWrite.All permission is granted
3. Confirm tenant ID matches the Azure AD tenant

---

## Best Practices

### 1. Use Dataset-Based Discovery

Always prefer `dataset_id` over `report_id`:
- Automatically finds ALL reports using the dataset
- No need to track individual report IDs
- Ensures comprehensive coverage

### 2. JSON for Integration, Markdown for Review

- Use **JSON** when integrating with other tools or automation
- Use **Markdown** for human review and documentation
- Use **Matrix** for quick visual overview

### 3. Combine with Other Tools

Use Report References in combination with:
- **Measure Conversion Pipeline**: Convert measures, then trace to reports
- **Field Parameters Tool**: Understand which reports use field parameters

### 4. Regular Audits

Run Report References periodically to:
- Track measure usage trends over time
- Identify orphaned measures (defined but not used)
- Document report dependencies for governance

### 5. Page URLs for Stakeholder Communication

When communicating impact to stakeholders, include the page URLs:
```
"The following report pages use the 'Total Revenue' measure and will be affected:
- Sales Dashboard > Revenue Overview: [Click to view](https://app.powerbi.com/...)
- Finance Overview > KPIs: [Click to view](https://app.powerbi.com/...)"
```

---

## Related Documentation

- [Power BI Migration Tools Guide](./powerbi-tools-guide.md)
- [Measure Conversion Pipeline](./tool-measure-conversion.md)
- [Field Parameters & Calculation Groups](./tool-field-parameters.md)
