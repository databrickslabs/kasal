# Power BI Migration Tools Guide

This guide covers the Power BI migration tools available in Kasal for converting Power BI assets to Databricks Unity Catalog.

## Overview

Kasal provides seven specialized tools for migrating different aspects of Power BI semantic models to Databricks and for analyzing Power BI data:

| Tool | ID | Purpose | SVP Type | Details |
|------|----|---------|----------|---------|
| **Comprehensive Analysis Tool** | 72 | Answer business questions via intelligent DAX generation with self-correction | **Admin API**  | [Full Guide](./tool-comprehensive-analysis.md) |
| **Measure Conversion Pipeline** | 73 | Convert DAX measures to UC Metrics/SQL | Non-Admin API | [Full Guide](./tool-measure-conversion.md) |
| **M-Query Conversion Pipeline** | 74 | Convert M-Query/Power Query to SQL views | **Admin API** | [Full Guide](./tool-mquery-conversion.md) |
| **Power BI Relationships Tool** | 75 | Extract relationships as FK constraints | Non-Admin API | [Full Guide](./tool-relationships-conversion.md) |
| **Power BI Hierarchies Tool** | 76 | Extract hierarchies as dimension views | Non-Admin API | [Full Guide](./tool-hierarchies-conversion.md) |
| **Field Parameters & Calculation Groups** | 77 | Extract field parameters/calc groups to SQL UNION views | Non-Admin API | [Full Guide](./tool-field-parameters.md) |
| **Report References Tool** | 78 | Map measures/tables to report pages with URLs | Non-Admin API | [Full Guide](./tool-report-references.md) |

> **Tip**: Click the "Full Guide" links above for detailed architecture, API examples, and troubleshooting for each tool.

---

## Service Principal Requirements

Power BI has two distinct API access patterns, each requiring different permissions and setup procedures. You will need **two separate Service Principals** depending on which tools you plan to use.

---

## Guide 1: Non-Admin API Service Principal (Execute Queries API)

### Overview

**Used by**: Measure Conversion Pipeline, Relationships Tool, Hierarchies Tool

This Service Principal needs to be a **workspace member** with read access to datasets. It uses the Execute Queries API to run DAX queries like `INFO.VIEW.RELATIONSHIPS()`.

> ⚠️ **Important**: This Service Principal requires workspace-level access, not tenant-level admin permissions. It must be added as a member of each Power BI workspace you want to access.

### Required API Permissions

| API / Permission | Type | Description | Admin Consent Required |
|------------------|------|-------------|------------------------|
| **Power BI REST APIs** | | | |
| `user_impersonation` | Delegated | Access Power BI workspace | No |
| **Power BI Service** | | | |
| `Dataset.Read.All` | Delegated | View all datasets | No |
| `Tenant.Read.All` | Application | View all content in tenant | **Yes** |

### Step-by-Step Setup

#### Step 1: Create Azure AD App Registration

1. Navigate to [Azure Portal](https://portal.azure.com)
2. Go to **Azure Active Directory** → **App registrations**
3. Click **New registration**
4. Configure the registration:
   - **Name**: Enter an application name (e.g., "PowerBI-NonAdmin-Connector")
   - **Supported account types**: Select **Accounts in this organizational directory only**
   - **Redirect URI**: Leave blank (not required for service principal)
5. Click **Register**
6. **Save the Application (client) ID** - you'll need this later
7. **Save the Directory (tenant) ID** from the Overview page

#### Step 2: Configure API Permissions

1. In your App Registration, go to **API permissions**
2. Click **Add a permission**
3. Select **Power BI Service**
4. Choose **Delegated permissions** and add:
   - `Dataset.Read.All` - View all datasets
   - `user_impersonation` - Access Power BI workspace (under Power BI REST APIs if shown separately)
5. Click **Add a permission** again
6. Select **Power BI Service**
7. Choose **Application permissions** and add:
   - `Tenant.Read.All` - View all content in tenant
8. Click **Grant admin consent for [Your Organization]**

> ⚠️ **Note**: Admin consent is required for `Tenant.Read.All`. You may need to request this from your Azure AD administrator if you don't have admin rights.

#### Step 3: Create Client Secret

1. In your App Registration, go to **Certificates & secrets**
2. Click **New client secret**
3. Configure the secret:
   - **Description**: Enter a description (e.g., "PowerBI Migration Tool - NonAdmin")
   - **Expires**: Select expiration period (recommended: 24 months)
4. Click **Add**
5. **IMMEDIATELY COPY THE SECRET VALUE** - you cannot retrieve it later!

> 🔴 **Security Warning**: Store the client secret securely. Never commit it to source control or share it publicly. Treat it like a password.

#### Step 4: Add Service Principal to Power BI Workspace

1. Go to [Power BI Service](https://app.powerbi.com)
2. Navigate to the workspace you want to access
3. Click **Access** (gear icon or "..." menu)
4. Click **Add people or groups**
5. Search for your App Registration name (e.g., "PowerBI-NonAdmin-Connector")
6. Select the application and assign the **Viewer** role (minimum required)
7. Click **Add**

> **Tip**: For migration purposes, you may want to assign **Contributor** role to ensure full read access to all dataset metadata.

#### Step 5: Configure the Migration Tool

1. Navigate to the **Configuration** page in Kasal
2. Select the **Power BI Non-Admin** configuration section
3. Enter the following values:

| Field | Value |
|-------|-------|
| **Tenant ID** | Your Azure AD tenant ID (from Step 1) |
| **Client ID** | Application (client) ID from Step 1 |
| **Client Secret** | Client secret value from Step 3 |

4. Click **Save Configuration**
5. Test the connection using the **Test Connection** button

### Verification Checklist

- [ ] App Registration created in Azure AD
- [ ] `Dataset.Read.All` (Delegated) permission added
- [ ] `Tenant.Read.All` (Application) permission added
- [ ] Admin consent granted for all permissions
- [ ] Client secret created and stored securely
- [ ] Service Principal added to Power BI workspace(s) as Member
- [ ] Connection test successful in Kasal

---

## Guide 2: Admin API Service Principal (Scan/Admin API)

### Overview

**Used by**: M-Query Conversion Pipeline

This Service Principal requires **Power BI Admin API access** to scan workspaces and extract M-Query expressions. This is a **tenant-level** permission that allows scanning workspace metadata across the entire organization.

> ⚠️ **Important**: Admin-level access is required because the Workspace Scan API (`/admin/workspaces/scanResult`) is an Admin API that requires tenant-wide permissions. Without admin-level permissions, you will receive authorization errors when attempting to scan workspaces.

### Business Context

We are developing a customer-facing tool to help users migrate analytics workloads from Power BI to Databricks with a single click. This leverages the Microsoft Power BI Admin Workspace APIs (specifically, the workspace scan API), which allows us to programmatically access model, lineage, M-Query expressions, and user details for all workspaces.

To fully test and roll out this capability, the service principal must access tenant-wide workspace metadata across the organization (not just individual workspaces it owns) using the Admin API.

### Required API Permissions

| API / Permission | Type | Description | Admin Consent Required |
|------------------|------|-------------|------------------------|
| **Microsoft Graph** | | | |
| `User.Read` | Delegated | Sign in and read user profile | No |
| **Power BI Service** | | | |
| `Dataset.ReadWrite.All` | Delegated | Read and write all datasets | No |

### Additional Tenant-Level Requirements

Beyond API permissions, the Service Principal must be enabled in **Power BI Admin Portal**:

1. The Service Principal must be added to a **security group** that has Admin API access
2. The **"Allow service principals to use Power BI APIs"** setting must be enabled
3. The **"Allow service principals to use read-only admin APIs"** setting must be enabled

### Step-by-Step Setup

#### Step 1: Create Azure AD App Registration

1. Navigate to [Azure Portal](https://portal.azure.com)
2. Go to **Azure Active Directory** → **App registrations**
3. Click **New registration**
4. Configure the registration:
   - **Name**: Enter an application name (e.g., "PowerBI-Admin-Connector")
   - **Supported account types**: Select **Accounts in this organizational directory only**
   - **Redirect URI**: Leave blank
5. Click **Register**
6. **Save the Application (client) ID** - you'll need this later
7. **Save the Directory (tenant) ID** from the Overview page

#### Step 2: Configure API Permissions

1. In your App Registration, go to **API permissions**
2. Click **Add a permission**
3. Select **Microsoft Graph**
4. Choose **Delegated permissions** and add:
   - `User.Read` - Sign in and read user profile
5. Click **Add a permission** again
6. Select **Power BI Service**
7. Choose **Delegated permissions** and add:
   - `Dataset.ReadWrite.All` - Read and write all datasets
8. Click **Grant admin consent for [Your Organization]**

#### Step 3: Create Client Secret

1. In your App Registration, go to **Certificates & secrets**
2. Click **New client secret**
3. Configure the secret:
   - **Description**: Enter a description (e.g., "PowerBI Migration Tool - Admin")
   - **Expires**: Select expiration period (recommended: 24 months)
4. Click **Add**
5. **IMMEDIATELY COPY THE SECRET VALUE** - you cannot retrieve it later!

> 🔴 **Security Warning**: Store the client secret securely. This secret grants tenant-wide admin access to Power BI metadata. Treat it with the highest security.

#### Step 4: Create Security Group for Admin API Access

1. In Azure Portal, go to **Azure Active Directory** → **Groups**
2. Click **New group**
3. Configure the group:
   - **Group type**: Security
   - **Group name**: Enter a name (e.g., "PowerBI-AdminAPI-ServicePrincipals")
   - **Group description**: "Service Principals with Power BI Admin API access"
   - **Membership type**: Assigned
4. Click **Create**
5. Open the newly created group
6. Go to **Members** → **Add members**
7. Search for your App Registration name (e.g., "PowerBI-Admin-Connector")
8. Select the application and click **Select**

#### Step 5: Enable Service Principal in Power BI Admin Portal

1. Go to [Power BI Admin Portal](https://app.powerbi.com/admin-portal/tenantSettings)
2. Sign in with a Power BI Admin account
3. Scroll to **Developer settings** section
4. Enable **"Allow service principals to use Power BI APIs"**:
   - Toggle the setting to **Enabled**
   - Under **Apply to**, select **Specific security groups**
   - Add your security group (e.g., "PowerBI-AdminAPI-ServicePrincipals")
5. Enable **"Allow service principals to use read-only admin APIs"**:
   - Toggle the setting to **Enabled**
   - Under **Apply to**, select **Specific security groups**
   - Add your security group (e.g., "PowerBI-AdminAPI-ServicePrincipals")
6. Click **Apply**

> ⚠️ **Note**: Changes to Power BI tenant settings may take **up to 15 minutes** to propagate. Wait before testing the connection.

#### Step 6: Configure the Migration Tool

1. Navigate to the **Configuration** page in Kasal
2. Select the **Power BI Admin** configuration section
3. Enter the following values:

| Field | Value |
|-------|-------|
| **Tenant ID** | Your Azure AD tenant ID (from Step 1) |
| **Client ID** | Application (client) ID from Step 1 |
| **Client Secret** | Client secret value from Step 3 |

4. Click **Save Configuration**
5. Wait 15 minutes after enabling tenant settings
6. Test the connection using the **Test Connection** button

### Verification Checklist

- [ ] App Registration created in Azure AD
- [ ] `User.Read` (Delegated, Microsoft Graph) permission added
- [ ] `Dataset.ReadWrite.All` (Delegated, Power BI Service) permission added
- [ ] Admin consent granted for all permissions
- [ ] Client secret created and stored securely
- [ ] Security group created for Admin API access
- [ ] Service Principal added to security group
- [ ] "Allow service principals to use Power BI APIs" enabled in Admin Portal
- [ ] "Allow service principals to use read-only admin APIs" enabled in Admin Portal
- [ ] Security group added to both settings
- [ ] Waited 15 minutes for propagation
- [ ] Connection test successful in Kasal

---

## When to Use a Service Account Instead

### Row-Level Security (RLS) Considerations

Some organizations implement **Row-Level Security (RLS)** in Power BI, which filters data based on the user identity. This can affect Service Principal access:

**Problem**: When RLS is configured, a Service Principal may:
- See no data (if not mapped to an RLS role)
- See filtered data (if mapped to a restrictive role)
- Be blocked entirely from querying the dataset

**Solution**: Use a **Service Account** (a regular user account) instead:

1. **Create a dedicated service account** (e.g., `svc-powerbi-migration@company.com`)
2. **Assign the service account** to the appropriate RLS role that grants full data access
3. **Use service account credentials** in the tool configuration instead of SP credentials

**When Service Account is Recommended:**
- Your Power BI datasets have RLS enabled
- You need to see all data during migration (not filtered by RLS)
- Your organization's security policies restrict Service Principal access to RLS-protected datasets
- You're migrating calculated columns that reference RLS-filtered data

**Trade-offs:**
| Aspect | Service Principal | Service Account |
|--------|-------------------|-----------------|
| Security | Better (no password) | Requires secure credential storage |
| RLS Handling | May be blocked/filtered | Can be assigned to "full access" role |
| Maintenance | Certificates/secrets expire | Password rotation needed |
| Audit Trail | Shows as "App" in logs | Shows as user in logs |

---

## Tool-Specific Configuration

### 1. Measure Conversion Pipeline

**Purpose**: Converts DAX measures from Power BI to:
- Unity Catalog Metrics (UC Metrics YAML)
- Databricks SQL
- DAX (re-formatted)

**API Used**: Execute Queries API with `EVALUATE INFO.MEASURES()`

**SVP Requirement**: Non-Admin API SVP (workspace member)

**Configuration:**
```yaml
workspace_id: "your-workspace-guid"
dataset_id: "your-dataset-guid"  # Semantic Model ID
tenant_id: "your-tenant-guid"
client_id: "your-sp-client-id"
client_secret: "your-sp-secret"

# Output options
inbound_connector: "powerbi"
outbound_format: "uc_metrics"  # or "sql", "dax"
target_catalog: "main"
target_schema: "default"
```

### 2. M-Query Conversion Pipeline

**Purpose**: Extracts and converts M-Query (Power Query) expressions to Databricks SQL views. Supports:
- `Value.NativeQuery` (SQL passthrough)
- `DatabricksMultiCloud.Catalogs` connections
- `Sql.Database` connections
- `Table.FromRows` (static data tables)
- ODBC, Oracle, Snowflake connections

**API Used**: Admin API (Workspace Scan)

**SVP Requirement**: **Admin API SVP** (tenant-level admin access)

**Configuration:**
```yaml
workspace_id: "your-workspace-guid"
dataset_id: "your-dataset-guid"  # Optional, scans all if not specified
tenant_id: "your-tenant-guid"
client_id: "your-admin-sp-client-id"
client_secret: "your-admin-sp-secret"

# LLM for complex conversions (optional)
llm_workspace_url: "https://your-workspace.databricks.com"
llm_token: "your-databricks-token"
llm_model: "databricks-claude-sonnet-4"
use_llm: true

# Output options
target_catalog: "main"
target_schema: "default"
```

**Supported Expression Types:**
| Expression Type | Conversion Method | Output |
|-----------------|-------------------|--------|
| `Value.NativeQuery` | Direct extraction | CREATE VIEW with embedded SQL |
| `DatabricksMultiCloud.Catalogs` | Direct mapping | CREATE VIEW referencing catalog |
| `Sql.Database` | LLM conversion | CREATE VIEW with transformed SQL |
| `Table.FromRows` | Rule-based | CREATE VIEW with VALUES clause |
| Other | LLM conversion | Best-effort SQL generation |

### 3. Power BI Relationships Tool

**Purpose**: Extracts relationships from Power BI semantic models and generates Unity Catalog Foreign Key constraints.

**API Used**: Execute Queries API with `EVALUATE INFO.VIEW.RELATIONSHIPS()`

**SVP Requirement**: Non-Admin API SVP (workspace member)

**Configuration:**
```yaml
workspace_id: "your-workspace-guid"
dataset_id: "your-dataset-guid"
tenant_id: "your-tenant-guid"
client_id: "your-sp-client-id"
client_secret: "your-sp-secret"

# Output options
target_catalog: "main"
target_schema: "default"
include_inactive: false
skip_system_tables: true
```

**Output Example:**
```sql
-- Relationship: sales_to_customer
ALTER TABLE main.default.sales
ADD CONSTRAINT fk_sales_customer_id_customer
FOREIGN KEY (customer_id)
REFERENCES main.default.customer(customer_id)
NOT ENFORCED;
```

### 4. Power BI Hierarchies Tool

**Purpose**: Extracts hierarchies from Fabric semantic models and generates:
- Unity Catalog dimension views with `hierarchy_path` column
- Metadata table (`_metadata_hierarchies`) with hierarchy definitions

**API Used**: Fabric API `getDefinition` endpoint (returns TMDL format)

**SVP Requirement**: Non-Admin API SVP with `SemanticModel.ReadWrite.All`

**Important**: This tool works with **Microsoft Fabric workspaces only** (not legacy Power BI Service workspaces).

**Configuration:**
```yaml
workspace_id: "your-fabric-workspace-guid"
dataset_id: "your-semantic-model-guid"
tenant_id: "your-tenant-guid"
client_id: "your-sp-client-id"
client_secret: "your-sp-secret"

# Output options
target_catalog: "main"
target_schema: "default"
skip_system_tables: true
include_hidden: false
```

**Output Example:**
```sql
-- Dimension View with hierarchy_path
CREATE OR REPLACE VIEW main.default.dim_customer_geography AS
SELECT DISTINCT
  Country,
  City,
  PostalCode,
  CONCAT(Country, ' > ', City, ' > ', PostalCode) AS hierarchy_path
FROM main.default.customer
ORDER BY Country, City, PostalCode;

-- Metadata Table
CREATE TABLE IF NOT EXISTS main.default._metadata_hierarchies (
  hierarchy_name STRING,
  table_name STRING,
  level_ordinal INT,
  level_name STRING,
  column_name STRING,
  is_hidden BOOLEAN
);
```

### 5. Field Parameters & Calculation Groups Tool

**Purpose**: Extracts Field Parameters and Calculation Groups from Fabric semantic models and generates:
- SQL UNION views that replicate the dynamic column switching behavior
- Metadata tables for field parameter and calculation group definitions

**API Used**: Fabric API `getDefinition` endpoint (returns TMDL format)

**SVP Requirement**: Non-Admin API SVP with `SemanticModel.ReadWrite.All`

**Important**: This tool works with **Microsoft Fabric workspaces only** (not legacy Power BI Service workspaces).

**Configuration:**
```yaml
workspace_id: "your-fabric-workspace-guid"
dataset_id: "your-semantic-model-guid"
tenant_id: "your-tenant-guid"
client_id: "your-sp-client-id"
client_secret: "your-sp-secret"

# Output options
target_catalog: "main"
target_schema: "default"
output_format: "sql"  # "sql", "json", or "markdown"
include_metadata_table: true
```

**Output Example (Field Parameter):**
```sql
-- Field Parameter View: Dynamic Measure Selector
CREATE OR REPLACE VIEW main.default.vw_field_param_measure_selector AS
SELECT 'Revenue' AS field_name, SUM(amount) AS value FROM main.default.sales
UNION ALL
SELECT 'Profit' AS field_name, SUM(profit) AS value FROM main.default.sales
UNION ALL
SELECT 'Units Sold' AS field_name, SUM(quantity) AS value FROM main.default.sales;
```

**Output Example (Calculation Group):**
```sql
-- Calculation Group View: Time Intelligence
CREATE OR REPLACE VIEW main.default.vw_calc_group_time_intelligence AS
SELECT 'Current' AS calculation_name, SUM(amount) AS value FROM main.default.sales
UNION ALL
SELECT 'YoY' AS calculation_name,
       SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY year) AS value
FROM main.default.sales
UNION ALL
SELECT 'YTD' AS calculation_name,
       SUM(SUM(amount)) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS value
FROM main.default.sales;
```

**Full Guide**: [Field Parameters & Calculation Groups Tool](./tool-field-parameters.md)

### 6. Report References Tool

**Purpose**: Extracts visual-to-measure/table mappings from Fabric reports using the Report Definition API (PBIR format). Generates:
- Report structure analysis (pages, visuals)
- Measure/table usage mapping per page with **direct page URLs for navigation**
- Cross-reference matrix showing which measures are used where

**API Used**: Fabric Report Definition API `getDefinition` endpoint (returns PBIR format)

**SVP Requirement**: Non-Admin API SVP with `Report.ReadWrite.All`

**Important**: This tool works with **Microsoft Fabric reports only** (PBIR format). Traditional .pbix uploads may not have this format.

**Key Feature**: **Dataset-based Discovery** - Provide a `dataset_id` to automatically discover ALL reports using that semantic model and extract references from each.

**Configuration:**
```yaml
workspace_id: "your-fabric-workspace-guid"
dataset_id: "your-semantic-model-guid"    # Recommended: discovers ALL reports using this dataset
# report_id: "your-report-guid"           # Alternative: single specific report

tenant_id: "your-tenant-guid"
client_id: "your-sp-client-id"
client_secret: "your-sp-secret"

# Output options
output_format: "json"  # "json", "markdown", or "matrix"
include_visual_details: true
group_by: "page"       # "page", "measure", or "table"
```

**Output Example (JSON):**
```json
{
  "report_references": [
    {
      "report_id": "f8ebce44-c9cb-4041-88d2-4ca33f314e82",
      "report_name": "Sales Dashboard",
      "report_url": "https://app.powerbi.com/groups/.../reports/...",
      "pages": [
        {
          "page_name": "Revenue Overview",
          "page_url": "https://app.powerbi.com/groups/.../reports/.../ReportSection1",
          "measures": [
            {"measure_name": "Total Revenue", "table_name": "Sales"},
            {"measure_name": "YoY Growth", "table_name": "Sales"}
          ]
        }
      ]
    }
  ],
  "measure_usage": {
    "Total Revenue": [
      {"report_name": "Sales Dashboard", "page_name": "Revenue Overview"}
    ]
  }
}
```

**Use Cases:**
- **Impact Analysis**: Before modifying a measure, see all report pages that will be affected
- **Documentation**: Generate comprehensive report-to-measure mappings
- **Migration Planning**: Understand report dependencies on semantic model measures
- **Audit**: Track measure usage across all reports in a workspace

**Full Guide**: [Report References Tool](./tool-report-references.md)

### 7. Comprehensive Analysis Tool

**Purpose**: Answer ad-hoc business questions by converting natural language queries into DAX queries and executing them against Power BI semantic models. Features intelligent self-correction with up to 5 retry attempts.

**API Used**: Execute Queries API with `EVALUATE` DAX queries

**SVP Requirement**: Non-Admin API SVP with `SemanticModel.ReadWrite.All`

**Key Features**:
- **Intelligent Self-Correction**: Automatically retries failed queries with LLM-based error analysis
- **Measure Hallucination Detection**: Validates generated DAX uses only available measures
- **Visual References**: Identifies which reports use the queried measures
- **Enhanced Logging**: Full diagnostic logging for debugging and optimization

**Configuration:**
```yaml
# Required
user_question: "What are total sales by region?"
workspace_id: "your-workspace-guid"
dataset_id: "your-dataset-guid"

# Authentication (Service Principal)
tenant_id: "your-tenant-guid"
client_id: "your-sp-client-id"
client_secret: "your-sp-secret"

# OR User OAuth
# access_token: "user-oauth-token"

# LLM for intelligent DAX generation (optional)
llm_workspace_url: "https://your-workspace.databricks.com"
llm_token: "your-databricks-token"
llm_model: "databricks-claude-sonnet-4"

# Retry configuration
max_dax_retries: 5  # 1-10, default 5

# Output options
include_visual_references: true
skip_system_tables: true
output_format: "markdown"  # or "json"
```

**Output Example (with retry history):**
```markdown
# Power BI Analysis Results

**Question**: What are total sales by region?

## Generated DAX Query

**Attempts**: 2 (successful on attempt 2)

### Retry History
**Attempt 1**: ❌ Failed
  - Error: Table 'Sales' does not exist
**Attempt 2**: ✅ Success

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Geography'[Region],
    "Total Sales", [Total Sales]
)
ORDER BY [Total Sales] DESC
```

## Execution Results

✅ **Success** - 5 rows returned

| Region | Total Sales |
| --- | --- |
| North | 1,234,567 |
| South | 987,654 |
```

**Use Cases:**
- **Ad-hoc Analysis**: Answer business questions without writing DAX manually
- **Data Exploration**: Quickly explore semantic model data with natural language
- **Self-Service BI**: Enable non-technical users to query Power BI data
- **Validation**: Test measure logic and data quality
- **Learning**: Understand DAX through LLM-generated examples

**How Self-Correction Works:**
1. LLM generates DAX from user question + model context
2. Execute query against Power BI
3. If failed:
   - Capture exact error message
   - LLM analyzes error and generates corrected DAX
   - Re-execute corrected query
   - Repeat up to `max_dax_retries` times
4. Return results with full retry history

**Corrects:**
- Table/column name errors
- Syntax errors
- Relationship errors
- Type mismatches
- Measure reference errors

**Performance:**
- First attempt success: ~2-5 seconds
- Each retry: +2-5 seconds
- Max latency (5 retries): ~25-30 seconds

**Full Guide**: [Comprehensive Analysis Tool](./tool-comprehensive-analysis.md)

---

## Quick Reference: Which SVP Do I Need?

```
┌────────────────────────────────────────┬───────────────────┬─────────────────────────┐
│ Tool                                   │ SVP Type          │ Key Permission          │
├────────────────────────────────────────┼───────────────────┼─────────────────────────┤
│ Measure Conversion Pipeline            │ Non-Admin API     │ Dataset.Read.All        │
│ M-Query Conversion Pipeline            │ Admin API         │ Dataset.ReadWrite.All   │
│ Power BI Relationships Tool            │ Non-Admin API     │ Dataset.Read.All        │
│ Power BI Hierarchies Tool              │ Non-Admin API     │ SemanticModel.ReadWrite │
│ Field Parameters & Calculation Groups  │ Non-Admin API     │ SemanticModel.ReadWrite │
│ Report References Tool                 │ Non-Admin API     │ Report.ReadWrite.All    │
│ Comprehensive Analysis Tool            │ Non-Admin API     │ SemanticModel.ReadWrite │
└────────────────────────────────────────┴───────────────────┴─────────────────────────┘
```

---

## Troubleshooting

### Common Issues

**1. "Unauthorized" or "Forbidden" errors**
- Verify the Service Principal has the correct permissions
- Check if admin consent was granted for Application permissions
- For Non-Admin API: Ensure the SP is added to the Power BI workspace as a member
- For Admin API: Ensure the SP is in the allowed security group in tenant settings

**2. "No data returned" from queries**
- RLS may be filtering data - consider using a Service Account
- Check if the dataset has any data
- Verify the SP has access to the specific dataset

**3. M-Query tool returns empty results**
- Admin API SVP is required - check tenant settings
- Ensure "Allow service principals to use Power BI APIs" is enabled
- Ensure "Allow service principals to use read-only admin APIs" is enabled
- Verify the SP is in the allowed security group
- Wait 15 minutes after enabling settings for propagation

**4. Hierarchies tool fails**
- Only works with Fabric workspaces (not legacy Power BI Service)
- SP needs `SemanticModel.ReadWrite.All` permission
- Check if the semantic model has any hierarchies defined

**5. Static tables (Table.FromRows) not converted**
- As of latest version, Table.FromRows expressions are automatically converted
- Check that `skip_static_tables` is not explicitly set to skip them

**6. Connection test fails**
- Check the following:
  - Client ID and Client Secret are correct (no extra spaces)
  - Tenant ID matches your Azure AD tenant
  - Client secret hasn't expired
  - Firewall or network policies aren't blocking Power BI API access

**7. No workspaces visible (Admin API)**
- Ensure that:
  - Service principal has read-only admin API access enabled in Power BI
  - Application permissions (not just delegated) are properly configured
  - The service principal is added to the security group allowed to use admin APIs

---

## Additional Resources

### Microsoft Documentation

- [Admin - WorkspaceInfo PostWorkspaceInfo API](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info)
- [Automate Premium workspace and dataset tasks with service principals](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-service-principal)
- [Register an application with Microsoft identity platform](https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
- [Power BI REST API Reference](https://learn.microsoft.com/en-us/rest/api/power-bi/)

---

## Best Practices

1. **Use separate Service Principals** for Admin and Non-Admin APIs
2. **Store credentials securely** - use Azure Key Vault or Databricks secrets
3. **Test with a single dataset first** before scanning entire workspaces
4. **Review RLS settings** before running migration tools
5. **Use dynamic mode** for crew configurations when deploying to production
6. **Validate generated SQL** in a development environment before production deployment

---

## Example Crews

Reference crew configurations are available in the `examples/` directory:

| Tool | Static Mode | Dynamic Mode |
|------|-------------|--------------|
| Measure Conversion | `crew_measureconverter_static.json` | `crew_measureconverter_dynamic.json` |
| M-Query Conversion | `crew_mqueryconverter_static.json` | `crew_mqueryconverter_dynamic.json` |
| Relationships | `crew_relationshipconverter_static.json` | `crew_relationshipconverter_dynamic.json` |
| Hierarchies | `crew_hierarchyconverter_static.json` | `crew_hierarchyconverter_dynamic.json` |

**Static Mode**: Credentials configured in UI tool settings
**Dynamic Mode**: Credentials provided at runtime via execution inputs

See the individual tool guides for detailed configuration examples.
