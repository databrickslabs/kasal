# Power BI Migration Tools Guide

This guide covers the Power BI migration tools available in Kasal for converting Power BI assets to Databricks Unity Catalog.

## Overview

Kasal provides four specialized tools for migrating different aspects of Power BI semantic models to Databricks:

| Tool | Purpose | API Used | SVP Type Required |
|------|---------|----------|-------------------|
| **Measure Conversion Pipeline** | Convert DAX measures to UC Metrics/SQL | Execute Queries API | Non-Admin API SVP |
| **M-Query Conversion Pipeline** | Convert M-Query/Power Query to SQL views | Admin API (Scan) | **Admin API SVP** |
| **Power BI Relationships Tool** | Extract relationships as FK constraints | Execute Queries API | Non-Admin API SVP |
| **Power BI Hierarchies Tool** | Extract hierarchies as dimension views | Fabric API (getDefinition) | Non-Admin API SVP |

---

## Service Principal Requirements

### Understanding the Two Types of Service Principals

Power BI has two distinct API access patterns, each requiring different permissions:

#### 1. Non-Admin API Service Principal (Execute Queries API)

**Used by**: Measure Conversion Pipeline, Relationships Tool, Hierarchies Tool

This Service Principal needs to be a **workspace member** with read access to datasets. It uses the Execute Queries API to run DAX queries like `INFO.VIEW.RELATIONSHIPS()`.

**Required API Permissions:**

| API / Permission | Type | Description | Admin Consent |
|------------------|------|-------------|---------------|
| Power BI REST APIs | | | |
| `user_impersonation` | Delegated | Access Power BI workspace | No |
| Power BI Service | | | |
| `Dataset.Read.All` | Delegated | View all datasets | No |
| `Tenant.Read.All` | Application | View all content in tenant | Yes |

**Setup Steps:**
1. Create an App Registration in Azure AD
2. Add the above API permissions
3. Grant admin consent for `Tenant.Read.All`
4. Add the Service Principal as a **Member** of the Power BI workspace
5. Ensure the SP has at least **Viewer** role on the datasets

#### 2. Admin API Service Principal (Scan/Admin API)

**Used by**: M-Query Conversion Pipeline

This Service Principal requires Power BI Admin API access to scan workspaces and extract M-Query expressions. This is a **tenant-level** permission.

**Required API Permissions:**

| API / Permission | Type | Description | Admin Consent |
|------------------|------|-------------|---------------|
| Microsoft Graph | | | |
| `User.Read` | Delegated | Sign in and read user profile | No |
| Power BI Service | | | |
| `Dataset.ReadWrite.All` | Delegated | Read and write all datasets | No |

**Additional Requirements:**
- The Service Principal must be enabled in Power BI Admin Portal:
  1. Go to **Power BI Admin Portal** → **Tenant Settings**
  2. Enable **"Allow service principals to use Power BI APIs"**
  3. Add your Service Principal to the allowed security group
- The SP needs to be in a security group with Admin API access

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

---

## Quick Reference: Which SVP Do I Need?

```
┌─────────────────────────────────┬───────────────────┬─────────────────────────┐
│ Tool                            │ SVP Type          │ Key Permission          │
├─────────────────────────────────┼───────────────────┼─────────────────────────┤
│ Measure Conversion Pipeline     │ Non-Admin API     │ Dataset.Read.All        │
│ M-Query Conversion Pipeline     │ Admin API         │ Dataset.ReadWrite.All   │
│ Power BI Relationships Tool     │ Non-Admin API     │ Dataset.Read.All        │
│ Power BI Hierarchies Tool       │ Non-Admin API     │ SemanticModel.ReadWrite │
└─────────────────────────────────┴───────────────────┴─────────────────────────┘
```

---

## Troubleshooting

### Common Issues

**1. "Unauthorized" or "Forbidden" errors**
- Verify the Service Principal has the correct permissions
- Check if admin consent was granted for Application permissions
- Ensure the SP is added to the Power BI workspace as a member

**2. "No data returned" from queries**
- RLS may be filtering data - consider using a Service Account
- Check if the dataset has any data
- Verify the SP has access to the specific dataset

**3. M-Query tool returns empty results**
- Admin API SVP is required - check tenant settings
- Ensure "Allow service principals to use Power BI APIs" is enabled
- Verify the SP is in the allowed security group

**4. Hierarchies tool fails**
- Only works with Fabric workspaces (not legacy Power BI Service)
- SP needs `SemanticModel.ReadWrite.All` permission
- Check if the semantic model has any hierarchies defined

**5. Static tables (Table.FromRows) not converted**
- As of latest version, Table.FromRows expressions are automatically converted
- Check that `skip_static_tables` is not explicitly set to skip them

---

## Best Practices

1. **Use separate Service Principals** for Admin and Non-Admin APIs
2. **Store credentials securely** - use Azure Key Vault or Databricks secrets
3. **Test with a single dataset first** before scanning entire workspaces
4. **Review RLS settings** before running migration tools
5. **Use dynamic mode** for crew configurations when deploying to production
6. **Validate generated SQL** in a development environment before production deployment
