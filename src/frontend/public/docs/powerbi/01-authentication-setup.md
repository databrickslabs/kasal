# Power BI authentication and service principal setup

Everything you need to configure before running any Power BI tool in Kasal.

- [Before you begin](#before-you-begin)
- [Why two service principals](#why-two-service-principals)
- [SP 1: non-admin service principal](#sp-1-non-admin-service-principal)
- [SP 2: admin service principal](#sp-2-admin-service-principal)
- [Tool 90: additional requirements](#tool-90-additional-requirements-two-sps-together)
- [Row-level security considerations](#row-level-security-rls-considerations)
- [Microsoft API reference](#microsoft-api-reference)
- [Storing credentials securely in Kasal](#storing-credentials-securely-in-kasal)
- [Troubleshooting](#troubleshooting)

## Before you begin

- Access to the Azure Portal with permission to create app registrations in your Azure AD tenant
- An Azure AD administrator who can grant admin consent for application permissions
- A Power BI Admin role (required only for the admin service principal and tenant settings)
- A Power BI or Fabric workspace containing the semantic model you want to access
- A running Kasal instance where you will store the credentials

## Why two service principals

Power BI's API surface has two distinct access tiers with different permission requirements:

| Tier | What it can do | Who uses it |
|------|---------------|-------------|
| Non-Admin SP (workspace member) | Execute queries against datasets you have access to | Tools 72, 73, 75, 79-82 |
| Admin SP (tenant-wide) | Scan all workspaces in the entire tenant | Tools 74, 90 |

You cannot collapse these into one SP without granting blanket tenant-admin access to a workspace-level principal - a security anti-pattern most organizations won't allow.

For Tools 76, 77, 78 (Fabric-only): same Non-Admin SP, but requires the additional `SemanticModel.ReadWrite.All` permission and a Fabric workspace (not legacy Power BI Service).

## SP 1: non-admin service principal

Used by: Tools 72, 73, 75, 79, 80, 81, 82

### Step 1 - create app registration

1. Go to [Azure Portal](https://portal.azure.com) → **Azure Active Directory** → **App registrations**
2. Click **New registration**
3. Name: e.g. `KasalPowerBI-NonAdmin`
4. Account type: **Accounts in this organizational directory only**
5. Redirect URI: leave blank
6. Click **Register**
7. **Save** the **Application (client) ID** and **Directory (tenant) ID** from the Overview page

### Step 2 - add API permissions

In the App Registration → **API permissions** → **Add a permission**:

| API | Permission | Type | Admin consent |
|-----|-----------|------|--------------|
| Power BI Service | `Dataset.Read.All` | Delegated | No |
| Power BI Service | `Tenant.Read.All` | Application | Yes |
| Power BI REST APIs | `user_impersonation` | Delegated | No |

Click **Grant admin consent for [Your Org]** after adding all permissions.

**Note:** `Tenant.Read.All` (Application type) requires an Azure AD admin to grant consent. If you don't have that access, request it from your Azure team.

### Step 3 - create client secret

1. App Registration → **Certificates & secrets** → **New client secret**
2. Description: `KasalPowerBI-NonAdmin`
3. Expiry: 24 months recommended
4. Click **Add**
5. **Copy the secret value immediately** - it is never shown again

### Step 4 - add SP to Power BI workspace

1. Go to [Power BI Service](https://app.powerbi.com)
2. Open the workspace containing your semantic model
3. Click **Access** (gear icon or `...` menu)
4. Search for `KasalPowerBI-NonAdmin` (your app registration name)
5. Assign role: **Viewer** (minimum) or **Contributor** (recommended for full metadata access)
6. Click **Add**

Repeat this for every workspace you want to access. The SP must be a workspace member - it cannot access datasets it has no membership for, even with `Tenant.Read.All`.

### Checklist - non-admin SP

- [ ] App Registration created
- [ ] `Dataset.Read.All` (Delegated) added
- [ ] `Tenant.Read.All` (Application) added
- [ ] Admin consent granted
- [ ] Client secret created and saved securely
- [ ] SP added to target Power BI workspace(s)

## SP 2: admin service principal

Used by: Tools 74 (M-Query extraction), 90 (Pipeline Config Generator)

This SP needs tenant-level scanning capability via the [Power BI Admin Workspace Scan API](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info).

### Step 1 - create app registration

Same as Non-Admin SP above. Name it `KasalPowerBI-Admin`.

Save the **Application (client) ID** and **Directory (tenant) ID**.

### Step 2 - add API permissions

| API | Permission | Type | Admin consent |
|-----|-----------|------|--------------|
| Microsoft Graph | `User.Read` | Delegated | No |
| Power BI Service | `Dataset.ReadWrite.All` | Delegated | No |

Click **Grant admin consent**.

### Step 3 - create client secret

Same process as SP 1. Name it `KasalPowerBI-Admin`.

### Step 4 - create security group for Admin API

The Power BI Admin Portal controls which SPs can use read-only admin APIs via a security group allowlist.

1. Azure Portal → **Azure Active Directory** → **Groups** → **New group**
2. Type: Security
3. Name: `KasalPowerBI-AdminAPI-SPs`
4. Members: add your `KasalPowerBI-Admin` app registration
5. Click **Create**

### Step 5 - enable in Power BI Admin Portal

1. Go to [Power BI Admin Portal](https://app.powerbi.com/admin-portal/tenantSettings)
2. Sign in as a **Power BI Admin**
3. Scroll to **Developer settings**
4. Enable **"Allow service principals to use Power BI APIs"**
   - Set to: **Specific security groups**
   - Add: `KasalPowerBI-AdminAPI-SPs`
5. Enable **"Allow service principals to use read-only admin APIs"**
   - Set to: **Specific security groups**
   - Add: `KasalPowerBI-AdminAPI-SPs`
6. Click **Apply**

> [!IMPORTANT]
> Wait 15 minutes after saving before testing. Tenant setting changes propagate slowly.

### Checklist - admin SP

- [ ] App Registration created
- [ ] `User.Read` (Delegated, Microsoft Graph) added
- [ ] `Dataset.ReadWrite.All` (Delegated, Power BI Service) added
- [ ] Admin consent granted
- [ ] Client secret created and saved securely
- [ ] Security group created in Azure AD
- [ ] Admin SP added to security group
- [ ] "Allow service principals to use Power BI APIs" enabled (security group)
- [ ] "Allow service principals to use read-only admin APIs" enabled (security group)
- [ ] Waited 15 minutes for propagation

## Tool 90: additional requirements (two SPs together)

Tool 90 (Pipeline Config Generator) calls 4 different PBI APIs that require different permission levels in a single run. It needs **both** SPs configured simultaneously:

| SP | API called | What it extracts |
|----|-----------|-----------------|
| Non-Admin SP | Execute Queries (`INFO.VIEW.RELATIONSHIPS()`) | Relationships, measures |
| Non-Admin SP | Execute Queries (`$SYSTEM.MDSCHEMA_MEASURES`) | DAX measure details |
| Admin SP | Admin Scanner (`/admin/workspaces/scanResult`) | Columns, M-Query, hidden flags |
| Non-Admin SP | Report Definition (optional) | Visual metadata |

Configuration in Tool 90:

```text
tenant_id         → shared by both SPs (same Azure AD tenant)
client_id         → Non-Admin SP client ID
client_secret     → Non-Admin SP client secret
admin_client_id   → Admin SP client ID
admin_client_secret → Admin SP client secret
```

## Row-level security (RLS) considerations

If your Power BI datasets use RLS, a Service Principal may see no data or filtered data because RLS rules typically don't include SPs.

Symptoms:

- Tool returns empty results or `[0 rows]`
- No errors, but data is missing

Solution - use a Service Account instead:

1. Create a dedicated user account (e.g. `svc-kasal-pbi@company.com`)
2. Assign it to an RLS role that grants full data access
3. Use `auth_method: "service_account"` with `username` + `password` instead of SP credentials

| Aspect | Service Principal | Service Account |
|--------|-------------------|-----------------|
| Security | Better (no password) | Requires credential storage |
| RLS | May be blocked/filtered | Can get full-access RLS role |
| Maintenance | Secret expiry | Password rotation |

## Microsoft API reference

These are the key Power BI Admin REST API endpoints used by Kasal tools:

| Endpoint | Used by | Docs |
|----------|---------|------|
| `POST /admin/workspaces/getInfo` | Tool 74, 90 | [PostWorkspaceInfo](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info) |
| `GET /admin/workspaces/scanResult/{scanId}` | Tool 74, 90 | [GetScanResult](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result) |
| `POST /groups/{groupId}/datasets/{datasetId}/executeQueries` | Tools 72, 73, 75, 79-82 | [ExecuteQueries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries) |
| `GET /groups/{groupId}/items/{itemId}/getDefinition` | Tools 76, 77, 78 | [GetDefinition (Fabric)](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition) |

## Storing credentials securely in Kasal

Never hardcode secrets in crew task descriptions. Use one of:

1. **Kasal Configuration UI**: Enter credentials in the tool's config form - they are stored encrypted
2. **Dynamic mode**: Pass `{client_secret}` as a runtime placeholder; supply via execution inputs
3. **Databricks Secrets**: Reference `{{secrets/scope/key}}` in task descriptions (Kasal resolves these at runtime)

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Unauthorized` / `403` | SP not in workspace, or missing permissions | Add SP to workspace; check admin consent |
| `Empty results, no error` | RLS filtering | Use Service Account with full-access RLS role |
| `Admin API returns nothing` | Admin Portal not configured | Enable both developer settings; wait 15 min |
| `401 for Admin API` | SP not in security group | Add SP to allowlisted security group |
| `Dataset.Read.All insufficient` | Trying Admin API with Non-Admin SP | Use Admin SP for tool 74 / 90 |
| `Hierarchies tool fails` | Legacy workspace, not Fabric | Tool 76/77/78 require Fabric workspaces |
| `Secret expired` | Client secret past expiry date | Rotate in Azure Portal, update in Kasal config |

## Related

- [Power BI integration hub](./README.md)
- [Simple migration story](./02-simple-migration-story.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)
- [Tool 74 - M-Query conversion pipeline](./tool-74-mquery-conversion.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)

Back to the [Power BI integration hub](./README.md).
