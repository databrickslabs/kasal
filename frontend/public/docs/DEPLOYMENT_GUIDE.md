# Databricks Apps Deployment Guide

This guide explains how to deploy your application to Databricks Apps with proper OAuth scopes configuration.

## Quick Start

```bash
# Deploy with default comprehensive scopes (recommended)
python deploy.py --app-name my-kasal-app --user-name your.email@company.com

# Deploy with minimal scopes for Genie functionality only
python deploy.py --app-name my-kasal-app --user-name your.email@company.com \
  --oauth-scopes dashboards.genie sql sql.warehouses sql.statement-execution

# Deploy using a custom configuration template
python deploy.py --app-name my-kasal-app --user-name your.email@company.com \
  --config-template app-config-template.yaml
```

## OAuth Scopes

The application supports the following OAuth scopes:

### Essential Scopes
- `dashboards.genie` - **Required** for Genie tool functionality
- `sql` - Basic SQL operations
- `sql.warehouses` - Access to SQL warehouses
- `sql.statement-execution` - Execute SQL statements

### Additional SQL Scopes
- `sql.alerts` - SQL alerts management
- `sql.alerts-legacy` - Legacy SQL alerts
- `sql.dashboards` - Dashboard access
- `sql.data-sources` - Data sources management
- `sql.dbsql-permissions` - DB SQL permissions
- `sql.queries` - Access to SQL queries
- `sql.queries-legacy` - Legacy SQL queries
- `sql.query-history` - Query history access

### Vector Search & Serving
- `vectorsearch.vector-search-endpoints` - Vector search capabilities
- `vectorsearch.vector-search-indexes` - Vector search indexes
- `serving.serving-endpoints` - Model serving endpoints
- `serving.serving-endpoints-data-plane` - Model serving data plane

### File Operations
- `files.files` - File operations

## Configuration Options

### Using Custom Scopes
```bash
python deploy.py --app-name my-app --user-name user@company.com \
  --oauth-scopes dashboards.genie sql sql.warehouses sql.statement-execution
```

### Using Configuration Template
1. Copy `app-config-template.yaml` to your custom configuration
2. Modify the `oauth_scopes` section as needed
3. Deploy with `--config-template` parameter

```bash
cp app-config-template.yaml my-app-config.yaml
# Edit my-app-config.yaml to customize scopes
python deploy.py --app-name my-app --user-name user@company.com \
  --config-template my-app-config.yaml
```

## Authentication Flow

The application implements OAuth/OBO authentication for Databricks Apps:

1. **User Access**: Users access the app through Databricks Apps
2. **Consent Screen**: Users grant permissions for requested scopes
3. **Token Forwarding**: Databricks Apps forwards user access tokens via `X-Forwarded-Access-Token` header
4. **Tool Authentication**: GenieTool and other tools use the user's token for API calls
5. **Permission Enforcement**: Users can only access data they have permissions for in Unity Catalog

## Minimal Genie-Only Configuration

For applications that only need Genie functionality:

```yaml
oauth_scopes:
  - dashboards.genie
  - sql
  - sql.warehouses
  - sql.statement-execution
```

## Troubleshooting

### Authentication Errors
- Ensure `dashboards.genie` scope is included for Genie tool functionality
- Verify users have appropriate Unity Catalog permissions
- Check that the app deployment includes the required scopes

### Permission Denied
- Users need Unity Catalog permissions for the data they're accessing
- The app can only access what the individual user is authorized to see
- Contact your Databricks admin to grant appropriate permissions

## Complete Deployment Example

```bash
# Build the wheel
python build.py

# Deploy with comprehensive scopes
python deploy.py \
  --app-name kasal-production \
  --user-name admin@company.com \
  --description "Production Kasal AI Agent Platform" \
  --profile production
```

The deployment will:
1. Create an `app.yaml` with the specified OAuth scopes
2. Upload the wheel file and dependencies
3. Deploy to Databricks Apps
4. Start the application

Users will be prompted to grant consent for the requested scopes when they first access the app.