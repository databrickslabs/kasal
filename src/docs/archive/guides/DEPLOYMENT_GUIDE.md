# Databricks Apps Deployment Guide

This guide explains how to deploy your application to Databricks Apps.

## Prerequisites

1. **Build the frontend**: Run `python build.py` to create the `frontend_static` directory
2. **Configure app.yaml**: Ensure `app.yaml` exists in the root directory with proper OAuth scopes
3. **Databricks CLI**: Ensure the Databricks CLI is installed and configured

## Quick Start

```bash
# First, build the frontend
python build.py

# Then deploy the application
python deploy.py --app-name my-kasal-app --user-name your.email@company.com
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

## OAuth Scopes Configuration

OAuth scopes must be configured in the Databricks Apps UI after deployment, not in the app.yaml file.

### Required OAuth Scopes Setup

1. Deploy your application using `python deploy.py`
2. Navigate to your Databricks workspace → "Apps" → [Your App] → "Authorization"
3. Configure the required OAuth scopes based on your tools:

**Essential scopes for most use cases:**
```
dashboards.genie
sql
sql.warehouses
sql.statement-execution
serving.serving-endpoints
serving.serving-endpoints-data-plane
```

### Scope Dependencies by Tool

- **Genie Tool**: `dashboards.genie`, `sql`, `sql.warehouses`, `sql.statement-execution`
- **Model Serving**: `serving.serving-endpoints`, `serving.serving-endpoints-data-plane`
- **Vector Search**: `vectorsearch.vector-search-endpoints`, `vectorsearch.vector-search-indexes`

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
# Build the frontend
python build.py

# Deploy the application
python deploy.py \
  --app-name kasal-production \
  --user-name admin@company.com \
  --description "Production Kasal AI Agent Platform" \
  --profile production
```

### Post-Deployment Steps

1. **Configure OAuth Scopes**: Go to Databricks Apps UI → [Your App] → Authorization and add required scopes
2. **Test the Application**: Access the app URL and verify functionality
3. **User Authorization**: Users will be prompted to grant consent for the requested scopes when they first access the app

The deployment will:
1. Upload the backend code and frontend static files
2. Deploy to Databricks Apps using the existing `app.yaml`
3. Start the application