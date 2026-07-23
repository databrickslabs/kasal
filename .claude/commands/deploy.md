# deploy

Deploy to Databricks Apps (frontend is built on Databricks during deployment).

## Prerequisites Check

Before deploying, you must have the following environment variables set:

```bash
export DATABRICKS_TOKEN=<your-databricks-token>
export DATABRICKS_HOST=<your-databricks-host>
```

**IMPORTANT**: Ask the user to provide these values if not already set in the environment.

## Execution Flow

1. **First**, check if `DATABRICKS_TOKEN` and `DATABRICKS_HOST` are set:
   ```bash
   echo "DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:+[SET]}" && echo "DATABRICKS_HOST: ${DATABRICKS_HOST:+[SET]}"
   ```

2. **If not set**, ask the user to provide them using AskUserQuestion tool with these questions:
   - "What is your Databricks host URL?" (e.g., https://your-workspace.cloud.databricks.com)
   - "What is your Databricks personal access token?"

3. **Ask for deployment parameters** using AskUserQuestion:
   - "What app name should be used?" (default: kasal)
   - "What is your Databricks user email?"

4. **After receiving all inputs**, run the deployment with `yes` command to auto-confirm prompts (no local build needed — frontend builds on Databricks):
   ```bash
   cd ~/workspace/kasal && source ~/workspace/venv/bin/activate && export DATABRICKS_TOKEN=<provided-token> && export DATABRICKS_HOST=<provided-host> && yes | python src/deploy.py --app-name <app-name> --user-name <user-email>
   ```

5. **Configure OAuth scopes** after deployment completes (run in Python):
   ```python
   import subprocess
   import json

   app_name = "<app-name>"

   # Define valid OAuth scopes for Kasal app
   desired_scopes = [
       # SQL related scopes
       "sql",
       "sql.alerts",
       "sql.alerts-legacy",
       "sql.dashboards",
       "sql.data-sources",
       "sql.dbsql-permissions",
       "sql.queries",
       "sql.queries-legacy",
       "sql.query-history",
       "sql.statement-execution",
       "sql.warehouses",
       # Vector Search scopes - CRITICAL for index creation
       "vectorsearch.vector-search-endpoints",
       "vectorsearch.vector-search-indexes",
       # Serving endpoints
       "serving.serving-endpoints",
       # Files
       "files.files",
       # Dashboards/Genie
       "dashboards.genie",
   ]

   payload = {"user_api_scopes": desired_scopes}

   cmd = [
       "databricks", "api", "patch",
       f"/api/2.0/apps/{app_name}",
       "--json", json.dumps(payload),
       "-o", "json"
   ]

   result = subprocess.run(cmd, capture_output=True, text=True)
   if result.returncode == 0:
       print(f"✅ OAuth scopes configured successfully for {app_name}")
   else:
       print(f"⚠️ OAuth scope configuration failed: {result.stderr}")
   ```

## Description

This command:
1. Validates Databricks credentials are available
2. Uploads frontend source, backend, docs, and package.json to Databricks workspace
3. Databricks Apps auto-detects package.json and runs `npm install` + `npm run build` to build frontend
4. Deploys to Databricks Apps platform (auto-confirms prompts)
5. Configures OAuth scopes for the app (SQL, Vector Search, Serving, Files, Dashboards)

**Note**: `frontend_static/` is no longer tracked in git. The frontend is built on Databricks Apps during deployment using npm lifecycle hooks in `src/package.json`. For local development, `python src/build.py` still works.

## Usage

Type `/deploy` in Claude Code to build and deploy. You will be prompted for:
- Databricks host URL (if not set)
- Databricks personal access token (if not set)
- App name for deployment
- User email for deployment

## Required Parameters

- `--app-name`: Name of the Databricks app (e.g., kasal, kasal-dev)
- `--user-name`: Your Databricks user email

## Optional Parameters

- `--workspace-dir`: Custom workspace directory
- `--profile`: Databricks CLI profile
- `--description`: App description
- `--oauth-scopes`: OAuth scopes to configure
- `--config-template`: Config template path
- `--api-url`: Custom API URL
- `--configure-oauth / --no-configure-oauth`: Enable/disable OAuth configuration
- `--include-dataplane`: Include dataplane scope

## OAuth Scopes Configured

The following scopes are configured automatically after deployment:

### SQL Scopes
- `sql`, `sql.alerts`, `sql.alerts-legacy`, `sql.dashboards`
- `sql.data-sources`, `sql.dbsql-permissions`, `sql.queries`
- `sql.queries-legacy`, `sql.query-history`, `sql.statement-execution`
- `sql.warehouses`

### Vector Search Scopes (Critical for memory/index operations)
- `vectorsearch.vector-search-endpoints`
- `vectorsearch.vector-search-indexes`

### Other Scopes
- `serving.serving-endpoints` - Model serving
- `files.files` - File operations
- `dashboards.genie` - Dashboard access

## Security Notes

- Never commit or log the DATABRICKS_TOKEN value
- Tokens should be treated as secrets
- Consider using Databricks CLI authentication as an alternative

## Troubleshooting

If OAuth scope configuration fails during deploy.py, the post-deployment scope configuration step should fix it. If that also fails:
1. Check if you have admin permissions for the app
2. Verify the app name is correct
3. Try manual configuration in Databricks UI
