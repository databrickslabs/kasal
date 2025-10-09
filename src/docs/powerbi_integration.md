# Power BI DAX Integration

## Overview

The Power BI DAX integration enables Kasal to execute DAX queries against Power BI semantic models (datasets) and analyze business intelligence data within AI agent workflows. This integration provides a production-ready, API-driven connector for Power BI analytics.

## Architecture

### Components

1. **Database Model** (`models/powerbi_config.py`)
   - Stores Power BI connection configuration
   - Supports multi-tenant isolation via `group_id`
   - Encrypts sensitive credentials (client secret, username, password)

2. **Repository** (`repositories/powerbi_config_repository.py`)
   - Handles data access for Power BI configurations
   - Manages active configuration per group
   - Provides async CRUD operations

3. **Service Layer** (`services/powerbi_service.py`)
   - Orchestrates DAX query execution
   - Manages authentication with Power BI API
   - Handles token generation and API communication

4. **API Router** (`api/powerbi_router.py`)
   - FastAPI endpoints for Power BI operations
   - Multi-tenant aware with group context
   - Admin-only configuration endpoints

5. **CrewAI Tool** (`engines/crewai/tools/custom/powerbi_dax_tool.py`)
   - AI agent tool for DAX query execution
   - Integrates with CrewAI framework
   - Supports async execution in agent workflows

## Configuration

### Database Setup

1. **Run Migration**:
   ```bash
   cd src/backend
   DATABASE_TYPE=sqlite alembic upgrade head
   ```

2. **Configuration Model Fields**:
   - `tenant_id`: Azure AD Tenant ID
   - `client_id`: Service Principal Application ID
   - `encrypted_client_secret`: Encrypted SPN secret
   - `workspace_id`: Power BI Workspace ID (optional)
   - `semantic_model_id`: Default semantic model/dataset ID (optional)
   - `encrypted_username`: Encrypted username for user/password auth
   - `encrypted_password`: Encrypted password for user/password auth
   - `is_enabled`: Enable/disable Power BI integration
   - `group_id`: Group isolation for multi-tenancy
   - `created_by_email`: Audit trail

### Authentication Methods

The service supports two authentication methods:

#### 1. Device Code Flow (Interactive) - **Recommended for Testing**
Best for testing, development, and personal workspaces:
- **Use Case**: When you don't have a service principal or need to test with your personal Power BI account
- **Requirements**: `tenant_id`, `client_id` (can use Power BI public client: `1950a258-227b-4e31-a9cf-717495945fc2`)
- **How it works**:
  - User is prompted to visit `microsoft.com/devicelogin`
  - Enter provided code to authenticate via browser
  - Supports MFA and conditional access policies
  - Token is cached for subsequent queries
- **Permissions**: Uses the authenticated user's Power BI permissions
- **Note**: Not suitable for automated/unattended workflows

#### 2. Username/Password Flow - **For Automated Workflows**
For production environments with service accounts:
- **Use Case**: Automated workflows, scheduled jobs, production deployments
- **Requirements**: `tenant_id`, `client_id`, `username`, `password` (stored in API Keys)
- **How it works**: Authenticates with username/password programmatically
- **Permissions**: Requires account without MFA
- **Note**: Less secure than service principal, consider migrating to OAuth when possible

### Environment Variables

Set these in your `.env` file:

```bash
# Power BI Configuration
POWERBI_TENANT_ID=your-tenant-id
POWERBI_CLIENT_ID=your-client-id
POWERBI_CLIENT_SECRET=your-client-secret
POWERBI_USERNAME=user@domain.com
POWERBI_PASSWORD=your-password
```

**Note**: Never commit credentials to source control. Use environment variables or the API Keys Service.

## API Endpoints

### POST `/powerbi/config`
**Admin Only** - Set Power BI configuration for the workspace.

**Request Body**:
```json
{
  "tenant_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "client_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "workspace_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "semantic_model_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "enabled": true
}
```

**Response**:
```json
{
  "message": "Power BI configuration saved successfully",
  "config": {
    "tenant_id": "...",
    "client_id": "...",
    "workspace_id": "...",
    "semantic_model_id": "...",
    "is_enabled": true,
    "is_active": true
  }
}
```

### GET `/powerbi/config`
Get current Power BI configuration.

**Response**:
```json
{
  "tenant_id": "...",
  "client_id": "...",
  "workspace_id": "...",
  "semantic_model_id": "...",
  "enabled": true
}
```

### POST `/powerbi/query`
Execute a DAX query against a Power BI semantic model.

**Request Body**:
```json
{
  "dax_query": "EVALUATE SUMMARIZECOLUMNS('Table'[Column], \"Measure\", SUM('Table'[Value]))",
  "semantic_model_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "workspace_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

**Response**:
```json
{
  "status": "success",
  "data": [
    {
      "Column": "Value1",
      "Measure": 12345
    },
    {
      "Column": "Value2",
      "Measure": 67890
    }
  ],
  "row_count": 2,
  "columns": ["Column", "Measure"],
  "execution_time_ms": 245
}
```

### GET `/powerbi/status`
Check Power BI integration status.

**Response**:
```json
{
  "configured": true,
  "enabled": true,
  "workspace_id": "...",
  "semantic_model_id": "...",
  "message": "Power BI is configured and ready"
}
```

## Using the CrewAI Tools

Kasal provides **two Power BI tools** for different use cases:

### 1. PowerBIDAXTool (Direct Execution)

**Best for**: Interactive queries, low latency, simple analysis

**Tool Name**: `PowerBIDAXTool`

**Actions**:
- `query`: Execute a DAX query directly
- `analyze`: Analyze with business questions (future feature)

### 2. PowerBIAnalysisTool (Databricks-Wrapped)

**Best for**: Heavy computation, complex analysis, large datasets

**Tool Name**: `PowerBIAnalysisTool`

**Parameters**:
- `dashboard_id`: Power BI semantic model ID
- `questions`: List of business questions to analyze
- `dax_statement`: Optional pre-generated DAX query
- `databricks_job_id`: Databricks job ID (configured in tool_configs)

**See**: [Tool Comparison Guide](powerbi_tool_comparison.md) for detailed comparison

### Agent Configuration Examples

**Example 1: Using PowerBIDAXTool (Direct)**
```json
{
  "role": "Sales Analyst",
  "goal": "Analyze sales data from Power BI",
  "tools": ["PowerBIDAXTool"],
  "tool_configs": {
    "PowerBIDAXTool": {}
  }
}
```

**Example 2: Using PowerBIAnalysisTool (Databricks)**
```json
{
  "role": "Business Intelligence Analyst",
  "goal": "Perform complex year-over-year growth analysis",
  "tools": ["PowerBIAnalysisTool"],
  "tool_configs": {
    "PowerBIAnalysisTool": {
      "databricks_job_id": 12345
    }
  }
}
```

**Example 3: Using Both Tools**
```json
{
  "role": "Data Analyst",
  "goal": "Analyze sales data with flexible approach",
  "tools": ["PowerBIDAXTool", "PowerBIAnalysisTool"],
  "tool_configs": {
    "PowerBIDAXTool": {},
    "PowerBIAnalysisTool": {
      "databricks_job_id": 12345
    }
  }
}
```

### Task Examples

**Simple Query (PowerBIDAXTool)**
```json
{
  "description": "Execute this DAX query: EVALUATE SUMMARIZECOLUMNS('Sales'[Region], \"Total Sales\", SUM('Sales'[Amount]))",
  "expected_output": "Sales totals by region"
}
```

**Complex Analysis (PowerBIAnalysisTool)**
```json
{
  "description": "Analyze year-over-year growth trends across all product categories",
  "expected_output": "Comprehensive growth analysis with insights and recommendations"
}
```

## DAX Query Examples

### 1. Simple Table Evaluation
```dax
EVALUATE 'Sales'
```

### 2. Summarize Columns
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Year],
    'Product'[Category],
    "Total Sales", SUM('Sales'[Amount]),
    "Total Quantity", SUM('Sales'[Quantity])
)
```

### 3. Top N Analysis
```dax
EVALUATE
TOPN(
    10,
    SUMMARIZECOLUMNS(
        'Product'[Name],
        "Revenue", SUM('Sales'[Amount])
    ),
    [Revenue],
    DESC
)
```

### 4. Filtered Results
```dax
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        'Sales'[Region],
        "Total", SUM('Sales'[Amount])
    ),
    'Date'[Year] = 2024
)
```

## Security Considerations

### Credential Storage

1. **Encrypted Storage**:
   - Client secrets, usernames, and passwords are encrypted in the database
   - Uses the same encryption mechanism as other API keys in Kasal

2. **API Keys Service**:
   - Preferred method for credential management
   - Centralized key storage with encryption
   - Supports key rotation

3. **Environment Variables**:
   - Fallback for development environments
   - Never commit `.env` files to source control

### Multi-Tenant Isolation

- Each group has separate Power BI configuration
- `group_id` ensures data isolation
- Only workspace admins can configure Power BI settings

### Permissions

- **Configuration**: Workspace admin only
- **Query Execution**: All authenticated users in the group
- **Status Check**: All authenticated users in the group

## Troubleshooting

### Authentication Errors

**Error**: "Failed to authenticate with Power BI"

**Solutions**:
1. Verify `tenant_id` and `client_id` are correct
2. Check credentials in environment variables or API Keys Service
3. Ensure Service Principal has Power BI API permissions
4. For user/password auth, verify credentials are correct

### Query Execution Errors

**Error**: "Semantic model ID is required"

**Solution**: Provide `semantic_model_id` in request or configure a default in settings.

**Error**: "Power BI API error (status 400)"

**Solution**:
- Check DAX query syntax
- Verify semantic model ID is correct
- Ensure columns and tables exist in the model

### Configuration Not Found

**Error**: "No active Power BI configuration found"

**Solution**:
1. Configure Power BI via `/powerbi/config` endpoint
2. Ensure `enabled` is set to `true`
3. Verify you're in the correct workspace/group

## Development Workflow

### 1. Setup Development Environment

```bash
# Install dependencies
cd src/backend
pip install -r ../requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your Power BI credentials

# Run migrations
DATABASE_TYPE=sqlite alembic upgrade head
```

### 2. Configure Power BI

Use the API or directly insert configuration:

```python
from src.core.unit_of_work import UnitOfWork
from src.repositories.powerbi_config_repository import PowerBIConfigRepository

async def configure_powerbi():
    async with UnitOfWork() as uow:
        repo = PowerBIConfigRepository(uow._session)
        config = await repo.create_config({
            "tenant_id": "your-tenant-id",
            "client_id": "your-client-id",
            "workspace_id": "your-workspace-id",
            "semantic_model_id": "your-model-id",
            "group_id": "your-group-id",
            "is_enabled": True,
            "is_active": True
        })
        return config
```

### 3. Test DAX Query

```python
from src.services.powerbi_service import PowerBIService
from src.schemas.powerbi_config import DAXQueryRequest

async def test_query(session, group_id):
    service = PowerBIService(session, group_id=group_id)

    request = DAXQueryRequest(
        dax_query="EVALUATE 'Sales'"
    )

    response = await service.execute_dax_query(request)
    print(f"Status: {response.status}")
    print(f"Rows: {response.row_count}")
    print(f"Data: {response.data}")
```

## Production Deployment

### Prerequisites

1. **Azure AD Service Principal**:
   - Created in Azure Portal
   - Granted Power BI Service API permissions
   - Client secret generated and stored securely

2. **Power BI Workspace**:
   - Service Principal added as member or admin
   - Semantic models published and accessible

3. **Database**:
   - PostgreSQL in production (SQLite for dev)
   - Migrations applied
   - Connection pooling configured

### Deployment Steps

1. **Set Environment Variables**:
   ```bash
   export POWERBI_TENANT_ID="your-tenant-id"
   export POWERBI_CLIENT_ID="your-client-id"
   export POWERBI_CLIENT_SECRET="your-secret"
   ```

2. **Run Migrations**:
   ```bash
   alembic upgrade head
   ```

3. **Configure via API**:
   - Use admin account to POST to `/powerbi/config`
   - Store workspace and semantic model IDs

4. **Test Connection**:
   - Check status: GET `/powerbi/status`
   - Execute test query: POST `/powerbi/query`

5. **Monitor**:
   - Check application logs for authentication issues
   - Monitor API response times
   - Track failed query attempts






## Testing Strategy

### Step 1: Local Development Testing

Local testing verifies the integration works correctly in your development environment before deploying to production.

#### 1.1 Start Backend and Frontend

**Backend**:
```bash
cd src/backend
./run.sh sqlite
# Backend starts on http://localhost:8000
# Check logs for: "Application startup complete"
```

**Frontend**:
```bash
cd src/frontend
npm start
# Frontend starts on http://localhost:3000
# Browser opens automatically
```

**Verify Services**:
```bash
# Check backend is running
ps aux | grep uvicorn

# Check frontend is running
ps aux | grep "npm start"
```

#### 1.2 Configure Power BI in the UI

**Navigate to Configuration**:
1. Open http://localhost:3000
2. Go to **Configuration** (gear icon in sidebar)
3. Click **Power BI** tab

**Set API Keys**:
1. Navigate to **API Keys** tab
2. Add the following keys:
   - `POWERBI_USERNAME`: Your Power BI service account email
   - `POWERBI_PASSWORD`: Service account password
   - `POWERBI_CLIENT_SECRET`: Azure AD app client secret (optional)
3. Click **Save**

**Configure Power BI Settings**:
1. Return to **Power BI** tab
2. Toggle **Enable Power BI Integration** to ON
3. Fill in required fields:
   - **Tenant ID**: Your Azure AD tenant GUID
   - **Client ID**: Your Azure AD application (client) ID
4. (Optional) Fill in default settings:
   - **Workspace ID**: Default Power BI workspace GUID
   - **Semantic Model ID**: Default semantic model GUID
5. Click **Save Configuration**
6. Verify status shows: "Power BI is configured and ready"

#### 1.3 Enable Power BI Tools

**Navigate to Tools**:
1. Go to **Tools** section in sidebar
2. Find **PowerBIDAXTool** and **PowerBIAnalysisTool**
3. Review security disclaimers
4. Enable both tools for your workspace

**Verify Tool Registration**:
- Both tools should appear in the available tools list
- Status should show "Enabled"
- Security profile should display risk levels

#### 1.4 Test Backend API Endpoints

**Check Configuration Status**:
```bash
curl -X GET http://localhost:8000/powerbi/status \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"

# Expected response:
# {
#   "configured": true,
#   "enabled": true,
#   "workspace_id": "...",
#   "semantic_model_id": "...",
#   "message": "Power BI is configured and ready"
# }
```

**Get Configuration**:
```bash
curl -X GET http://localhost:8000/powerbi/config \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"

# Expected response:
# {
#   "tenant_id": "...",
#   "client_id": "...",
#   "workspace_id": "...",
#   "semantic_model_id": "...",
#   "enabled": true
# }
```

**Execute Test DAX Query**:
```bash
curl -X POST http://localhost:8000/powerbi/query \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dax_query": "EVALUATE TOPN(5, '\''Sales'\'')",
    "semantic_model_id": "your-model-id",
    "workspace_id": "your-workspace-id"
  }'

# Expected response:
# {
#   "status": "success",
#   "data": [...],
#   "row_count": 5,
#   "columns": ["Column1", "Column2", ...],
#   "execution_time_ms": 245
# }
```

#### 1.5 Verify Database Configuration

**Check SQLite Database**:
```bash
cd src/backend
sqlite3 kasal.db

# Run queries:
.tables                          # Verify 'powerbiconfig' table exists
SELECT * FROM powerbiconfig;     # Check configuration saved
.exit
```

**Verify Fields**:
- `tenant_id` should match your Azure AD tenant
- `client_id` should match your application ID
- `is_enabled` should be `1` (true)
- `is_active` should be `1` (true)
- `group_id` should match your workspace/group

#### 1.6 Test Agent Workflow (Optional)

**Create Test Agent with PowerBIDAXTool**:
1. Navigate to **Agents** section
2. Create new agent:
   - **Role**: "Sales Analyst"
   - **Goal**: "Analyze sales data from Power BI"
   - **Tools**: Enable "PowerBIDAXTool"
3. Create task:
   - **Description**: "Execute DAX query to get top 10 products by revenue"
   - **Expected Output**: "List of products with revenue amounts"
4. Run the workflow
5. Check execution logs for successful DAX query execution

---

### Step 2: Real-World Testing with Databricks App

Production testing validates the integration in a real Databricks environment with actual Power BI data.

#### 2.1 Prerequisites

**Azure AD Application Setup**:
1. Go to Azure Portal → **Azure Active Directory** → **App registrations**
2. Create or select existing application
3. Note the following IDs:
   - **Application (client) ID**
   - **Directory (tenant) ID**
4. Under **Certificates & secrets**, create a **Client Secret** (optional)
5. Under **API permissions**, add:
   - **Power BI Service** → **Dataset.Read.All**
   - **Power BI Service** → **Content.Create** (if needed)
6. Grant admin consent for permissions

**Power BI Workspace Setup**:
1. Go to Power BI Service (app.powerbi.com)
2. Create a test workspace or use existing
3. Note the **Workspace ID** (from URL or workspace settings)
4. Publish a test semantic model with sample data
5. Note the **Semantic Model ID** (Dataset ID)
6. Grant workspace access:
   - Add service principal as workspace member/admin
   - OR add service account user

**Test Data Setup**:
Create a simple Power BI dataset with sample data:
```dax
Sales = DATATABLE(
    "Product", STRING,
    "Region", STRING,
    "Amount", CURRENCY,
    {
        {"Product A", "North", 1000},
        {"Product B", "South", 1500},
        {"Product C", "East", 1200},
        {"Product D", "West", 800},
        {"Product E", "North", 2000}
    }
)
```

#### 2.2 Build and Deploy to Databricks Apps

**Build Frontend Static Assets**:
```bash
cd src
python build.py

# Verify output:
# - Frontend static files copied to src/frontend_static/
# - Build completed successfully
```

**Deploy to Databricks**:
```bash
cd src
python3 deploy.py --app-name kasal-david --user-name david.schwarzenbacher@databricks.com

# Deployment steps:
# 1. Uploads application files to Databricks
# 2. Creates/updates Databricks App
# 3. Configures environment variables
# 4. Starts the application

# Note the App URL from deployment output
```

**Verify Deployment**:
1. Open Databricks workspace
2. Navigate to **Apps** section
3. Find your deployed Kasal app
4. Click to open the app URL
5. Verify app loads correctly

#### 2.3 Configure Power BI in Databricks App

**Set Environment Variables** (if not using API Keys Service):
1. In Databricks workspace, go to your App settings
2. Add environment variables:
   ```
   POWERBI_USERNAME=your-service-account@domain.com
   POWERBI_PASSWORD=your-password
   POWERBI_CLIENT_SECRET=your-client-secret
   ```
3. Restart the app

**Configure via UI**:
1. Open the deployed app
2. Navigate to **Configuration** → **API Keys**
3. Add credentials:
   - `POWERBI_USERNAME`
   - `POWERBI_PASSWORD`
   - `POWERBI_CLIENT_SECRET`
4. Navigate to **Configuration** → **Power BI**
5. Configure settings:
   - **Enable Power BI Integration**: ON
   - **Tenant ID**: Your Azure AD tenant ID
   - **Client ID**: Your application (client) ID
   - **Workspace ID**: Your Power BI workspace ID
   - **Semantic Model ID**: Your test dataset ID
6. Click **Save Configuration**
7. Verify status: "Power BI is configured and ready"

#### 2.4 End-to-End Testing

**Test 1: Direct DAX Query (PowerBIDAXTool)**

1. **Enable Tool**:
   - Navigate to **Tools** → **PowerBIDAXTool**
   - Enable the tool

2. **Create Agent**:
   - **Role**: "Power BI Analyst"
   - **Goal**: "Execute DAX queries to analyze sales data"
   - **Tools**: PowerBIDAXTool
   - **Backstory**: "Expert in DAX and Power BI analysis"

3. **Create Task**:
   - **Description**: "Get the top 5 products by total sales amount"
   - **Expected Output**: "List of products with sales amounts in descending order"
   - **Agent**: Select the Power BI Analyst agent

4. **Run Workflow**:
   - Click **Run**
   - Monitor execution logs
   - Verify DAX query is executed
   - Check results contain expected data

5. **Validate Results**:
   - Results should match your Power BI data
   - Execution time should be reasonable (< 5 seconds for small datasets)
   - No authentication errors

**Test 2: Complex Analysis (PowerBIAnalysisTool)**

1. **Configure Databricks Job**:
   - Create a Databricks job for Power BI analysis
   - Configure job to accept parameters: `dax_queries`, `semantic_model_id`
   - Note the **Job ID**

2. **Enable Tool with Configuration**:
   - Navigate to **Tools** → **PowerBIAnalysisTool**
   - Configure tool settings:
     ```json
     {
       "databricks_job_id": 12345
     }
     ```
   - Enable the tool

3. **Create Agent**:
   - **Role**: "Business Intelligence Analyst"
   - **Goal**: "Perform complex Power BI analysis using Databricks"
   - **Tools**: PowerBIAnalysisTool
   - **Tool Config**:
     ```json
     {
       "PowerBIAnalysisTool": {
         "databricks_job_id": 12345
       }
     }
     ```

4. **Create Task**:
   - **Description**: "Analyze year-over-year growth trends by product category and region"
   - **Expected Output**: "Comprehensive growth analysis with insights"
   - **Agent**: Select the BI Analyst agent

5. **Run Workflow**:
   - Click **Run**
   - Monitor Databricks job execution
   - Verify job receives correct parameters
   - Check job logs for DAX query execution
   - Validate results when job completes

**Test 3: Combined Workflow (Both Tools)**

Create an agent with both tools to handle different scenarios:
```json
{
  "role": "Data Analyst",
  "goal": "Analyze sales data with flexible approach - use direct queries for simple tasks, Databricks jobs for complex analysis",
  "tools": ["PowerBIDAXTool", "PowerBIAnalysisTool"],
  "tool_configs": {
    "PowerBIDAXTool": {},
    "PowerBIAnalysisTool": {
      "databricks_job_id": 12345
    }
  }
}
```

Test with multiple tasks:
- Simple query → Should use PowerBIDAXTool
- Complex analysis → Should use PowerBIAnalysisTool
- Agent should choose appropriate tool based on task complexity

#### 2.5 Troubleshooting Checklist

**Authentication Issues**:
- [ ] Verify tenant_id and client_id are correct
- [ ] Check credentials in API Keys Service
- [ ] Confirm service principal has Power BI API permissions
- [ ] Test authentication with Azure AD:
  ```bash
  # Test token generation
  curl -X POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token \
    -d "client_id={client_id}" \
    -d "scope=https://analysis.windows.net/powerbi/api/.default" \
    -d "username={username}" \
    -d "password={password}" \
    -d "grant_type=password"
  ```

**Query Execution Issues**:
- [ ] Verify semantic_model_id is correct
- [ ] Check workspace_id matches configuration
- [ ] Validate DAX query syntax
- [ ] Ensure tables/columns exist in semantic model
- [ ] Test query directly in Power BI Desktop

**Configuration Issues**:
- [ ] Check database has powerbiconfig table
- [ ] Verify is_enabled = true
- [ ] Confirm is_active = true
- [ ] Check group_id matches your workspace

**Databricks App Issues**:
- [ ] Verify app is running (check Apps section)
- [ ] Check app logs for errors
- [ ] Confirm environment variables are set
- [ ] Test backend API endpoints directly
- [ ] Verify database connection is working

**Tool Registration Issues**:
- [ ] Check tool appears in Tools list
- [ ] Verify tool is enabled
- [ ] Confirm tool_configs are saved correctly
- [ ] Check agent has tool assigned
- [ ] Review CrewAI logs for tool instantiation errors

#### 2.6 Quick Testing Script

Save this as `/tmp/test_powerbi.sh` for rapid testing:

```bash
#!/bin/bash

# Test Power BI Integration
BASE_URL="${1:-http://localhost:8000}"
TOKEN="${2:-your-jwt-token}"

echo "=== Testing Power BI Integration ==="
echo "Base URL: $BASE_URL"
echo ""

# Test 1: Status Check
echo "1. Checking Power BI status..."
curl -s -X GET "$BASE_URL/powerbi/status" \
  -H "Authorization: Bearer $TOKEN" | jq .
echo ""

# Test 2: Get Configuration
echo "2. Getting Power BI configuration..."
curl -s -X GET "$BASE_URL/powerbi/config" \
  -H "Authorization: Bearer $TOKEN" | jq .
echo ""

# Test 3: Execute Simple DAX Query
echo "3. Executing test DAX query..."
curl -s -X POST "$BASE_URL/powerbi/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dax_query": "EVALUATE TOPN(5, '\''Sales'\'')",
    "semantic_model_id": "your-model-id"
  }' | jq .
echo ""

echo "=== Testing Complete ==="
```

**Usage**:
```bash
# Local testing
bash /tmp/test_powerbi.sh http://localhost:8000 your-local-token

# Databricks App testing
bash /tmp/test_powerbi.sh https://your-app-url.databricks.com your-databricks-token
```

---

## Future Enhancements

### Planned Features

1. **DAX Generation from Natural Language**:
   - LLM-powered DAX query generation
   - Business question to DAX translation
   - Schema-aware query construction

2. **Query Caching**:
   - Cache frequently executed queries
   - Configurable TTL
   - Invalidation strategies

3. **Result Streaming**:
   - Stream large result sets
   - Pagination support
   - Progressive loading

4. **Advanced Analytics**:
   - Time series analysis
   - Trend detection
   - Anomaly detection

5. **Visualization Integration**:
   - Generate charts from results
   - Export to various formats
   - Embedded Power BI reports

### Contributing

To contribute to the Power BI integration:

1. Follow the clean architecture pattern
2. Ensure all operations are async
3. Add comprehensive tests
4. Update documentation
5. Submit pull request

## References

- [Power BI REST API Documentation](https://learn.microsoft.com/en-us/rest/api/power-bi/)
- [DAX Query Language Reference](https://learn.microsoft.com/en-us/dax/)
- [Azure Identity Python SDK](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
- [CrewAI Tool Development](https://docs.crewai.com/concepts/tools)

## Support

For issues or questions:
- Check the troubleshooting section above
- Review application logs
- Consult Power BI API documentation
- Create an issue in the project repository
