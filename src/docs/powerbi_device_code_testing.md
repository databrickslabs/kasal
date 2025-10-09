# Power BI Device Code Flow Testing Guide

## Overview

This guide explains how to test the Power BI integration using **Device Code Flow** authentication, which allows you to use your personal Power BI account without requiring a premium account or service principal.

## Your Test Environment

**Workspace Information:**
- **Workspace ID**: `bcb084ed-f8c9-422c-b148-29839c0f9227`
- **Semantic Model ID**: `a17de62e-8dc0-4a8a-acaa-2a9954de8c75`
- **Dataset Name**: `test_pbi`
- **Table**: `TestData`
- **Columns**: `fiscper`, `country`, `product`, `nsr`, `cogs`, `net_income`

**Authentication Details:**
- **Tenant ID**: `9f37a392-f0ae-4280-9796-f1864a10effc`
- **Client ID**: `1950a258-227b-4e31-a9cf-717495945fc2` (Power BI public client - no secret needed)
- **Auth Method**: Device Code Flow (interactive browser authentication)

## Testing Approaches

### Option 1: Test with Standalone Script (Quickest)

Run the test script directly:

```bash
cd /Users/david.schwarzenbacher/workspace/kasal/src/backend
python3 /tmp/test_powerbi_device_code.py
```

**Expected Flow:**
1. Script starts and displays configuration
2. You'll see a message like:
   ```
   To sign in, use a web browser to open the page https://microsoft.com/devicelogin
   and enter the code ABC12DEF to authenticate.
   ```
3. Open browser, go to `microsoft.com/devicelogin`
4. Enter the code shown
5. Sign in with your Databricks/Microsoft account
6. Script continues and executes the DAX query
7. Results are displayed

### Option 2: Test via Backend API

#### Step 1: Configure Power BI in the UI

1. Start backend: `cd src/backend && ./run.sh sqlite`
2. Start frontend: `cd src/frontend && npm start`
3. Open http://localhost:3000
4. Navigate to **Configuration** → **Power BI**
5. Configure:
   - **Enable Power BI Integration**: ON
   - **Tenant ID**: `9f37a392-f0ae-4280-9796-f1864a10effc`
   - **Client ID**: `1950a258-227b-4e31-a9cf-717495945fc2`
   - **Authentication Method**: Select **Device Code Flow (Interactive)**
   - **Workspace ID**: `bcb084ed-f8c9-422c-b148-29839c0f9227`
   - **Semantic Model ID**: `a17de62e-8dc0-4a8a-acaa-2a9954de8c75`
6. Click **Save Configuration**

#### Step 2: Test Query via API

Use curl or Postman to test:

```bash
curl -X POST http://localhost:8000/powerbi/query \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dax_query": "EVALUATE TOPN(10, TestData)"
  }'
```

**Note**: When using device code flow via API:
- The authentication prompt will appear in the backend logs
- You'll need access to the backend console to see the device code
- This is why device code flow is recommended for development/testing, not production

### Option 3: Test via Agent Workflow

#### Step 1: Enable PowerBIDAXTool

1. Go to **Tools** section
2. Find **PowerBIDAXTool**
3. Enable the tool

#### Step 2: Create Test Agent

```json
{
  "role": "Power BI Analyst",
  "goal": "Execute DAX queries to analyze test data",
  "tools": ["PowerBIDAXTool"],
  "backstory": "Expert in Power BI DAX analysis"
}
```

#### Step 3: Create Test Task

```json
{
  "description": "Get the top 10 rows from TestData and summarize the results",
  "expected_output": "Table with fiscper, country, product, and financial metrics"
}
```

#### Step 4: Run Workflow

- Click **Run**
- Monitor backend logs for device code prompt
- Authenticate via browser
- Check execution results

## Sample DAX Queries for Testing

### Query 1: Simple Table Scan
```dax
EVALUATE
TOPN(10, TestData)
```

### Query 2: Aggregation by Country
```dax
EVALUATE
SUMMARIZECOLUMNS(
    TestData[country],
    "Total NSR", SUM(TestData[nsr]),
    "Total COGS", SUM(TestData[cogs]),
    "Total Net Income", SUM(TestData[net_income])
)
```

### Query 3: Product Performance
```dax
EVALUATE
TOPN(
    5,
    SUMMARIZECOLUMNS(
        TestData[product],
        "Revenue", SUM(TestData[nsr]),
        "Profit", SUM(TestData[net_income])
    ),
    [Revenue],
    DESC
)
```

### Query 4: Time Period Analysis
```dax
EVALUATE
SUMMARIZECOLUMNS(
    TestData[fiscper],
    TestData[country],
    "Period Revenue", SUM(TestData[nsr]),
    "Period Profit", SUM(TestData[net_income])
)
```

## Troubleshooting

### Issue: "Device code authentication failed"

**Solutions:**
- Ensure you have access to the Power BI workspace
- Check that your account has Power BI license
- Verify tenant ID is correct
- Try authenticating in an incognito browser window

### Issue: "Semantic model ID is required"

**Solutions:**
- Provide semantic_model_id in the query request
- OR configure default semantic_model_id in Power BI Configuration

### Issue: "DAX query syntax error"

**Solutions:**
- Verify table name is exactly `TestData` (case-sensitive)
- Check column names: `fiscper`, `country`, `product`, `nsr`, `cogs`, `net_income`
- Test query in Power BI Desktop first

### Issue: "Token expired"

**Solution:**
- Device code tokens are cached but expire
- Re-run query to trigger new authentication flow
- Token is automatically refreshed

## Deployment Considerations

### For Testing (Current Setup)
- ✅ Use Device Code Flow
- ✅ No API keys needed
- ✅ Use your personal credentials
- ✅ Perfect for development

### For Production (Future)
- Consider upgrading to Power BI Premium
- Set up Service Principal with proper permissions
- Switch to Username/Password flow with service account
- Store credentials in API Keys service
- Update `auth_method` to `username_password`

## Next Steps

1. **Test locally** with the standalone script (`test_powerbi_device_code.py`)
2. **Verify results** match what you see in Power BI Desktop
3. **Test via UI** using the Configuration → Power BI interface
4. **Create test agent** to execute queries through workflows
5. **Deploy to Databricks App** (device code flow will work there too!)

## Important Notes

### Device Code Flow Behavior

- **First Request**: Prompts for device code authentication
- **Subsequent Requests**: Uses cached token (valid for ~1 hour)
- **Token Refresh**: Automatically refreshed when expired
- **Databricks Apps**: Device code prompt appears in app logs
- **Parallel Requests**: Share the same authenticated session

### Security Considerations

- Device code flow uses **your personal credentials**
- Tokens are **cached in memory** only (not persisted to disk)
- **Workspace permissions** apply - you can only query data you have access to
- For production, **migrate to service principal** authentication

### Performance

- **First Query**: Slower (includes authentication ~5-10 seconds)
- **Subsequent Queries**: Fast (<2 seconds for simple queries)
- **Token Caching**: Reduces authentication overhead
- **Query Complexity**: Impacts execution time (aggregations, large datasets)

## Testing Checklist

- [ ] Standalone script test passes
- [ ] Configuration saves successfully in UI
- [ ] API endpoint returns data
- [ ] Tool appears in Tools list
- [ ] Agent can execute queries
- [ ] Results match Power BI Desktop
- [ ] Error handling works properly
- [ ] Token refresh works automatically

## Support

For issues or questions:
- Check backend logs for authentication prompts
- Verify Power BI workspace access
- Test DAX queries in Power BI Desktop first
- Review `/docs/powerbi_integration.md` for full documentation
