# Power BI Analysis Tool - Setup Guide

## Overview

The Power BI Analysis Tool enables AI agents in Kasal to query and analyze Power BI semantic models using DAX queries. This tool supports both direct query execution and complex analysis via Databricks job orchestration.

**Key Features:**
- Direct DAX query execution against Power BI semantic models
- Complex analysis using Databricks compute resources
- Service Principal authentication for automated, non-interactive access
- Metadata extraction and DAX generation pipeline

---

## Prerequisites

- **Operating System**: Ubuntu (on VDI)
- **Python**: 3.11+
- **Node.js**: LTS version
- **Databricks Workspace**: Access with token
- **Azure Tenant**: Admin access for Service Principal setup
- **Power BI**: Admin access for Service Principal permissions

---

## 1. Development Environment Setup

### 1.1 Install Python 3.11

```bash
# Add the deadsnakes PPA
sudo add-apt-repository ppa:deadsnakes/ppa -y

# Update package list
sudo apt update

# Install Python 3.11
sudo apt install python3.11 python3.11-venv python3.11-dev -y

# Verify installation
python3.11 --version
```

### 1.2 Clone Repository

```bash
# Clone the Kasal repository
git clone https://github.com/databrickslabs/kasal.git
cd kasal

# Checkout the feature branch
git checkout feature/pbi-tool
```

### 1.3 Create Virtual Environment

```bash
# Create virtual environment with Python 3.11
python3.11 -m venv myenv

# Activate the environment
source myenv/bin/activate

# Upgrade pip (recommended)
pip install --upgrade pip
```

### 1.4 Install Dependencies

```bash
# Navigate to src directory
cd src

# Install Python dependencies
pip install -r requirements.txt

# Verify installations
pip freeze
```

---

## 2. Databricks Configuration

### 2.1 Set Environment Variables

Set your Databricks credentials for the target workspace:

```bash
# Example: PROD-005 workspace
export DATABRICKS_TOKEN="your-databricks-token"
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net/"
```

### 2.2 Configure Databricks SDK

```bash
# Configure Databricks CLI for deployment
databricks configure --host https://adb-xxx.azuredatabricks.net --token
```

This will prompt you for your token. **If the prompt doesn't appear**, run:

```bash
# Unset environment variables and retry
unset DATABRICKS_HOST
unset DATABRICKS_TOKEN
databricks configure --host https://adb-xxx.14.azuredatabricks.net --token
```

### 2.3 Verify Databricks Connection

```bash
# Test workspace access
databricks workspace list /
```

You should see a list of workspace folders if configured correctly.

---

## 3. Frontend Build & Deployment

### 3.1 Install Node.js (if not available)

```bash
# Clean npm cache
npm cache clean --force

# Install Node Version Manager (nvm)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash

# Load nvm
source ~/.bashrc

# Install Node.js LTS
nvm install --lts
nvm use --lts

# Verify installations
node --version
npm --version
```

### 3.2 Build Frontend

```bash
# From the project root, build the frontend
python3 src/build.py
```

This creates a `frontend_static` folder with the compiled React application.

### 3.3 Deploy to Databricks Apps

```bash
# Deploy the application (from src directory)
cd src
python3 deploy.py \
    --app-name kasal \
    --user-name <your-mailaddress@test.com>
```

**Note**: Replace `--app-name` and `--user-name` with your specific values.

---

## 4. Databricks Job Setup

The Power BI Analysis Tool requires a Databricks job to execute the analysis pipeline.

### 4.1 Create Databricks Job

1. **Navigate to Databricks Workflows**:
   - Go to your Databricks workspace
   - Click **Workflows** in the left sidebar

2. **Create New Job**:
   - Click **Create Job**
   - **Job Name**: `pbi_e2e_pipeline`

3. **Add Task**:
   - Click **Add Task**
   - **Task Name**: `pbi_e2e_pipeline`
   - **Type**: Notebook
   - **Notebook Path**: `/Workspace/path/to/powerbi_full_pipeline` (upload the notebook first)
   - **Cluster**: Select or create an appropriate cluster

4. **Configure Notebook Parameters** (optional):
   - The notebook accepts parameters like:
     - `question`: Business question to analyze
     - `semantic_model_id`: Power BI semantic model ID
     - `workspace_id`: Power BI workspace ID
     - `auth_method`: Authentication method (e.g., `service_principal`)
     - `tenant_id`, `client_id`, `client_secret`: Service Principal credentials

### 4.2 Upload the Pipeline Notebook

```bash
# From your local machine, upload the notebook to Databricks
databricks workspace import \
    [your_path]/kasal/examples/powerbi_full_pipeline.ipynb \
    /Workspace/Shared/powerbi_full_pipeline \
    --language PYTHON \
    --format JUPYTER
```

Or manually upload via the Databricks UI:
1. Go to **Workspace** → **Shared**
2. Click **Create** → **Import**
3. Upload `examples/powerbi_full_pipeline.ipynb`

### 4.3 Note the Job ID

After creating the job:
1. Open the job in Databricks Workflows
2. Copy the **Job ID** from the URL (e.g., `365257288725339`)
3. This Job ID will be used in the PowerBI Analysis Tool configuration

---

## 5. Azure Service Principal Setup for Power BI

To enable **non-interactive authentication** (without device code flow), you need an Azure Service Principal with Power BI read permissions.

### 5.1 Create Service Principal in Azure Portal

1. **Navigate to Azure Portal**: https://portal.azure.com
2. **Go to Azure Active Directory** → **App registrations**
3. **Click "New registration"**:
   - **Name**: `MetricsJet-PowerBI-Connector` (or your preferred name)
   - **Supported account types**: Single tenant
   - **Redirect URI**: Leave blank
4. **Note the Application (client) ID** and **Directory (tenant) ID**

### 5.2 Create Client Secret

1. In your app registration, go to **Certificates & secrets**
2. Click **New client secret**
3. **Description**: `Kasal PowerBI Tool`
4. **Expires**: Choose expiration period
5. **Copy the secret value** (you won't be able to see it again)

### 5.3 Configure API Permissions

**Critical Step**: The Service Principal needs **Application** permissions, not **Delegated**.

1. Go to **API permissions** in your app registration
2. **Remove any Delegated permissions** if present
3. Click **Add a permission**
4. Select **Power BI Service**
5. Choose **Application permissions** (NOT Delegated)
6. Check **Dataset.Read.All**
7. Click **Add permissions**
8. **Click "Grant admin consent for [Your Organization]"** (requires admin)

**Important**: This step requires **Azure AD Admin** privileges. If you don't have admin rights, contact your Azure administrator.

### 5.4 Enable Service Principal in Power BI Admin Portal

1. Go to **Power BI Admin Portal**: https://app.powerbi.com/admin-portal/tenantSettings
2. Navigate to **Developer settings** (or **Tenant settings**)
3. Find **Service principals can use Power BI APIs**
4. **Enable** this setting
5. Add your Service Principal to the allowed list:
   - Option 1: Add specific Service Principal by name
   - Option 2: Add to a security group that's allowed

### 5.5 Grant Workspace Access

For each Power BI workspace you want to access:

1. Open the Power BI workspace
2. Click **Workspace settings**
3. Go to **Access**
4. Click **Add people or groups**
5. Search for your Service Principal name
6. Assign role: **Member** or **Contributor**

---

## 6. Configure Kasal for Power BI

### 6.1 Add Power BI Configuration in Kasal

After deploying the Kasal app:

1. **Navigate to Configuration** → **Power BI Settings**
2. **Add Power BI Workspace**:
   - **Workspace Name**: Your workspace name
   - **Workspace ID**: Power BI workspace GUID
   - **Authentication Method**: `Service Principal`
   - **Tenant ID**: Azure AD tenant ID
   - **Client ID**: Service Principal application ID
   - **Client Secret**: Service Principal secret (created in step 5.2)

### 6.2 Configure PowerBI Analysis Tool

1. Go to **Configuration** → **Tools**
2. Find **PowerBIAnalysisTool**
3. Configure:
   - **databricks_job_id**: The Job ID from section 4.3
   - **result_as_answer**: `false` (or `true` if you want raw results)

### 6.3 Test Configuration

Use the following test parameters:

```json
{
  "dashboard_id": "your-semantic-model-id",
  "questions": ["What is the total revenue?"],
  "workspace_id": "your-workspace-id",
  "additional_params": {
    "auth_method": "service_principal",
    "tenant_id": "your-tenant-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "task_key": "pbi_e2e_pipeline"
  }
}
```

---

## 7. Testing the Setup

### 7.1 Import the Reference Crew Template

A pre-configured reference crew is available in `examples/crew_pbi_tool_template.json` that demonstrates PowerBI Analysis Tool usage.

**Step 1: Customize the Template**

Download and edit `examples/crew_pbi_tool_template.json` to replace placeholders:

```json
{
  "description": "Execute PowerBI analysis job ID {YOUR_JOB_ID} ONE TIME with the following parameters:\n\nRequired Parameters:\n- question: {question}\n- workspace_id: '{YOUR_POWERBI_WORKSPACE_ID}'\n- semantic_model_id: '{YOUR_SEMANTIC_MODEL_ID}'\n- auth_method: 'service_principal'\n- tenant_id: '{YOUR_AZURE_TENANT_ID}'\n- client_id: '{YOUR_SERVICE_PRINCIPAL_CLIENT_ID}'\n- client_secret: '{YOUR_SERVICE_PRINCIPAL_SECRET}'\n- databricks_host: '{YOUR_DATABRICKS_WORKSPACE_URL}'\n- databricks_token: '{YOUR_DATABRICKS_TOKEN}'\n..."
}
```

**Replace these placeholders:**
- `{YOUR_JOB_ID}` → Your Databricks job ID (e.g., `365257288725339`)
- `{YOUR_POWERBI_WORKSPACE_ID}` → Power BI workspace GUID
- `{YOUR_SEMANTIC_MODEL_ID}` → Power BI semantic model GUID
- `{YOUR_AZURE_TENANT_ID}` → Azure AD tenant ID
- `{YOUR_SERVICE_PRINCIPAL_CLIENT_ID}` → Service Principal app ID
- `{YOUR_SERVICE_PRINCIPAL_SECRET}` → Service Principal secret
- `{YOUR_DATABRICKS_WORKSPACE_URL}` → Databricks workspace URL (e.g., `https://adb-xxx.azuredatabricks.net/`)
- `{YOUR_DATABRICKS_TOKEN}` → Databricks personal access token

**Step 2: Import into Kasal**

1. Open Kasal UI → **Crews** page
2. Click **Import Crew**
3. Upload your customized `crew_pbi_tool_template.json`
4. The crew will appear with:
   - **Agent**: "PowerBI Job Orchestrator"
   - **Task**: "Run PowerBI Analysis"
   - **Tool**: PowerBIAnalysisTool (ID: 72)

**Step 3: Run the Crew**

1. Open the imported crew
2. Click **Run Crew**
3. When prompted for `{question}`, enter your business question:
   - Example: "What is the total revenue for Q4 2024?"
4. Monitor execution in the **Runs** tab

### 7.2 Test via Manual Crew Creation

Alternatively, create a crew manually:

```json
{
  "name": "PowerBI Analyst",
  "agents": [
    {
      "role": "Business Analyst",
      "goal": "Analyze Power BI data",
      "tools": ["PowerBIAnalysisTool"]
    }
  ],
  "tasks": [
    {
      "description": "Analyze total revenue from Power BI",
      "agent": "Business Analyst"
    }
  ]
}
```

**Run the Crew** and monitor execution

### 7.3 Verify Databricks Job Execution

1. Go to **Workflows** in Databricks
2. Find your `pbi_e2e_pipeline` job
3. Check the **Runs** tab for execution history
4. Review logs for any errors

### 7.4 Check Power BI Audit Logs

1. Go to **Power BI Admin Portal**
2. Navigate to **Audit logs**
3. Verify that your Service Principal is making successful API calls

---

## 8. Troubleshooting

### Issue: "Provided OAuth token does not have required scopes"

**Cause**: Missing OAuth scopes in `app.yaml` or Service Principal permissions

**Solution**:
- Verify Service Principal has **Application** (not Delegated) permissions
- Ensure admin consent was granted in Azure AD
- Check that Service Principal is enabled in Power BI Admin Portal

### Issue: "Authentication failed: 403 Forbidden"

**Cause**: Service Principal doesn't have workspace access

**Solution**:
- Add Service Principal to Power BI workspace with Member/Contributor role
- Verify the workspace ID is correct

### Issue: Databricks Job Times Out

**Cause**: Large dataset or complex DAX query

**Solution**:
- Increase job timeout in PowerBI Analysis Tool configuration
- Optimize DAX query
- Use a more powerful cluster for the job

### Issue: "Dataset.Read.All permission not found"

**Cause**: Using Delegated permission instead of Application permission

**Solution**:
- Go to Azure AD → App registrations → API permissions
- Remove Delegated permissions
- Add **Application** permission: Dataset.Read.All
- Grant admin consent

### Issue: Client Secret Expired

**Cause**: Azure client secrets expire after a set period

**Solution**:
- Create a new client secret in Azure Portal
- Update the secret in Kasal's Power BI configuration

---

## 9. Architecture Overview

```
┌─────────────────┐
│   Kasal AI      │
│   Agent         │
└────────┬────────┘
         │
         ├─ PowerBIDAXTool (Direct query)
         │  └─> Power BI REST API
         │
         └─ PowerBIAnalysisTool (Complex analysis)
            └─> Databricks Job (pbi_e2e_pipeline)
                ├─ Step 1: Extract metadata
                ├─ Step 2: Generate DAX query
                └─ Step 3: Execute query
                    └─> Power BI REST API
                        └─> Returns: JSON result data
```

**Authentication Flow:**
1. Kasal retrieves Service Principal credentials
2. Credentials passed to Databricks job
3. Databricks job authenticates with Azure AD
4. Azure AD issues access token
5. Access token used to call Power BI REST API

---

## 10. Security Best Practices

1. **Rotate Credentials Regularly**:
   - Rotate Service Principal secrets every 90 days
   - Use Azure Key Vault for production deployments

2. **Principle of Least Privilege**:
   - Only grant workspace access where needed
   - Use Power BI RLS (Row-Level Security) for data filtering

3. **Monitor Usage**:
   - Review Power BI audit logs regularly
   - Set up alerts for unusual API activity

4. **Secure Storage**:
   - Never commit credentials to git
   - Use environment variables or secret management systems

---

## 11. Support & Resources

- **Kasal Documentation**: `/docs/powerbi_integration.md`
- **Power BI REST API**: https://learn.microsoft.com/en-us/rest/api/power-bi/
- **Databricks Jobs API**: https://docs.databricks.com/dev-tools/api/latest/jobs.html
- **Azure AD App Registration**: https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps

---

## Appendix: Example Service Principal Setup Email Template

Use this template when requesting Azure admin assistance:

```
Subject: Azure AD Admin Consent Required for Power BI Service Principal

Hi [Admin Name],

I need admin consent for a Service Principal to enable automated Power BI data access for our Kasal AI platform.

**Service Principal Details:**
- Name: MetricsJet-PowerBI-Connector
- App ID: 7b597aac-de00-44c9-8e2a-3d2c345c36a9
- Requested Permission: Power BI Service → Dataset.Read.All (Application)

**Steps Required:**
1. Go to Azure Portal → Azure AD → App registrations → MetricsJet-PowerBI-Connector
2. Go to "API permissions"
3. Remove any Delegated Dataset.Read.All permission
4. Add Application permission: Power BI Service → Dataset.Read.All (Application, not Delegated)
5. Click "Grant admin consent for [Organization]"

**Additionally:**
- Enable Service Principal in Power BI Admin Portal under "Developer settings"
- Allow service principals to use Power BI APIs

**Test Plan:**
After setup, I will test by running the Kasal app: [Your App URL]

Let me know if you have any questions!

Best regards,
[Your Name]
```

---

**Document Version**: 1.0
**Last Updated**: 2025-01-26
**Maintained By**: Kasal Development Team
