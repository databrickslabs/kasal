# Power BI Integration Guide

Complete guide for integrating Power BI with Kasal AI agents for advanced business intelligence analysis.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
  - [Development Environment](#1-development-environment-setup)
  - [Azure Service Principal](#2-azure-service-principal-setup)
  - [Databricks Configuration](#3-databricks-configuration)
  - [Kasal Configuration](#4-kasal-configuration)
- [Authentication Methods](#authentication-methods)
- [API Configuration](#api-configuration)
- [PowerBI Analysis Tool](#powerbi-analysis-tool)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Security & Best Practices](#security--best-practices)

---

## Overview

The Power BI integration enables Kasal AI agents to execute complex analysis against Power BI semantic models using Databricks compute resources. This provides a production-ready, API-driven connector for Power BI analytics within AI workflows based on a preconfigured template notebook for tracability.

**Key Features:**
- DAX query execution against Power BI semantic models
- Complex analysis using Databricks job orchestration
- Multiple authentication methods (Service Principal, Device Code Flow)
- Task-level configuration for workspace and semantic model selection
- Multi-tenant isolation with encrypted credential storage

**Use Cases:**
- Year-over-year growth analysis
- Trend detection and forecasting
- Complex financial reporting
- Multi-dimensional business analysis
- Automated business intelligence reporting

---

## Architecture

### System Components

```
┌─────────────────┐
│   Kasal AI      │
│   Agent         │
└────────┬────────┘
         │
         └─ PowerBIAnalysisTool
            └─> Databricks Job
                ├─ Step 1: Extract Power BI metadata
                ├─ Step 2: Generate DAX query from business question
                └─ Step 3: Execute query
                    └─> Power BI REST API
                        └─> Returns: JSON result data
```

### Backend Components

1. **API Keys Service** (`services/api_keys_service.py`)
   - Stores encrypted Power BI credentials
   - Multi-tenant isolation via `group_id`
   - Handles: `POWERBI_CLIENT_SECRET`, `POWERBI_USERNAME`, `POWERBI_PASSWORD`

2. **Databricks Auth Context** (`utils/databricks_auth.py`)
   - Auto-detects `databricks_host` from environment
   - Retrieves `databricks_token` from API Keys or environment

3. **PowerBIAnalysisTool** (`engines/crewai/tools/custom/powerbi_analysis_tool.py`)
   - CrewAI tool for Power BI analysis
   - Wraps Databricks job execution
   - Handles credential retrieval and job parameter passing

4. **Tool Factory** (`engines/crewai/tools/tool_factory.py`)
   - Instantiates tools with task-level configuration
   - Merges base tool config with task-specific overrides

### Frontend Components

1. **PowerBIConfigSelector** (`components/Common/PowerBIConfigSelector.tsx`)
   - Task-level Power BI configuration UI
   - Appears when PowerBIAnalysisTool is selected
   - Validates required API Keys

2. **TaskForm** (`components/Tasks/TaskForm.tsx`)
   - Integrates PowerBIConfigSelector
   - Stores configuration in `tool_configs.PowerBIAnalysisTool`

### Authentication Flow

1. Kasal retrieves credentials from API Keys Service
2. Auto-detects Databricks host from unified auth context
3. Passes credentials to Databricks job as parameters
4. Databricks job authenticates with Azure AD
5. Azure AD issues Power BI access token
6. Access token used to call Power BI REST API

---

## Prerequisites

### Required Accounts & Access

- **Azure Tenant**: Admin access for Service Principal or Service Account (if PBI with RLS enforcement) setup
- **Power BI**: Workspace access and semantic model permissions
- **Databricks Workspace**: Access with token for job creation
- **Operating System**: Linux/macOS (Ubuntu on VDI for production)
- **Key vault connect to Databricks**: Connection of centrally managed secrets as KV variables within Databricks
- **Python**: 3.11+
- **Node.js**: LTS version

### Power BI Requirements

- Power BI workspace with semantic models
- Workspace ID and Semantic Model ID
- Admin permissions to grant Service Principal access

### Azure AD Requirements

- Permission to create App Registrations
- Admin consent capability for API permissions
- Ability to create and manage Client Secrets

---

## Setup Guide

### 1. Development Environment Setup

#### 1.1 Install Python 3.11

```bash
# Add the deadsnakes PPA (Ubuntu)
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update

# Install Python 3.11
sudo apt install python3.11 python3.11-venv python3.11-dev -y

# Verify installation
python3.11 --version
```

#### 1.2 Clone Repository

```bash
# Clone the Kasal repository
git clone https://github.com/databrickslabs/kasal.git
cd kasal

# Checkout the feature branch
git checkout feature/pbi-tool
```

#### 1.3 Create Virtual Environment

```bash
# Create virtual environment with Python 3.11
python3.11 -m venv venv

# Activate the environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip
```

#### 1.4 Install Dependencies

```bash
# Navigate to src directory
cd src

# Install Python dependencies
pip install -r requirements.txt

# Verify installations
pip freeze | grep -E "crewai|litellm|databricks"
```

#### 1.5 Install Node.js (if needed)

```bash
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

---

### 2. Azure Service Principal Setup

To enable non-interactive authentication, create an Azure Service Principal with Power BI read permissions.

#### 2.1 Create Service Principal in Azure Portal

1. **Navigate to Azure Portal**: https://portal.azure.com
2. **Go to Azure Active Directory** → **App registrations**
3. **Click "New registration"**:
   - **Name**: `Kasal-PowerBI-Connector` (or your preferred name)
   - **Supported account types**: Single tenant
   - **Redirect URI**: Leave blank
4. **Note the Application (client) ID** and **Directory (tenant) ID**

Please consider that for some PowerBI reports a service principal might not be enough, but a service account might be needed. This will be especially the case for PowerBIs that enfore RLS within the PowerBI. 

#### 2.2 Create Client Secret

1. In your app registration, go to **Certificates & secrets**
2. Click **New client secret**
3. **Description**: `Kasal PowerBI Tool`
4. **Expires**: Choose expiration period (recommended: 90 days)
5. **Copy the secret value** immediately (you won't be able to see it again)

#### 2.3 Configure API Permissions

**Critical**: The Service Principal needs **Application** permissions, not **Delegated**.

1. Go to **API permissions** in your app registration
2. **Remove any Delegated permissions** if present
3. Click **Add a permission**
4. Select **Power BI Service**
5. Choose **Application permissions** (NOT Delegated)
6. Check **Dataset.Read.All**
7. Click **Add permissions**
8. **Click "Grant admin consent for [Your Organization]"** (requires admin)

**Important**: This step requires **Azure AD Admin** privileges. If you don't have admin rights, use the email template in the Appendix.

#### 2.4 Enable Service Principal in Power BI Admin Portal

1. Go to **Power BI Admin Portal**: https://app.powerbi.com/admin-portal/tenantSettings
2. Navigate to **Developer settings** (or **Tenant settings**)
3. Find **Service principals can use Power BI APIs**
4. **Enable** this setting
5. Add your Service Principal to the allowed list:
   - Option 1: Add specific Service Principal by name
   - Option 2: Add to a security group that's allowed

#### 2.5 Grant Workspace Access

For each Power BI workspace you want to access:

1. Open the Power BI workspace
2. Click **Workspace settings**
3. Go to **Access**
4. Click **Add people or groups**
5. Search for your Service Principal name
6. Assign role: **Member** or **Contributor**

---

### 3. Databricks Configuration

#### 3.1 Set Environment Variables

```bash
# Set Databricks credentials
export DATABRICKS_TOKEN="your-databricks-token"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com/"
```

#### 3.2 Configure Databricks CLI

```bash
# Configure Databricks CLI
databricks configure --host https://your-workspace.cloud.databricks.com --token
```

If the prompt doesn't appear:
```bash
# Unset environment variables and retry
unset DATABRICKS_HOST
unset DATABRICKS_TOKEN
databricks configure --host https://your-workspace.cloud.databricks.com --token
```

#### 3.3 Verify Connection

```bash
# Test workspace access
databricks workspace list /
```

#### 3.4 Create Databricks Job

The PowerBIAnalysisTool requires a Databricks job for executing the analysis pipeline.

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
   - **Notebook Path**: `/Workspace/Shared/powerbi_full_pipeline`
   - **Cluster**: Select or create appropriate cluster

4. **Note the Job ID**:
   - After creating the job, copy the **Job ID** from the URL
   - Example: `365257288725339`
   - This will be used in PowerBIAnalysisTool configuration

#### 3.5 Upload Pipeline Notebook

```bash
# Upload the notebook to Databricks
# Please note that the security features were implemented
# But for the notebook to work you need to be precise with
# pre-requisites (Key-Vault setup) and PBI SVP setting (ask respective admins)
databricks workspace import \
    examples/powerbi_full_pipeline.ipynb \
    /Workspace/Shared/powerbi_full_pipeline \
    --language PYTHON \
    --format JUPYTER
```

Or manually upload via Databricks UI:
1. Go to **Workspace** → **Shared**
2. Click **Create** → **Import**
3. Upload `examples/powerbi_full_pipeline.ipynb`

---

### 4. Kasal Configuration

#### 4.1 Build Frontend

```bash
# From the project root
python src/build.py
```

This creates a `frontend_static` folder with compiled React application.

#### 4.2 Deploy to Databricks Apps

```bash
# Deploy the application
cd src
python deploy.py \
    --app-name kasal \
    --user-name your-email@domain.com
```

**Note**: Replace `--app-name` and `--user-name` with your specific values.

#### 4.3 Configure API Keys

After deploying, configure required API Keys:

1. **Navigate to Configuration** → **API Keys**
2. **Add the following keys**:
   - `POWERBI_CLIENT_SECRET`: Service Principal secret (from section 2.2)
   - `POWERBI_USERNAME`: Power BI service account email (for device code auth)
   - `POWERBI_PASSWORD`: Service account password (for device code auth)
   - `DATABRICKS_API_KEY` or `DATABRICKS_TOKEN`: Databricks access token

**Important**: All values are encrypted at rest and never returned in plain text via API.

#### 4.4 Enable PowerBIAnalysisTool

1. Go to **Tools** section
2. Find **PowerBIAnalysisTool**
3. Review security disclaimers
4. Enable the tool for your workspace

---

## Authentication Methods

The PowerBIAnalysisTool supports two authentication methods:

### Service Principal (Recommended for Production)

**Best for**: Automated workflows, production deployments, unattended execution

**Requirements**:
- `tenant_id`: Azure AD Tenant ID
- `client_id`: Service Principal Application ID
- `POWERBI_CLIENT_SECRET`: Stored in API Keys

**Configuration**:
```json
{
  "tenant_id": "9f37a392-f0ae-4280-9796-f1864a10effc",
  "client_id": "7b597aac-de00-44c9-8e2a-3d2c345c36a9",
  "auth_method": "service_principal"
}
```

**Advantages**:
- Non-interactive, fully automated
- No MFA requirements
- Production-ready
- Supports scheduled workflows

### Device Code Flow (Recommended for Testing)

**Best for**: Development, testing, personal workspaces

**Requirements**:
- `tenant_id`: Azure AD Tenant ID
- `client_id`: Can use Power BI public client `1950a258-227b-4e31-a9cf-717495945fc2`
- `POWERBI_USERNAME`: User email (stored in API Keys)
- `POWERBI_PASSWORD`: User password (stored in API Keys)

**Configuration**:
```json
{
  "tenant_id": "9f37a392-f0ae-4280-9796-f1864a10effc",
  "client_id": "1950a258-227b-4e31-a9cf-717495945fc2",
  "auth_method": "device_code"
}
```

**How it works**:
1. First request prompts: "Visit microsoft.com/devicelogin"
2. Enter provided code in browser
3. Sign in with your credentials
4. Token is cached for subsequent requests (~1 hour)

**Advantages**:
- No Service Principal setup required
- Uses your personal Power BI permissions
- Perfect for development and testing
- Supports MFA

---

## API Configuration

### Task-Level Configuration

Configure Power BI settings at the **task level** for flexibility across different semantic models:

1. **Create or Edit Task**
2. **Select PowerBIAnalysisTool** in tools list
3. **Configure Power BI settings** (fields appear automatically):
   - **Tenant ID**: Azure AD tenant GUID
   - **Client ID**: Service Principal or app client ID
   - **Workspace ID**: Power BI workspace GUID (optional)
   - **Semantic Model ID**: Power BI semantic model/dataset GUID
   - **Auth Method**: `service_principal` or `device_code`
   - **Databricks Job ID**: Databricks job ID for analysis pipeline

**Example Task Configuration**:
```json
{
  "name": "Analyze Sales Data",
  "description": "Analyze Q4 sales trends using Power BI",
  "agent_id": "agent_123",
  "tools": [71],
  "tool_configs": {
    "PowerBIAnalysisTool": {
      "tenant_id": "9f37a392-f0ae-4280-9796-f1864a10effc",
      "client_id": "7b597aac-de00-44c9-8e2a-3d2c345c36a9",
      "semantic_model_id": "a17de62e-8dc0-4a8a-acaa-2a9954de8c75",
      "workspace_id": "bcb084ed-f8c9-422c-b148-29839c0f9227",
      "auth_method": "service_principal",
      "databricks_job_id": 365257288725339
    }
  }
}
```

### Required API Keys Check

The UI automatically checks for required API Keys when PowerBIAnalysisTool is selected:
- `POWERBI_CLIENT_SECRET`
- `POWERBI_USERNAME`
- `POWERBI_PASSWORD`
- `DATABRICKS_API_KEY` (or `DATABRICKS_TOKEN`)

If keys are missing, an error alert is displayed with instructions.

---

## PowerBI Analysis Tool

### Tool Overview

**PowerBIAnalysisTool** (ID: 71) enables complex Power BI analysis via Databricks job orchestration.

**Best for**:
- Heavy computation and large datasets
- Complex multi-query analysis
- Year-over-year comparisons
- Trend detection and forecasting
- Resource-intensive business intelligence tasks

### Tool Parameters

**Input Parameters**:
- `question` (str): Business question to analyze
- `dashboard_id` (str): Semantic model ID (can be provided by LLM or task config)
- `workspace_id` (str): Power BI workspace ID (optional)
- `additional_params` (dict): Optional additional parameters

**Configuration** (from tool_configs):
- `tenant_id`: Azure AD tenant
- `client_id`: Application client ID
- `semantic_model_id`: Default semantic model
- `workspace_id`: Default workspace
- `auth_method`: Authentication method
- `databricks_job_id`: Databricks job ID for pipeline

### Agent Configuration Example

```json
{
  "role": "Business Intelligence Analyst",
  "goal": "Perform complex Power BI analysis using Databricks",
  "backstory": "Expert analyst with deep understanding of business metrics",
  "tools": ["PowerBIAnalysisTool"],
  "llm_config": {
    "model": "databricks-meta-llama-3-1-70b-instruct",
    "temperature": 0.1
  }
}
```

### Task Configuration Example

```json
{
  "name": "Q4 Revenue Analysis",
  "description": "Analyze Q4 2024 revenue trends by product category and region, comparing year-over-year growth",
  "expected_output": "Comprehensive analysis report with insights and recommendations",
  "agent_id": "agent_123",
  "tools": [71],
  "tool_configs": {
    "PowerBIAnalysisTool": {
      "tenant_id": "9f37a392-f0ae-4280-9796-f1864a10effc",
      "client_id": "7b597aac-de00-44c9-8e2a-3d2c345c36a9",
      "semantic_model_id": "a17de62e-8dc0-4a8a-acaa-2a9954de8c75",
      "workspace_id": "bcb084ed-f8c9-422c-b148-29839c0f9227",
      "auth_method": "service_principal",
      "databricks_job_id": 365257288725339
    }
  }
}
```

### How It Works

1. **Agent receives task** with business question
2. **PowerBIAnalysisTool invoked** with question and semantic model ID
3. **Tool retrieves credentials** from API Keys Service
4. **Tool auto-detects** databricks_host from environment
5. **Databricks job triggered** with parameters:
   - `question`: Business question
   - `semantic_model_id`: Dataset to query
   - `workspace_id`: Power BI workspace
   - `tenant_id`, `client_id`: Authentication
   - `client_secret`, `username`, `password`: Credentials
   - `databricks_host`, `databricks_token`: For recursive auth
6. **Job executes pipeline**:
   - Extracts Power BI metadata
   - Generates DAX query from question
   - Executes query against Power BI
   - Returns structured results
7. **Agent receives results** and continues workflow

---

## Testing

### Local Development Testing

#### 1. Start Services

**Backend**:
```bash
cd src/backend
./run.sh sqlite
# Backend starts on http://localhost:8000
```

**Frontend**:
```bash
cd src/frontend
npm start
# Frontend starts on http://localhost:3000
```

#### 2. Configure via UI

1. Open http://localhost:3000
2. Navigate to **Configuration** → **API Keys**
3. Add required keys:
   - `POWERBI_CLIENT_SECRET`
   - `POWERBI_USERNAME`
   - `POWERBI_PASSWORD`
   - `DATABRICKS_API_KEY`
4. Navigate to **Tools** → Enable **PowerBIAnalysisTool**

#### 3. Create Test Agent and Task

**Agent**:
```json
{
  "role": "Sales Analyst",
  "goal": "Analyze Power BI sales data",
  "tools": ["PowerBIAnalysisTool"]
}
```

**Task**:
```json
{
  "description": "What is the total revenue for Q4 2024?",
  "expected_output": "Revenue figure with analysis",
  "tool_configs": {
    "PowerBIAnalysisTool": {
      "tenant_id": "your-tenant-id",
      "client_id": "your-client-id",
      "semantic_model_id": "your-model-id",
      "workspace_id": "your-workspace-id",
      "auth_method": "service_principal",
      "databricks_job_id": 365257288725339
    }
  }
}
```

#### 4. Run Workflow

1. Click **Run Crew**
2. Monitor execution in **Runs** tab
3. Check Databricks **Workflows** for job execution
4. Verify results in execution logs

### Production Testing (Databricks App)

#### 1. Deploy to Databricks

```bash
cd src
python deploy.py --app-name kasal-prod --user-name your-email@domain.com
```

#### 2. Configure in Deployed App

1. Open deployed app URL
2. Navigate to **Configuration** → **API Keys**
3. Add production credentials
4. Enable **PowerBIAnalysisTool**

#### 3. Create Production Workflow

Create agent and task using production semantic model IDs and workspace IDs.

#### 4. End-to-End Test

1. Run crew execution
2. Monitor Databricks job logs
3. Verify Power BI API calls in Azure AD audit logs
4. Validate results accuracy

### Sample Test Queries

**Simple aggregation**:
```json
{
  "question": "What is the total revenue by region?"
}
```

**Year-over-year analysis**:
```json
{
  "question": "Compare Q4 2024 revenue to Q4 2023 by product category"
}
```

**Trend analysis**:
```json
{
  "question": "Show monthly sales trends for the last 12 months"
}
```

---

## Troubleshooting

### Authentication Issues

**Error**: "Provided OAuth token does not have required scopes"

**Causes**:
- Missing OAuth scopes in Databricks App configuration
- Service Principal lacks Power BI API permissions

**Solutions**:
1. Verify Service Principal has **Application** (not Delegated) permissions
2. Ensure admin consent was granted in Azure AD
3. Check Service Principal is enabled in Power BI Admin Portal
4. For Databricks Apps, configure OAuth scopes: `sql`, `all-apis`

---

**Error**: "Authentication failed: 403 Forbidden"

**Causes**:
- Service Principal doesn't have workspace access
- Incorrect workspace ID

**Solutions**:
1. Add Service Principal to Power BI workspace with Member/Contributor role
2. Verify workspace_id matches the actual workspace GUID
3. Check Power BI audit logs for access denied events

---

### Configuration Issues

**Error**: "tenant_id showing as 'your_tenant_id'"

**Cause**: LLM-provided placeholder values taking precedence over task config

**Solution**: Verify task configuration priority in tool_factory.py - task config should override LLM values

---

**Error**: "semantic_model_id truncated or incorrect"

**Cause**: dashboard_id from kwargs overriding task config value

**Solution**: Check powerbi_analysis_tool.py lines 314-316 for proper priority handling

---

**Error**: "Missing databricks_host or databricks_token in job parameters"

**Cause**: Credentials not being passed to job parameters

**Solution**: Verify powerbi_analysis_tool.py lines 411-418 add credentials to job_params

---

### Job Execution Issues

**Error**: "Databricks job times out"

**Causes**:
- Large dataset
- Complex DAX query
- Insufficient cluster resources

**Solutions**:
1. Increase job timeout in tool configuration
2. Optimize DAX query for performance
3. Use more powerful cluster for the job
4. Consider breaking analysis into smaller queries

---

**Error**: "Dataset.Read.All permission not found"

**Cause**: Using Delegated permission instead of Application permission

**Solution**:
1. Go to Azure AD → App registrations → API permissions
2. Remove Delegated permissions
3. Add **Application** permission: Dataset.Read.All
4. Grant admin consent

---

**Error**: "Client secret expired"

**Cause**: Azure client secrets expire after set period

**Solution**:
1. Create new client secret in Azure Portal
2. Update `POWERBI_CLIENT_SECRET` in API Keys
3. Rotate secrets regularly (recommended: every 90 days)

---

## Security & Best Practices

### Credential Management

1. **Use API Keys Service**:
   - All credentials stored encrypted at rest
   - Multi-tenant isolation via group_id
   - Never commit credentials to source control

2. **Rotate Credentials Regularly**:
   - Rotate Service Principal secrets every 90 days
   - Use Azure Key Vault for production deployments
   - Monitor credential usage in audit logs

3. **Principle of Least Privilege**:
   - Only grant workspace access where needed
   - Use Power BI RLS (Row-Level Security) for data filtering
   - Limit Service Principal to read-only permissions

### Production Secret Management with Key Vaults

For production deployments, **never pass credentials directly as job parameters**. Instead, use key vault references:

#### Architecture Pattern

```
┌──────────────┐
│   Kasal App  │
└──────┬───────┘
       │ Pass secret names only
       ▼
┌──────────────────┐
│ Databricks Job   │
│  Parameters:     │
│  {               │
│    "client_secret_key": "powerbi-client-secret"  ← Secret name
│    "username_key": "powerbi-username"            ← Secret name
│  }               │
└──────┬───────────┘
       │ Retrieve actual values
       ▼
┌──────────────────────────┐
│   Key Vault Storage      │
│  (Azure Key Vault,       │
│   Databricks Secrets,    │
│   AWS Secrets Manager)   │
└──────────────────────────┘
```

#### Option 1: Azure Key Vault (Recommended for Azure)

**Setup Azure Key Vault:**

1. **Create Key Vault** in Azure Portal
2. **Add Secrets**:
   - `powerbi-client-secret`: Service Principal secret
   - `powerbi-username`: Service account username
   - `powerbi-password`: Service account password
   - `databricks-token`: Databricks PAT

3. **Grant Access** to Databricks workspace:
   - Use Managed Identity or Service Principal
   - Assign "Key Vault Secrets User" role

**Configure Databricks to Access Azure Key Vault:**

```bash
# Create secret scope backed by Azure Key Vault
databricks secrets create-scope --scope azure-key-vault \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.KeyVault/vaults/{vault-name} \
  --dns-name https://{vault-name}.vault.azure.net/
```

**Notebook Code (Secure Approach):**

```python
import os

# Retrieve secrets from Databricks secret scope (backed by Azure Key Vault)
client_secret = dbutils.secrets.get(scope="azure-key-vault", key="powerbi-client-secret")
username = dbutils.secrets.get(scope="azure-key-vault", key="powerbi-username")
password = dbutils.secrets.get(scope="azure-key-vault", key="powerbi-password")
databricks_token = dbutils.secrets.get(scope="azure-key-vault", key="databricks-token")

# Use credentials for authentication
powerbi_config = {
    "tenant_id": dbutils.widgets.get("tenant_id"),
    "client_id": dbutils.widgets.get("client_id"),
    "client_secret": client_secret,  # Retrieved from Key Vault
    "username": username,             # Retrieved from Key Vault
    "password": password              # Retrieved from Key Vault
}
```

**Job Parameters (No Sensitive Data):**

```json
{
  "question": "Analyze Q4 revenue",
  "semantic_model_id": "a17de62e-8dc0-4a8a-acaa-2a9954de8c75",
  "workspace_id": "bcb084ed-f8c9-422c-b148-29839c0f9227",
  "tenant_id": "9f37a392-f0ae-4280-9796-f1864a10effc",
  "client_id": "7b597aac-de00-44c9-8e2a-3d2c345c36a9"
}
```

**Set environment variables** in Databricks job cluster configuration:

```json
{
  "spark_env_vars": {
    "POWERBI_CLIENT_SECRET": "{{secrets/powerbi-secrets/client-secret}}",
    "POWERBI_USERNAME": "{{secrets/powerbi-secrets/username}}",
    "POWERBI_PASSWORD": "{{secrets/powerbi-secrets/password}}"
  }
}
```

**Note**: No secrets in job parameters - just their names! All retrieved from Key Vault.

---

#### Option 2: Environment Variables (Development Only)

**For local development**, use environment variables:

```python
import os

# Retrieve from environment
client_secret = os.getenv("POWERBI_CLIENT_SECRET")
username = os.getenv("POWERBI_USERNAME")
password = os.getenv("POWERBI_PASSWORD")
databricks_token = os.getenv("DATABRICKS_TOKEN")
```

**Set environment variables** in Databricks job cluster configuration:

```json
{
  "spark_env_vars": {
    "POWERBI_CLIENT_SECRET": "{{secrets/powerbi-secrets/client-secret}}",
    "POWERBI_USERNAME": "{{secrets/powerbi-secrets/username}}",
    "POWERBI_PASSWORD": "{{secrets/powerbi-secrets/password}}"
  }
}
```
