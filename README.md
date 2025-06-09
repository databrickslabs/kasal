# Kasal

A modern, full-stack application platform for building and managing AI agent-based workflows.

## Overview

Kasal combines a Python FastAPI backend with a React frontend to create a powerful environment for designing, orchestrating, and monitoring autonomous AI agents. It provides a sophisticated infrastructure for developing agent-based workflows using the Agentic AI frameworks.

### Core Capabilities

- **Design AI Agent Workflows**: Create multi-agent systems with specialized roles and goals
- **Orchestrate Agent Interactions**: Define how agents collaborate and share information
- **Monitor Executions**: Track agent activities with comprehensive logging and tracing
- **Integrate External Tools**: Connect agents to APIs, data sources, and services
- **Manage Resources**: Control resource allocation and execution constraints

## Documentation

Comprehensive documentation is available in the `/docs` directory and served at the `/docs` endpoint when you run the application.

## Quick Start

### Prerequisites

- Python 3.9+
- Node.js and npm
- PostgreSQL (optional, SQLite is supported by default)

### Installation

1. Clone this repository
2. Create a virtual environment:
   ```
   python3.11 -m venv venv
   source venv/bin/activate 
   ```
3. Install Python dependencies:
   ```
   pip3.11 install -r requirements.txt
   ```
4. Install frontend dependencies:
   ```
   cd frontend
   npm install
   ```

### Running the Application

1. Start the backend:
   ```
   cd backend
   #Postgres
   sh run.sh

   #sqlite
   sh run.sh sqlite
   ```
2. Start the frontend (in a separate terminal):
   ```
   cd frontend
   npm install
   npm start
   ```

3. Access the application at http://localhost:3000
4. Access the documentation at http://localhost:8000/docs

## Building and Deploying

To build and deploy the application to Databricks Apps:

### Option 1: Using Environment Variables (Recommended)

1. Set the `DATABRICKS_APP_URL` environment variable:
   ```bash
   export DATABRICKS_APP_URL="https://your-app-name.cloud.databricks.com/api/v1"
   ```

2. Build the wheel package:
   ```bash
   python build.py
   ```

3. Deploy the built package:
   ```bash
   python deploy.py --app-name kasal --user-name your.email@databricks.com
   ```

### Option 2: Using Command Line Arguments

1. Build the wheel package with explicit API URL:
   ```bash
   python build.py --api-url="https://your-custom-api-url.com/api/v1"
   ```

2. Deploy the built package:
   ```bash
   python deploy.py --app-name kasal --user-name your.email@databricks.com
   ```

The wheel package will be created in the `dist` directory and then deployed to your Databricks workspace. Once deployed, the app will be available under "Apps" in your Databricks workspace.

### ⚠️ Important: OAuth Scope Authorization

**After deploying to Databricks Apps, users must complete OAuth scope authorization to access certain features (like Genie tool).**

The app is configured with OAuth scopes including:
- `sql` - Required for SQL warehouse and database access
- `dashboards.genie` - Required for Genie AI/BI functionality

**Required Steps for Users:**

1. **First-time access**: When users first visit the deployed app, they will see an OAuth consent screen
2. **Grant permissions**: Users must click "Authorize" to grant the app access to the required scopes
3. **Re-authorization needed**: If scopes are updated after deployment, existing users need to:
   - Clear their browser cache/session
   - Log out of Databricks completely  
   - Visit the app URL again to trigger the OAuth consent flow
   - Re-authorize with the new scopes

**Troubleshooting OAuth Issues:**

If users encounter "403 Forbidden" errors when using tools like GenieTool:

1. Check the app's OAuth configuration in Databricks workspace under "Apps" → [Your App] → "Authorization"
2. Verify the user has granted consent for `sql` and `dashboards.genie` scopes
3. If scopes appear correct but tools still fail, the user should:
   - Log out of Databricks completely
   - Clear browser cache
   - Access the app URL fresh to re-trigger OAuth consent
   - Look for and approve any new scope authorization prompts

The OAuth token forwarded via `X-Forwarded-Access-Token` header will only contain scopes the user has explicitly consented to, regardless of what the app is configured to request.

### API Configuration

The frontend automatically picks up the API URL in the following priority order:

1. `DATABRICKS_APP_URL` environment variable (highest priority)
2. `REACT_APP_API_URL` environment variable  
3. `http://localhost:8000/api/v1` (default for local development)

This is configured in `frontend/src/config/api/ApiConfig.ts`:

```typescript
export const config = {
  apiUrl: process.env.DATABRICKS_APP_URL || process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1',
};
```

For Databricks Apps deployment, set the `DATABRICKS_APP_URL` environment variable to your app's URL, and the frontend will automatically use it without requiring code changes.

## Architecture

Kasal follows a modular, layered architecture:

```
Frontend (React) → API (FastAPI) → Services → Repositories → Database
```

The CrewAI Engine is integrated at the service layer, providing agent management, task orchestration, and execution monitoring.

## License

[Databricks License](LICENSE)