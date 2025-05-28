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

1. Build the wheel package:
   ```
   python build.py --api-url="https://your-custom-api-url.com/api/v1"
   ```

2. Deploy the built package:
   ```
   python deploy.py --app-name kasal --user-name your.email@databricks.com
   ```

The wheel package will be created in the `dist` directory and then deployed to your Databricks workspace. Once deployed, the app will be available under "Apps" in your Databricks workspace.

### API Configuration

Before deploying, make sure to configure the API URL in `frontend/src/config/api/ApiConfig.ts`:

```typescript
export const config = {
  apiUrl: 'http://localhost:8000/api/v1',  // For local development
  // apiUrl: 'https://your-app-name.cloud.databricks.com/api/v1',  // For production
};
```

Update this URL to match your deployment environment. The app will use this URL to communicate with the backend API.

## Architecture

Kasal follows a modular, layered architecture:

```
Frontend (React) → API (FastAPI) → Services → Repositories → Database
```

The CrewAI Engine is integrated at the service layer, providing agent management, task orchestration, and execution monitoring.

## License

[Databricks License](LICENSE)