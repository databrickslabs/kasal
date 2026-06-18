# Kasal REST API Documentation

This document provides comprehensive documentation for the Kasal REST API, covering all endpoints for managing AI agent workflows.

## Table of Contents

- [Overview](#overview)
- [Authentication](#authentication)
- [Base URL & API Structure](#base-url--api-structure)
- [Core AI Workflow Endpoints](#core-ai-workflow-endpoints)
- [Execution & Monitoring](#execution--monitoring)
- [Generation & AI Services](#generation--ai-services)
- [Configuration & Management](#configuration--management)
- [Common Request/Response Patterns](#common-requestresponse-patterns)
- [Error Handling](#error-handling)
- [OpenAPI Documentation](#openapi-documentation)

## Overview

Kasal's REST API provides endpoints for creating, managing, and executing AI agent workflows. The API is built with FastAPI and includes:

- **AI Agent Management**: Create and configure autonomous AI agents
- **Crew Orchestration**: Organize agents into collaborative teams
- **Task Definition**: Define specific tasks for agents to complete
- **Workflow Execution**: Run crews and flows with real-time monitoring
- **Multi-tenant Support**: Group-based data isolation
- **Multiple LLM Providers**: Support for OpenAI, Anthropic, DeepSeek, Ollama, Databricks

## Authentication

Kasal supports multiple authentication methods:

### API Keys
```http
Authorization: Bearer your-api-key-here
```

### Databricks Apps Integration
When deployed as a Databricks App, authentication is handled through Databricks OAuth headers:
- User context extracted from Databricks headers
- Automatic group assignment based on workspace

### Multi-tenant Groups
All API operations are scoped to user groups for data isolation. Group context is automatically determined from authentication.

## Base URL & API Structure

- **Base URL**: `http://localhost:8000` (development)
- **API Version**: `/api/v1`
- **Full API Base**: `http://localhost:8000/api/v1`

## Core AI Workflow Endpoints

### Agents API (`/agents`)

Manage AI agents - individual autonomous workers in your workflow.

#### Create Agent
```http
POST /api/v1/agents
Content-Type: application/json

{
  "name": "Research Agent",
  "role": "Senior Researcher",
  "goal": "Conduct thorough research on given topics",
  "backstory": "You are an experienced researcher with expertise in gathering and analyzing information",
  "llm": "gpt-4",
  "tools": ["search_tool", "browser_tool"],
  "allow_delegation": false,
  "verbose": true
}
```

#### List Agents
```http
GET /api/v1/agents
```

#### Get Specific Agent
```http
GET /api/v1/agents/{agent_id}
```

#### Update Agent
```http
PUT /api/v1/agents/{agent_id}
Content-Type: application/json

{
  "name": "Updated Research Agent",
  "goal": "Enhanced research capabilities"
}
```

#### Delete Agent
```http
DELETE /api/v1/agents/{agent_id}
```

### Crews API (`/crews`)

Manage crews - teams of agents working together on complex tasks.

#### Create Crew
```http
POST /api/v1/crews
Content-Type: application/json

{
  "name": "Research Team",
  "description": "Team specialized in research and analysis",
  "agents_yaml": "agent_configs_here",
  "tasks_yaml": "task_definitions_here",
  "model": "gpt-4",
  "process": "sequential",
  "verbose": true
}
```

#### List Crews
```http
GET /api/v1/crews
```

#### Get Specific Crew
```http
GET /api/v1/crews/{crew_id}
```

#### Update Crew
```http
PUT /api/v1/crews/{crew_id}
Content-Type: application/json

{
  "name": "Updated Research Team",
  "description": "Enhanced research capabilities"
}
```

#### Debug Crew Configuration
```http
POST /api/v1/crews/debug
Content-Type: application/json

{
  "agents_yaml": "agent_config_to_validate",
  "tasks_yaml": "task_config_to_validate"
}
```

### Tasks API (`/tasks`)

Manage individual tasks within workflows.

#### Create Task
```http
POST /api/v1/tasks
Content-Type: application/json

{
  "name": "Research Task",
  "description": "Research the latest developments in AI",
  "expected_output": "A comprehensive report on AI developments",
  "agent": "research_agent",
  "tools": ["search_tool"]
}
```

#### List Tasks
```http
GET /api/v1/tasks
```

#### Get Specific Task
```http
GET /api/v1/tasks/{task_id}
```

#### Update Task (Full)
```http
PUT /api/v1/tasks/{task_id}/full
Content-Type: application/json

{
  "name": "Updated Research Task",
  "description": "Enhanced research requirements",
  "expected_output": "Detailed research report"
}
```

#### Update Task (Partial)
```http
PUT /api/v1/tasks/{task_id}
Content-Type: application/json

{
  "name": "Updated Task Name"
}
```

### Tools API (`/tools`)

Manage tools that agents can use to perform their tasks.

#### List All Tools
```http
GET /api/v1/tools
```

#### List Enabled Tools Only
```http
GET /api/v1/tools/enabled
```

#### Get Specific Tool
```http
GET /api/v1/tools/{tool_id}
```

#### Create Tool
```http
POST /api/v1/tools
Content-Type: application/json

{
  "name": "Custom Search Tool",
  "description": "A tool for searching specific databases",
  "tool_type": "custom",
  "configuration": {
    "api_endpoint": "https://api.example.com",
    "parameters": {}
  },
  "enabled": true
}
```

#### Toggle Tool Status
```http
PATCH /api/v1/tools/{tool_id}/toggle-enabled
```

#### Get Tool Configuration Schema
```http
GET /api/v1/tools/configurations/{tool_name}/schema
```

## Execution & Monitoring

### Executions API (`/executions`)

Execute crews and workflows with real-time monitoring.

#### Create and Start Execution
```http
POST /api/v1/executions
Content-Type: application/json

{
  "crew_id": "crew-uuid-here",
  "inputs": {
    "topic": "AI developments in 2024"
  },
  "model": "gpt-4"
}
```

**Response:**
```json
{
  "execution_id": "exec-12345",
  "status": "pending",
  "message": "Execution started successfully",
  "run_name": "Research Team - AI Analysis"
}
```

#### Get Execution Status
```http
GET /api/v1/executions/{execution_id}
```

**Response:**
```json
{
  "execution_id": "exec-12345",
  "status": "completed",
  "result": "Detailed research report...",
  "created_at": "2024-01-01T10:00:00Z",
  "completed_at": "2024-01-01T10:15:00Z",
  "run_name": "Research Team - AI Analysis"
}
```

**Status Values:**
- `PENDING` - Execution queued but not started
- `PREPARING` - Execution is being prepared
- `RUNNING` - Execution is actively running
- `COMPLETED` - Execution finished successfully
- `FAILED` - Execution failed with errors
- `CANCELLED` - Execution was cancelled (manually or due to service restart)

#### List Executions
```http
GET /api/v1/executions?limit=10&offset=0
```

#### Generate Execution Name
```http
POST /api/v1/executions/generate-name
Content-Type: application/json

{
  "agents_yaml": "agent_config",
  "tasks_yaml": "task_config",
  "model": "gpt-4"
}
```

### Execution History API (`/execution-history`)

Access historical execution data and analytics.

#### Get Execution History
```http
GET /api/v1/execution-history?limit=50&offset=0
```

#### Get Specific Execution History
```http
GET /api/v1/execution-history/{execution_id}
```

### Execution Logs API (`/execution-logs`)

Access real-time and historical execution logs.

#### Get Execution Logs
```http
GET /api/v1/execution-logs/{execution_id}
```

#### Stream Live Logs (WebSocket)
```http
GET /api/v1/execution-logs/{execution_id}/stream
```

### Execution Trace API (`/execution-trace`)

Detailed execution tracing for debugging and optimization.

#### Get Execution Trace
```http
GET /api/v1/execution-trace/{execution_id}
```

### Execution Lifecycle & Service Restarts

#### Execution State Management

Kasal maintains execution state in the database to ensure consistency. When the service is restarted:

1. **Automatic Cleanup**: Any executions in `PENDING`, `PREPARING`, or `RUNNING` states are automatically marked as `CANCELLED`
2. **Reason Tracking**: The cancellation includes a message: "Job cancelled - service was restarted while job was running"
3. **Single Job Constraint**: Kasal currently enforces that only one job can run at a time
4. **Clean State**: After restart, users can immediately start new executions

#### Graceful Shutdown

The service handles shutdown through FastAPI's lifespan manager:
- The finally block attempts to clean up running executions
- State is properly persisted to the database
- In development mode with `--reload`, shutdown may be less graceful

This design ensures that users are never blocked from starting new executions. Even if shutdown isn't graceful, the startup cleanup will handle any orphaned jobs from the previous service instance.

## Generation & AI Services

### Agent Generation API (`/agent-generation`)

AI-powered agent generation using natural language descriptions.

#### Generate Agent from Description
```http
POST /api/v1/agent-generation/generate
Content-Type: application/json

{
  "description": "Create a customer service agent that can handle inquiries and escalate complex issues",
  "model": "gpt-4"
}
```

**Response:**
```json
{
  "agent": {
    "name": "Customer Service Agent",
    "role": "Customer Support Specialist",
    "goal": "Provide excellent customer service and resolve inquiries",
    "backstory": "You are an experienced customer service professional...",
    "tools": ["email_tool", "knowledge_base_tool"]
  }
}
```

**Note:** Generation features are accessible through UI dialogs and components, not through keyboard shortcuts.

### Task Generation API (`/task-generation`)

AI-powered task generation for workflow automation.

#### Generate Task from Description
```http
POST /api/v1/task-generation/generate
Content-Type: application/json

{
  "description": "Create a task for analyzing customer feedback data",
  "model": "gpt-4"
}
```

### Crew Generation API (`/crew-generation`)

AI-powered crew generation for complex workflows.

#### Generate Crew Configuration
```http
POST /api/v1/crew-generation/generate
Content-Type: application/json

{
  "description": "Create a crew for content marketing including research, writing, and review",
  "model": "gpt-4"
}
```

## Configuration & Management

### Models API (`/models`)

Manage LLM model configurations.

#### List Available Models
```http
GET /api/v1/models
```

#### Get Model Configuration
```http
GET /api/v1/models/{model_name}
```

#### Create Model Configuration
```http
POST /api/v1/models
Content-Type: application/json

{
  "name": "custom-gpt-4",
  "provider": "openai",
  "model": "gpt-4",
  "api_key_name": "openai_api_key",
  "parameters": {
    "temperature": 0.7,
    "max_tokens": 2000
  }
}
```

### API Keys API (`/api-keys`)

Manage API keys for various services.

#### List API Keys
```http
GET /api/v1/api-keys
```

#### Create API Key
```http
POST /api/v1/api-keys
Content-Type: application/json

{
  "name": "OpenAI API Key",
  "service": "openai",
  "key": "sk-..."
}
```

#### Update API Key
```http
PUT /api/v1/api-keys/{key_id}
Content-Type: application/json

{
  "name": "Updated OpenAI Key",
  "key": "sk-new-key..."
}
```

### Templates API (`/templates`)

Manage prompt templates for consistent AI interactions.

#### List Templates
```http
GET /api/v1/templates
```

#### Create Template
```http
POST /api/v1/templates
Content-Type: application/json

{
  "name": "Research Agent Template",
  "template": "You are a research agent. Your task is to {task_description}. Please provide {expected_output}.",
  "variables": ["task_description", "expected_output"]
}
```

### Scheduler API (`/scheduler`)

Manage scheduled workflow executions.

#### List Schedules
```http
GET /api/v1/scheduler/schedules
```

#### Create Schedule
```http
POST /api/v1/scheduler/schedules
Content-Type: application/json

{
  "name": "Daily Reports",
  "cron_expression": "0 9 * * *",
  "crew_id": "crew-uuid",
  "inputs": {
    "report_date": "today"
  },
  "enabled": true
}
```

#### Enable/Disable Schedule
```http
PATCH /api/v1/scheduler/schedules/{schedule_id}/toggle
```

## Common Request/Response Patterns

### Standard Response Format

Most endpoints return responses in this format:

```json
{
  "id": "resource-id",
  "name": "Resource Name",
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-01T10:00:00Z",
  "group_id": "group-uuid",
  // ... other resource-specific fields
}
```

### Pagination

List endpoints support pagination:

```http
GET /api/v1/agents?limit=20&offset=0
```

**Response:**
```json
{
  "items": [...],
  "total": 100,
  "limit": 20,
  "offset": 0,
  "has_more": true
}
```

### Filtering

Many endpoints support filtering by group:

```http
GET /api/v1/executions?group_id=group-uuid&status=completed
```

## Error Handling

The API uses standard HTTP status codes and returns error details:

### Common Status Codes

- `200 OK` - Success
- `201 Created` - Resource created successfully
- `400 Bad Request` - Invalid request data
- `401 Unauthorized` - Authentication required
- `403 Forbidden` - Insufficient permissions
- `404 Not Found` - Resource not found
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Server error

### Error Response Format

```json
{
  "detail": "Error description",
  "type": "validation_error",
  "errors": [
    {
      "field": "name",
      "message": "Name is required"
    }
  ]
}
```

### Validation Errors

Pydantic validation errors provide detailed field-level feedback:

```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "name"],
      "msg": "Field required",
      "input": {}
    }
  ]
}
```

## OpenAPI Documentation

Kasal provides interactive API documentation through FastAPI's built-in OpenAPI support:

### Interactive Documentation
- **Swagger UI**: `http://localhost:8000/api-docs`
- **ReDoc**: `http://localhost:8000/api-redoc`
- **OpenAPI JSON**: `http://localhost:8000/api-openapi.json`

### Features
- Interactive endpoint testing
- Request/response schema documentation
- Authentication testing
- Example requests and responses
- Downloadable OpenAPI specification

### Configuration

API documentation can be enabled/disabled via environment variables:
```bash
DOCS_ENABLED=true  # Enable API docs (default: true in development)
```

## SDK and Client Libraries

While Kasal doesn't currently provide official SDKs, the OpenAPI specification can be used to generate client libraries in various programming languages:

```bash
# Generate Python client
openapi-generator generate -i http://localhost:8000/api-openapi.json -g python -o ./kasal-python-client

# Generate JavaScript/TypeScript client
openapi-generator generate -i http://localhost:8000/api-openapi.json -g typescript-axios -o ./kasal-js-client
```

## Rate Limiting and Performance

- **Rate Limiting**: Not currently implemented (consider for production)
- **Request Timeout**: 120 seconds for execution endpoints
- **Concurrent Executions**: Managed through background task queue
- **Database Connections**: Async connection pooling with SQLAlchemy

## Security Considerations

1. **API Key Management**: Store API keys securely, never in plain text
2. **Group Isolation**: All data is isolated by group for multi-tenancy
3. **Tool Security**: Tools can be enabled/disabled for security
4. **Input Validation**: All inputs validated through Pydantic schemas
5. **CORS Configuration**: Configurable for cross-origin requests

## Best Practices

1. **Error Handling**: Always check response status codes
2. **Polling**: Use appropriate intervals when polling execution status
3. **Resource Management**: Clean up unused agents, crews, and tasks
4. **Model Selection**: Choose appropriate models based on task complexity
5. **Monitoring**: Use execution logs and traces for debugging
6. **Testing**: Use the debug endpoints to validate configurations

## Support and Troubleshooting

### Health Check
```http
GET /health
```

### Common Issues

1. **403 Forbidden**: Check group permissions and authentication
2. **422 Validation Error**: Review request schema requirements
3. **Execution Timeout**: Check execution logs for detailed error information
4. **Tool Not Found**: Ensure required tools are enabled and configured

### Getting Help

- Check execution logs for detailed error information
- Use debug endpoints to validate configurations
- Review the interactive API documentation at `/api-docs`
- Monitor execution traces for performance optimization