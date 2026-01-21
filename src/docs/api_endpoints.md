# Kasal API Endpoints Reference

Complete reference for all available API endpoints in the Kasal platform.

---

## Base URL

All API endpoints use the following base URL structure:

```
https://<your-app>.databricksapps.com/api/v1
```

**Example:**
```
https://kasal-dev-1444828305810485.aws.databricksapps.com/api/v1/executions
```

**Local Development:**
```
http://localhost:8000/api/v1
```

---

## Table of Contents

- [Authentication](#authentication)
- [Crews (Workflows)](#crews-workflows)
- [Agents](#agents)
- [Tasks](#tasks)
- [Tools](#tools)
- [Executions](#executions)
- [Models](#models)
- [API Keys](#api-keys)
- [Power BI Integration](#power-bi-integration)
- [Measure Conversion Pipeline](#measure-conversion-pipeline)
- [Health & Status](#health--status)

---

## Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/auth/login` | User login with credentials |
| `POST` | `/auth/logout` | User logout |
| `GET` | `/auth/me` | Get current user information |
| `POST` | `/auth/refresh` | Refresh JWT token |

**Authentication Header:**
```
Authorization: Bearer <JWT_TOKEN>
```

---

## Crews (Workflows)

### Crew Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/crews` | List all crews in workspace |
| `POST` | `/crews` | Create a new crew |
| `GET` | `/crews/{id}` | Get crew details by ID |
| `PUT` | `/crews/{id}` | Update crew configuration |
| `DELETE` | `/crews/{id}` | Delete crew |
| `POST` | `/crews/{id}/duplicate` | Duplicate crew with new name |

### Crew Execution

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/crews/{id}/kickoff` | Start crew execution |
| `POST` | `/crews/{id}/kickoff-async` | Start async crew execution |
| `GET` | `/crews/{id}/status` | Get crew execution status |
| `POST` | `/crews/{id}/stop` | Stop running crew |

### Crew Export/Import

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/crews/{id}/export` | Export crew configuration as JSON |
| `POST` | `/crews/import` | Import crew from JSON |

---

## Agents

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/agents` | List all agents |
| `POST` | `/agents` | Create a new agent |
| `GET` | `/agents/{id}` | Get agent details by ID |
| `PUT` | `/agents/{id}` | Update agent configuration |
| `DELETE` | `/agents/{id}` | Delete agent |

**Agent Configuration Fields:**
- `name`: Agent name
- `role`: Agent role description
- `goal`: Agent's objective
- `backstory`: Agent's background context
- `tools`: Array of tool IDs
- `tool_configs`: Tool-specific configurations
- `llm_config`: LLM model and parameters

---

## Tasks

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/tasks` | List all tasks |
| `POST` | `/tasks` | Create a new task |
| `GET` | `/tasks/{id}` | Get task details by ID |
| `PUT` | `/tasks/{id}` | Update task configuration |
| `DELETE` | `/tasks/{id}` | Delete task |

**Task Configuration Fields:**
- `name`: Task name
- `description`: Task description
- `expected_output`: Expected output format
- `agent_id`: Assigned agent ID
- `context`: Context task IDs (dependencies)
- `tool_configs`: Task-level tool configurations

---

## Tools

### Tool Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/tools` | List all available tools |
| `GET` | `/tools/{id}` | Get tool details by ID |
| `PUT` | `/tools/{id}` | Update tool configuration |
| `POST` | `/tools/{id}/enable` | Enable tool for workspace |
| `POST` | `/tools/{id}/disable` | Disable tool for workspace |

### Tool Categories

**Available Tool Types:**
- `ai`: AI-powered tools (Dall-E, Perplexity)
- `database`: Database tools (Genie, Databricks, Power BI)
- `search`: Search tools (Serper, Knowledge Search)
- `web`: Web tools (Scrape Website)
- `integration`: Integration tools (MCP)
- `development`: Development tools

---

## Executions

### Execution Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/executions` | List all executions |
| `GET` | `/executions/{id}` | Get execution details |
| `GET` | `/executions/{id}/status` | Get execution status |
| `GET` | `/executions/{id}/logs` | Get execution logs |
| `POST` | `/executions/{id}/stop` | Stop running execution |
| `DELETE` | `/executions/{id}` | Delete execution record |

### Execution Status Values

- `pending`: Execution queued
- `running`: Execution in progress
- `completed`: Execution finished successfully
- `failed`: Execution failed with error
- `stopped`: Execution manually stopped

---

## Models

### Model Configuration

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/models` | List available LLM models |
| `GET` | `/models/{id}` | Get model configuration |
| `PUT` | `/models/{id}` | Update model parameters |
| `POST` | `/models/test` | Test model connection |

**Supported Model Providers:**
- Databricks (Foundation Models)
- OpenAI (GPT-3.5, GPT-4)
- Anthropic (Claude)
- Google (Gemini)
- Azure OpenAI
- Ollama (Local models)

---

## API Keys

### API Key Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api-keys` | List all API keys (encrypted) |
| `POST` | `/api-keys` | Create new API key |
| `GET` | `/api-keys/{id}` | Get API key details |
| `PUT` | `/api-keys/{id}` | Update API key value |
| `DELETE` | `/api-keys/{id}` | Delete API key |

**Common API Keys:**
- `OPENAI_API_KEY`: OpenAI authentication
- `ANTHROPIC_API_KEY`: Anthropic Claude authentication
- `SERPER_API_KEY`: Serper search tool
- `PERPLEXITY_API_KEY`: Perplexity AI tool
- `DATABRICKS_TOKEN`: Databricks API access
- `POWERBI_CLIENT_SECRET`: Power BI service principal
- `POWERBI_USERNAME`: Power BI device code auth
- `POWERBI_PASSWORD`: Power BI device code auth

**Security:**
- All API keys are encrypted at rest
- Keys are never returned in plain text via API
- Multi-tenant isolation by group_id

---

## Power BI Integration

### Power BI Analysis Tool

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/tasks` | Create task with PowerBIAnalysisTool |
| `GET` | `/tasks/{id}` | Get PowerBI task configuration |
| `PUT` | `/tasks/{id}` | Update PowerBI task configuration |

#### POST Example: Create Task with PowerBI Analysis Tool

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Analyze Year-over-Year Sales Growth",
    "description": "Execute DAX analysis on Power BI semantic model to calculate YoY growth for all product categories",
    "expected_output": "Detailed sales growth analysis with trends and insights",
    "agent_id": "agent_123",
    "tools": [71],
    "tool_configs": {
      "PowerBIAnalysisTool": {
        "tenant_id": "<YOUR_AZURE_TENANT_ID>",
        "client_id": "<YOUR_AZURE_CLIENT_ID>",
        "semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
        "workspace_id": "<YOUR_POWERBI_WORKSPACE_ID>",
        "auth_method": "service_principal",
        "databricks_job_id": 365257288725339
      }
    }
  }'
```

**Response:**
```json
{
  "id": "task_456",
  "name": "Analyze Year-over-Year Sales Growth",
  "description": "Execute DAX analysis on Power BI semantic model to calculate YoY growth for all product categories",
  "expected_output": "Detailed sales growth analysis with trends and insights",
  "agent_id": "agent_123",
  "tools": [71],
  "tool_configs": {
    "PowerBIAnalysisTool": {
      "tenant_id": "<YOUR_AZURE_TENANT_ID>",
      "client_id": "<YOUR_AZURE_CLIENT_ID>",
      "semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
      "workspace_id": "<YOUR_POWERBI_WORKSPACE_ID>",
      "auth_method": "service_principal",
      "databricks_job_id": 365257288725339
    }
  },
  "created_at": "2024-12-15T10:00:00Z",
  "updated_at": "2024-12-15T10:00:00Z"
}
```

#### GET Example: Retrieve PowerBI Task Configuration

**Request:**
```bash
curl -X GET http://localhost:8000/api/v1/tasks/task_456 \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**
```json
{
  "id": "task_456",
  "name": "Analyze Year-over-Year Sales Growth",
  "description": "Execute DAX analysis on Power BI semantic model to calculate YoY growth for all product categories",
  "expected_output": "Detailed sales growth analysis with trends and insights",
  "agent_id": "agent_123",
  "tools": [71],
  "tool_configs": {
    "PowerBIAnalysisTool": {
      "tenant_id": "<YOUR_AZURE_TENANT_ID>",
      "client_id": "<YOUR_AZURE_CLIENT_ID>",
      "semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
      "workspace_id": "<YOUR_POWERBI_WORKSPACE_ID>",
      "auth_method": "service_principal",
      "databricks_job_id": 365257288725339
    }
  },
  "async_execution": false,
  "config": {
    "retry_on_fail": true,
    "max_retries": 3
  },
  "created_at": "2024-12-15T10:00:00Z",
  "updated_at": "2024-12-15T10:00:00Z"
}
```

**Required API Keys:**
- `POWERBI_CLIENT_SECRET`
- `POWERBI_USERNAME` (for device_code)
- `POWERBI_PASSWORD` (for device_code)
- `DATABRICKS_API_KEY` or `DATABRICKS_TOKEN`

---

## Measure Conversion Pipeline

### Measure Converter Tool

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/tasks` | Create task with Measure Conversion Pipeline |
| `GET` | `/tasks/{id}` | Get measure converter task configuration |
| `PUT` | `/tasks/{id}` | Update measure converter configuration |

#### POST Example: Power BI to Unity Catalog Conversion

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Migrate Sales Measures to UC",
    "description": "Extract all sales-related measures from Power BI and convert to Unity Catalog Metrics format",
    "expected_output": "Unity Catalog metrics created in analytics.sales_metrics schema",
    "agent_id": "agent_789",
    "tools": [85],
    "tool_configs": {
      "Measure Conversion Pipeline": {
        "inbound_connector": "powerbi",
        "outbound_format": "uc_metrics",
        "powerbi_semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
        "powerbi_group_id": "<YOUR_POWERBI_WORKSPACE_ID>",
        "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
        "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
        "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
        "powerbi_include_hidden": false,
        "powerbi_filter_pattern": "Sales.*",
        "uc_catalog": "analytics",
        "uc_schema": "sales_metrics",
        "uc_process_structures": true,
        "definition_name": "Sales Metrics Migration"
      }
    }
  }'
```

**Response:**
```json
{
  "id": "task_789",
  "name": "Migrate Sales Measures to UC",
  "description": "Extract all sales-related measures from Power BI and convert to Unity Catalog Metrics format",
  "expected_output": "Unity Catalog metrics created in analytics.sales_metrics schema",
  "agent_id": "agent_789",
  "tools": [85],
  "tool_configs": {
    "Measure Conversion Pipeline": {
      "inbound_connector": "powerbi",
      "outbound_format": "uc_metrics",
      "powerbi_semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
      "powerbi_group_id": "<YOUR_POWERBI_WORKSPACE_ID>",
      "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
      "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
      "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
      "powerbi_include_hidden": false,
      "powerbi_filter_pattern": "Sales.*",
      "uc_catalog": "analytics",
      "uc_schema": "sales_metrics",
      "uc_process_structures": true,
      "definition_name": "Sales Metrics Migration"
    }
  },
  "created_at": "2024-12-15T11:00:00Z",
  "updated_at": "2024-12-15T11:00:00Z"
}
```

#### POST Example: Power BI to SQL Conversion

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Convert Revenue Measures to SQL",
    "description": "Convert Power BI revenue measures to Snowflake SQL queries",
    "expected_output": "SQL query definitions compatible with Snowflake",
    "agent_id": "agent_789",
    "tools": [85],
    "tool_configs": {
      "Measure Conversion Pipeline": {
        "inbound_connector": "powerbi",
        "outbound_format": "sql",
        "powerbi_semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
        "powerbi_group_id": "<YOUR_POWERBI_WORKSPACE_ID>",
        "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
        "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
        "powerbi_filter_pattern": "Revenue.*",
        "sql_dialect": "snowflake",
        "sql_include_comments": true,
        "sql_process_structures": true,
        "definition_name": "Revenue Metrics SQL Export"
      }
    }
  }'
```

**Response:**
```json
{
  "id": "task_790",
  "name": "Convert Revenue Measures to SQL",
  "description": "Convert Power BI revenue measures to Snowflake SQL queries",
  "expected_output": "SQL query definitions compatible with Snowflake",
  "agent_id": "agent_789",
  "tools": [85],
  "tool_configs": {
    "Measure Conversion Pipeline": {
      "inbound_connector": "powerbi",
      "outbound_format": "sql",
      "powerbi_semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
      "powerbi_group_id": "<YOUR_POWERBI_WORKSPACE_ID>",
      "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
      "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
      "powerbi_filter_pattern": "Revenue.*",
      "sql_dialect": "snowflake",
      "sql_include_comments": true,
      "sql_process_structures": true,
      "definition_name": "Revenue Metrics SQL Export"
    }
  },
  "created_at": "2024-12-15T11:30:00Z",
  "updated_at": "2024-12-15T11:30:00Z"
}
```

#### POST Example: YAML to DAX Conversion

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Generate DAX from KPI Definitions",
    "description": "Convert YAML KPI definitions to Power BI DAX measures",
    "expected_output": "DAX measure definitions ready for Power BI",
    "agent_id": "agent_789",
    "tools": [85],
    "tool_configs": {
      "Measure Conversion Pipeline": {
        "inbound_connector": "yaml",
        "outbound_format": "dax",
        "yaml_file_path": "/dbfs/mnt/data/kpi_definitions.yaml",
        "dax_process_structures": true,
        "definition_name": "KPI to DAX Conversion"
      }
    }
  }'
```

**Response:**
```json
{
  "id": "task_791",
  "name": "Generate DAX from KPI Definitions",
  "description": "Convert YAML KPI definitions to Power BI DAX measures",
  "expected_output": "DAX measure definitions ready for Power BI",
  "agent_id": "agent_789",
  "tools": [85],
  "tool_configs": {
    "Measure Conversion Pipeline": {
      "inbound_connector": "yaml",
      "outbound_format": "dax",
      "yaml_file_path": "/dbfs/mnt/data/kpi_definitions.yaml",
      "dax_process_structures": true,
      "definition_name": "KPI to DAX Conversion"
    }
  },
  "created_at": "2024-12-15T12:00:00Z",
  "updated_at": "2024-12-15T12:00:00Z"
}
```

**Configuration Options by Format:**

**Inbound Connectors (FROM):**
- `powerbi`: Extract from Power BI datasets
  - Required: `powerbi_semantic_model_id`, `powerbi_group_id`, `powerbi_tenant_id`, `powerbi_client_id`
  - Optional: `powerbi_include_hidden`, `powerbi_filter_pattern`
- `yaml`: Load from YAML files
  - Required: `yaml_content` OR `yaml_file_path`

**Outbound Formats (TO):**
- `dax`: Power BI / Analysis Services measures
  - Optional: `dax_process_structures`
- `sql`: SQL queries (multiple dialects)
  - Optional: `sql_dialect` (databricks, postgresql, mysql, sqlserver, snowflake, bigquery, standard)
  - Optional: `sql_include_comments`, `sql_process_structures`
- `uc_metrics`: Databricks Unity Catalog Metrics
  - Required: `uc_catalog`, `uc_schema`
  - Optional: `uc_process_structures`
- `yaml`: Portable YAML definitions
  - Optional: `definition_name`

**Required API Keys:**
- `POWERBI_CLIENT_SECRET` (for Power BI source)
- `POWERBI_USERNAME` (for device_code auth)
- `POWERBI_PASSWORD` (for device_code auth)
- `DATABRICKS_API_KEY` (for Unity Catalog target)

---

## Health & Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | API health check |
| `GET` | `/health/db` | Database connection status |
| `GET` | `/health/services` | External services status |
| `GET` | `/version` | API version information |

---

## Common Response Formats

### Success Response

```json
{
  "status": "success",
  "data": { ... },
  "message": "Operation completed successfully"
}
```

### Error Response

```json
{
  "status": "error",
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": { ... }
  }
}
```

### Pagination

For list endpoints that support pagination:

```
GET /crews?page=1&limit=50&sort=created_at&order=desc
```

**Query Parameters:**
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 50, max: 100)
- `sort`: Sort field
- `order`: Sort order (`asc` or `desc`)

---

## Rate Limiting

**Default Limits:**
- Anonymous: 100 requests/hour
- Authenticated: 1000 requests/hour
- Enterprise: 10,000 requests/hour

**Rate Limit Headers:**
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1609459200
```

---

## Memory Management

### GET /api/v1/memory/{crew_id}
**Get crew memory (short-term and long-term)**

```json
Response: 200 OK
{
  "short_term": [
    {
      "timestamp": "2024-01-15T10:00:00Z",
      "content": "Customer prefers email communication"
    }
  ],
  "long_term": [
    {
      "category": "preferences",
      "insights": ["Email preferred", "Weekly reports"]
    }
  ]
}
```

### POST /api/v1/memory/{crew_id}/clear
**Clear crew memory**

```json
Request:
{
  "type": "short_term"  // Options: "short_term", "long_term", or "all"
}

Response: 204 No Content
```

---

## WebSocket Endpoints

### Real-Time Execution Updates

```
ws://localhost:8000/ws/executions/{execution_id}
```

**Message Format:**
```json
{
  "type": "status_update",
  "execution_id": "abc123",
  "status": "running",
  "progress": 45,
  "message": "Processing task 2 of 5..."
}
```

---

## Examples

### Create and Execute a Crew

```bash
# 1. Create a crew
curl -X POST http://localhost:8000/api/v1/crews \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Sales Analysis Crew",
    "agents": [...],
    "tasks": [...]
  }'

# Response: {"id": "crew_123", ...}

# 2. Start execution
curl -X POST http://localhost:8000/api/v1/crews/crew_123/kickoff \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"inputs": {"query": "Analyze Q4 sales"}}'

# Response: {"execution_id": "exec_456", ...}

# 3. Monitor execution
curl -X GET http://localhost:8000/api/v1/executions/exec_456/status \
  -H "Authorization: Bearer $TOKEN"
```

### Configure Power BI Tool in Task

```bash
# Create task with PowerBI configuration
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Analyze Sales Data",
    "description": "Analyze sales trends using Power BI",
    "agent_id": "agent_123",
    "tools": [71],
    "tool_configs": {
      "PowerBIAnalysisTool": {
        "tenant_id": "<YOUR_AZURE_TENANT_ID>",
        "client_id": "<YOUR_AZURE_CLIENT_ID>",
        "semantic_model_id": "<YOUR_POWERBI_SEMANTIC_MODEL_ID>",
        "workspace_id": "<YOUR_POWERBI_WORKSPACE_ID>",
        "auth_method": "service_principal",
        "databricks_job_id": 365257288725339
      }
    }
  }'
```

---

## Error Codes

| Code | Description |
|------|-------------|
| `AUTH_001` | Invalid or expired token |
| `AUTH_002` | Insufficient permissions |
| `CREW_001` | Crew not found |
| `CREW_002` | Invalid crew configuration |
| `EXEC_001` | Execution failed |
| `EXEC_002` | Execution timeout |
| `TOOL_001` | Tool not available |
| `TOOL_002` | Tool configuration error |
| `DB_001` | Database connection error |
| `EXT_001` | External service unavailable |

---

## SDK Examples

### Python SDK

```python
from kasal import KasalClient

# Initialize client
client = KasalClient(
    base_url="http://localhost:8000",
    token="your-jwt-token"
)

# Create and execute crew
crew = client.crews.create(
    name="Data Analysis Crew",
    agents=[...],
    tasks=[...]
)

execution = crew.kickoff(inputs={"query": "Analyze data"})
result = execution.wait()  # Blocks until complete

print(result.output)
```

### JavaScript/TypeScript SDK

```typescript
import { KasalClient } from '@kasal/sdk';

const client = new KasalClient({
  baseUrl: 'http://localhost:8000',
  token: 'your-jwt-token'
});

// Create and execute crew
const crew = await client.crews.create({
  name: 'Data Analysis Crew',
  agents: [...],
  tasks: [...]
});

const execution = await crew.kickoff({
  inputs: { query: 'Analyze data' }
});

// Stream results
execution.on('status', (status) => {
  console.log('Status:', status);
});

const result = await execution.wait();
console.log('Result:', result.output);
```

---

## Additional Resources

- **API Playground**: `/api/playground`
- **OpenAPI Schema**: `/api/openapi.json`
- **Swagger UI**: `/api/docs`
- **ReDoc**: `/api/redoc`

For more information, see:
- [Power BI Integration Guide](powerbi_integration.md)
- [Tool Configuration Guide](powerbi_analysis_tool_setup.md)
- [Crew Deployment Guide](crew_export_deployment.md)
