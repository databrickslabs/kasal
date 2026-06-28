# Kasal API endpoints reference

Complete reference for all available API endpoints in the Kasal platform.

---

## Base URL

All API endpoints use the following base URL structure:

```text
https://<your-app>.databricksapps.com/api/v1
```

**Example:**
```text
https://<your-app>.aws.databricksapps.com/api/v1/executions
```

**Local Development:**
```text
http://localhost:8000/api/v1
```

---

## Table of contents

- [Authentication](#authentication)
- [Crews (workflows)](#crews-workflows)
- [Agents](#agents)
- [Tasks](#tasks)
- [Tools](#tools)
- [Executions](#executions)
- [Models](#models)
- [API keys](#api-keys)
- [Power BI integration](#power-bi-integration)
- [Health and status](#health-and-status)

---

## Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/auth/login` | User login with credentials |
| `POST` | `/auth/logout` | User logout |
| `GET` | `/auth/me` | Get current user information |
| `POST` | `/auth/refresh` | Refresh JWT token |

**Authentication Header:**
```text
Authorization: Bearer <JWT_TOKEN>
```

---

## Crews (workflows)

### Crew management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/crews` | List all crews in workspace |
| `POST` | `/crews` | Create a new crew |
| `GET` | `/crews/{id}` | Get crew details by ID |
| `PUT` | `/crews/{id}` | Update crew configuration |
| `DELETE` | `/crews/{id}` | Delete crew |
| `POST` | `/crews/{id}/duplicate` | Duplicate crew with new name |

### Crew execution

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/crews/{id}/kickoff` | Start crew execution |
| `POST` | `/crews/{id}/kickoff-async` | Start async crew execution |
| `GET` | `/crews/{id}/status` | Get crew execution status |
| `POST` | `/crews/{id}/stop` | Stop running crew |

### Crew export/import

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

### Tool management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/tools` | List all available tools |
| `GET` | `/tools/{id}` | Get tool details by ID |
| `PUT` | `/tools/{id}` | Update tool configuration |
| `POST` | `/tools/{id}/enable` | Enable tool for workspace |
| `POST` | `/tools/{id}/disable` | Disable tool for workspace |

### Tool categories

**Available Tool Types:**
- `ai`: AI-powered tools (Dall-E, Perplexity)
- `database`: Database tools (Genie, Databricks, Power BI)
- `search`: Search tools (Serper, Knowledge Search)
- `web`: Web tools (Scrape Website)
- `integration`: Integration tools (MCP)
- `development`: Development tools

---

## Executions

### Execution management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/executions` | List all executions |
| `GET` | `/executions/{id}` | Get execution details |
| `GET` | `/executions/{id}/status` | Get execution status |
| `GET` | `/executions/{id}/logs` | Get execution logs |
| `POST` | `/executions/{id}/stop` | Stop running execution |
| `POST` | `/executions/{id}/force-stop` | Force-stop a running execution |
| `DELETE` | `/executions/{id}` | Delete execution record |

### Execution status values

- `pending`: Execution queued
- `running`: Execution in progress
- `completed`: Execution finished successfully
- `failed`: Execution failed with error
- `stopped`: Execution manually stopped

### Execution traces

Trace records capture per-agent / per-task events for a run.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/traces` | List execution traces |
| `GET` | `/traces/{trace_id}` | Get a single trace item |
| `GET` | `/traces/execution/{run_id}` | Traces for a run (by run id) |
| `GET` | `/traces/job/{job_id}` | Traces for a job (by job id) |
| `GET` | `/traces/job/{job_id}/crew-node-states` | Crew node states for a job |
| `GET` | `/traces/job/{job_id}/task-states` | Task states for a job |
| `POST` | `/traces` | Create a trace record |
| `DELETE` | `/traces/{trace_id}` | Delete a trace item |
| `DELETE` | `/traces/execution/{run_id}` | Delete traces for a run |
| `DELETE` | `/traces/job/{job_id}` | Delete traces for a job |

---

## Models

### Model configuration

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

## API keys

### API key management

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

## Power BI integration

### Power BI configuration

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/powerbi/config` | Configure Power BI connection |
| `GET` | `/powerbi/config` | Get Power BI configuration |

**Power BI Tool Configuration (Task-Level):**
```json
{
  "tenant_id": "Azure AD Tenant ID",
  "client_id": "Azure AD Application ID",
  "semantic_model_id": "Power BI Dataset ID",
  "workspace_id": "Power BI Workspace ID (optional)",
  "auth_method": "service_principal or device_code",
  "databricks_job_id": "Databricks Job ID (optional)"
}
```

**Required API Keys:**
- `POWERBI_CLIENT_SECRET`
- `POWERBI_USERNAME` (for device_code)
- `POWERBI_PASSWORD` (for device_code)
- `DATABRICKS_API_KEY` or `DATABRICKS_TOKEN`

---

## Health and status

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | API health check |
| `GET` | `/health/db` | Database connection status |
| `GET` | `/health/services` | External services status |
| `GET` | `/version` | API version information |

---

## Common response formats

### Success response

```json
{
  "status": "success",
  "data": { ... },
  "message": "Operation completed successfully"
}
```

### Error response

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

```text
GET /crews?page=1&limit=50&sort=created_at&order=desc
```

**Query Parameters:**
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 50, max: 100)
- `sort`: Sort field
- `order`: Sort order (`asc` or `desc`)

---

## Rate limiting

**Default Limits:**
- Anonymous: 100 requests/hour
- Authenticated: 1000 requests/hour
- Enterprise: 10,000 requests/hour

**Rate Limit Headers:**
```text
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1609459200
```

---

## Memory management

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

## WebSocket endpoints

### Real-time execution updates

```text
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

### Create and execute a crew

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

### Configure Power BI tool in task

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
        "tenant_id": "<tenant-id>",
        "client_id": "<client-id>",
        "semantic_model_id": "<semantic-model-id>",
        "workspace_id": "<workspace-id>",
        "auth_method": "service_principal",
        "databricks_job_id": "<databricks-job-id>"
      }
    }
  }'
```

---

## Error codes

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

## SDK examples

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

## Additional resources

- **API Playground**: `/api/playground`
- **OpenAPI Schema**: `/api/openapi.json`
- **Swagger UI**: `/api/docs`
- **ReDoc**: `/api/redoc`

## See also
- [Power BI tools reference](./powerbi/README.md)
- [Power BI comprehensive analysis tool](./powerbi/tool-72-comprehensive-analysis.md)
- [Crew export and deployment guide](./crew-export-deployment.md)
- [Developer guide](./DEVELOPER_GUIDE.md)
- [Architecture guide](./ARCHITECTURE_GUIDE.md)

Back to the [documentation hub](./README.md).
