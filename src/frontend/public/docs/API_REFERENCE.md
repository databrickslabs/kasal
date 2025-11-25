# Kasal API Reference

> **RESTful API v1** - Complete endpoint documentation with examples

---

## Getting Started
Base URLs, authentication, and rate limits you need before calling endpoints.

### Base URL
```
Production:  https://api.example.com/v1
Staging:     https://staging-api.example.com/v1
Local:       http://localhost:8000/api/v1
```

### Authentication
```bash
# Get access token
curl -X POST https://api.example.com/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "user@example.com", "password": "secure_pass"}'

# Use token in requests
curl -X GET https://api.example.com/v1/crews \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

### Rate Limits
| Tier | Requests/Hour | Burst |
|------|---------------|-------|
| **Free** | 1,000 | 100/min |
| **Pro** | 10,000 | 1,000/min |
| **Enterprise** | Unlimited | Custom |

---

## Authentication Endpoints
Login, refresh, and logout flows to manage tokens.

### POST /auth/login
**Login with credentials**
```json
Request:
{
  "email": "user@example.com",
  "password": "secure_password"
}

Response: 200 OK
{
  "access_token": "eyJ0eXAi...",
  "token_type": "bearer",
  "expires_in": 86400
}
```

### POST /auth/refresh
**Refresh access token**
```json
Request:
{
  "refresh_token": "eyJ0eXAi..."
}

Response: 200 OK
{
  "access_token": "eyJ0eXAi...",
  "expires_in": 86400
}
```

### POST /auth/logout
**Invalidate tokens**
```json
Response: 204 No Content
```

---

## Crew Management
Create and manage multi-agent crews and their configurations.

### GET /crews
**List all crews**
```json
Response: 200 OK
{
  "crews": [
    {
      "id": "crew_abc123",
      "name": "Customer Support Crew",
      "status": "active",
      "agents_count": 3,
      "created_at": "2024-01-15T10:30:00Z"
    }
  ],
  "total": 15,
  "page": 1
}
```

### POST /crews
**Create new crew**
```json
Request:
{
  "name": "Marketing Crew",
  "description": "Content generation team",
  "process": "hierarchical",
  "agents": [
    {
      "role": "Content Writer",
      "goal": "Create engaging content",
      "model": "gpt-4"
    }
  ]
}

Response: 201 Created
{
  "id": "crew_xyz789",
  "name": "Marketing Crew",
  "status": "configuring"
}
```

### GET /crews/{crew_id}
**Get crew details**
```json
Response: 200 OK
{
  "id": "crew_abc123",
  "name": "Customer Support Crew",
  "agents": [...],
  "tasks": [...],
  "configuration": {...}
}
```

### PUT /crews/{crew_id}
**Update crew configuration**
```json
Request:
{
  "name": "Updated Crew Name",
  "process": "sequential"
}

Response: 200 OK
{
  "id": "crew_abc123",
  "updated": true
}
```

### DELETE /crews/{crew_id}
**Delete crew**
```json
Response: 204 No Content
```

---

## Agent Management
Create and list individual agents with roles, models, and tools.

### GET /agents
**List all agents**
```json
Response: 200 OK
{
  "agents": [
    {
      "id": "agent_001",
      "name": "Research Agent",
      "crew_id": "crew_abc123",
      "model": "gpt-4",
      "status": "ready"
    }
  ]
}
```

### POST /agents
**Create new agent**
```json
Request:
{
  "crew_id": "crew_abc123",
  "role": "Data Analyst",
  "goal": "Analyze metrics",
  "backstory": "Expert analyst with 10 years experience",
  "model": "claude-3-opus",
  "tools": ["web_search", "calculator"]
}

Response: 201 Created
{
  "id": "agent_002",
  "status": "created"
}
```

---

## Execution Management
Start executions, get status, retrieve traces, and stop runs.

### POST /executions
**Start crew execution**
```json
Request:
{
  "crew_id": "crew_abc123",
  "inputs": {
    "topic": "Q4 Marketing Strategy",
    "deadline": "2024-12-31"
  }
}

Response: 202 Accepted
{
  "job_id": "job_qwerty123",
  "status": "queued",
  "estimated_duration": 300
}
```

### GET /executions/{job_id}
**Get execution status**
```json
Response: 200 OK
{
  "job_id": "job_qwerty123",
  "status": "running",
  "progress": 65,
  "current_task": "Analyzing data",
  "started_at": "2024-01-15T14:00:00Z"
}
```

### GET /executions/{job_id}/traces
**Get execution trace**
```json
Response: 200 OK
{
  "traces": [
    {
      "timestamp": "2024-01-15T14:00:05Z",
      "agent": "Research Agent",
      "action": "web_search",
      "result": "Found 15 relevant articles"
    }
  ]
}
```

### POST /executions/{job_id}/stop
**Stop execution**
```json
Response: 200 OK
{
  "job_id": "job_qwerty123",
  "status": "stopped"
}
```

---

## Task Management
Create and list tasks assigned to agents.

### GET /tasks
**List tasks**
```json
Response: 200 OK
{
  "tasks": [
    {
      "id": "task_001",
      "description": "Generate report",
      "agent_id": "agent_001",
      "status": "completed"
    }
  ]
}
```

### POST /tasks
**Create task**
```json
Request:
{
  "agent_id": "agent_001",
  "description": "Analyze competitor pricing",
  "expected_output": "Markdown report",
  "context": ["Previous analysis from Q3"]
}

Response: 201 Created
{
  "id": "task_002",
  "status": "created"
}
```

---

## Tool Management
Discover built-in tools and register custom tools.

### GET /tools
**List available tools**
```json
Response: 200 OK
{
  "tools": [
    {
      "name": "web_search",
      "description": "Search the web",
      "category": "research"
    },
    {
      "name": "file_reader",
      "description": "Read files",
      "category": "data"
    }
  ]
}
```

### POST /tools/custom
**Register custom tool**
```json
Request:
{
  "name": "salesforce_api",
  "description": "Query Salesforce data",
  "endpoint": "https://api.example.com/salesforce",
  "auth_type": "bearer"
}

Response: 201 Created
{
  "tool_id": "tool_custom_001",
  "status": "registered"
}
```

---

## Memory Management
Fetch and clear short/long-term memory for a crew.

### GET /memory/{crew_id}
**Get crew memory**
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

### POST /memory/{crew_id}/clear
**Clear memory**
```json
Request:
{
  "type": "short_term" // or "long_term" or "all"
}

Response: 204 No Content
```

---



## 🔵 WebSocket Events
Real-time updates for task lifecycle, errors, and progress.

### Connection
```javascript
const ws = new WebSocket('wss://api.kasal.ai/v1/ws');

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'subscribe',
    job_id: 'job_qwerty123'
  }));
};
```

### Event Types
```javascript
// Task started
{
  "type": "task_start",
  "job_id": "job_qwerty123",
  "task_id": "task_001",
  "agent": "Research Agent"
}

// Task completed
{
  "type": "task_complete",
  "job_id": "job_qwerty123",
  "task_id": "task_001",
  "result": "Analysis complete"
}

// Error
{
  "type": "error",
  "job_id": "job_qwerty123",
  "message": "Rate limit exceeded",
  "code": "RATE_LIMIT"
}
```

---

## 🔷 Error Codes
Standardized error responses and meanings.

| Code | Message | Description |
|------|---------|-------------|
| 400 | Bad Request | Invalid parameters |
| 401 | Unauthorized | Invalid/expired token |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource doesn't exist |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Error | Server error |
| 503 | Service Unavailable | Maintenance mode |

### Error Response Format
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid crew configuration",
    "details": {
      "field": "agents",
      "reason": "At least one agent required"
    }
  }
}
```

---

## 🔹 Testing
Sandbox, Postman collection, and OpenAPI spec.

### Sandbox Environment
```bash
# Use sandbox for testing
curl -X POST https://sandbox-api.kasal.ai/v1/crews \
  -H "Authorization: Bearer SANDBOX_TOKEN" \
  -H "Content-Type: application/json" \
  -d @crew.json
```


---

*Build powerful integrations with Kasal API*