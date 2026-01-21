# Converter API Integration Guide

**Complete guide to using MetricsConverter APIs with CrewAI agents**

---

## Overview

The MetricsConverter provides two integration patterns:

1. **Direct API Usage**: REST endpoints for managing conversion history, jobs, and configurations
2. **CrewAI Tools**: Converter tools that can be used by AI agents in crews

Both patterns work together seamlessly - crews can use converter tools for conversions while the API tracks history and manages configurations.

---

## Architecture Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                        Frontend / Client                         │
└────────────┬─────────────────────────────────┬──────────────────┘
             │                                 │
             ├─────────────────────┐           │
             │                     │           │
             ▼                     ▼           ▼
    ┌────────────────┐   ┌─────────────────┐  ┌──────────────────┐
    │ Converter API  │   │   Crews API     │  │  Direct Tools    │
    │ /api/converters│   │  /api/v1/crews  │  │    (Agents)      │
    └────────┬───────┘   └────────┬────────┘  └────────┬─────────┘
             │                    │                     │
             │                    └──────┬──────────────┘
             │                           │
             ▼                           ▼
    ┌────────────────────────────────────────────────┐
    │           Converter Engine Core                │
    │  ┌──────────────────────────────────────────┐  │
    │  │  Inbound → KPIDefinition → Outbound      │  │
    │  └──────────────────────────────────────────┘  │
    └────────────────────────────────────────────────┘
```

---

## 1. Converter API Endpoints

### Base Path: `/api/converters`

All endpoints support multi-tenant isolation via group context.

---

### 1.1 Conversion History

Track and analyze conversion operations for audit trails and analytics.

#### Create History Entry
```http
POST /api/converters/history
Content-Type: application/json

{
  "source_format": "powerbi",
  "target_format": "dax",
  "execution_id": "crew_run_12345",
  "status": "success",
  "input_data": {
    "semantic_model_id": "abc-123",
    "measure_count": 15
  },
  "output_data": {
    "measures_generated": 15,
    "output_format": "dax"
  },
  "execution_time_seconds": 3.5
}
```

**Response:**
```json
{
  "id": 1,
  "source_format": "powerbi",
  "target_format": "dax",
  "status": "success",
  "execution_id": "crew_run_12345",
  "created_at": "2025-12-04T10:30:00Z",
  "execution_time_seconds": 3.5
}
```

#### Get History Entry
```http
GET /api/converters/history/{history_id}
```

#### List History with Filters
```http
GET /api/converters/history?source_format=powerbi&target_format=dax&limit=50&offset=0
```

**Query Parameters:**
- `source_format`: Filter by source (powerbi, yaml, tableau, etc.)
- `target_format`: Filter by target (dax, sql, uc_metrics, yaml)
- `status`: Filter by status (pending, success, failed)
- `execution_id`: Filter by specific crew execution
- `limit`: Number of results (1-1000, default: 100)
- `offset`: Pagination offset

#### Get Statistics
```http
GET /api/converters/history/statistics?days=30
```

**Response:**
```json
{
  "total_conversions": 145,
  "successful_conversions": 138,
  "failed_conversions": 7,
  "success_rate": 95.17,
  "average_execution_time": 2.8,
  "popular_conversion_paths": [
    {"from": "powerbi", "to": "sql", "count": 65},
    {"from": "yaml", "to": "dax", "count": 42}
  ]
}
```

---

### 1.2 Conversion Jobs

Manage async conversion jobs for long-running operations.

#### Create Job
```http
POST /api/converters/jobs
Content-Type: application/json

{
  "job_id": "conv_job_abc123",
  "source_format": "powerbi",
  "target_format": "sql",
  "status": "pending",
  "configuration": {
    "semantic_model_id": "dataset-123",
    "sql_dialect": "databricks"
  }
}
```

#### Get Job Status
```http
GET /api/converters/jobs/{job_id}
```

**Response:**
```json
{
  "job_id": "conv_job_abc123",
  "status": "running",
  "progress_percentage": 45,
  "current_step": "extracting_measures",
  "started_at": "2025-12-04T10:30:00Z",
  "result_data": null
}
```

#### Update Job Status (for workers)
```http
PATCH /api/converters/jobs/{job_id}/status
Content-Type: application/json

{
  "status": "completed",
  "progress_percentage": 100,
  "result_data": {
    "measures_converted": 25,
    "output_location": "s3://bucket/result.sql"
  }
}
```

#### List Jobs
```http
GET /api/converters/jobs?status=running&limit=50
```

#### Cancel Job
```http
POST /api/converters/jobs/{job_id}/cancel
```

---

### 1.3 Saved Configurations

Save and reuse converter configurations.

#### Create Configuration
```http
POST /api/converters/configs
Content-Type: application/json

{
  "name": "PowerBI to Databricks SQL",
  "source_format": "powerbi",
  "target_format": "sql",
  "configuration": {
    "sql_dialect": "databricks",
    "include_comments": true,
    "process_structures": true
  },
  "is_public": false,
  "is_template": false
}
```

#### Get Configuration
```http
GET /api/converters/configs/{config_id}
```

#### List Configurations
```http
GET /api/converters/configs?source_format=powerbi&is_public=true&limit=50
```

**Query Parameters:**
- `source_format`: Filter by source format
- `target_format`: Filter by target format
- `is_public`: Show public/shared configs
- `is_template`: Show system templates
- `search`: Search in configuration names

#### Use Configuration (track usage)
```http
POST /api/converters/configs/{config_id}/use
```

#### Update Configuration
```http
PATCH /api/converters/configs/{config_id}
Content-Type: application/json

{
  "name": "Updated Name",
  "configuration": {
    "sql_dialect": "postgresql"
  }
}
```

#### Delete Configuration
```http
DELETE /api/converters/configs/{config_id}
```

---

### 1.4 Health Check

```http
GET /api/converters/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "converter",
  "version": "1.0.0"
}
```

---

## 2. CrewAI Converter Tools

Use these tools within AI agent crews for intelligent measure conversions.

### 2.1 Measure Conversion Pipeline Tool

**Universal converter for any source → any target format**

#### Tool Name
`Measure Conversion Pipeline`

#### Capabilities
- **Inbound**: Power BI, YAML (future: Tableau, Excel, Looker)
- **Outbound**: DAX, SQL (7 dialects), UC Metrics, YAML

#### Configuration Example (in Crew JSON)
```json
{
  "crew": {
    "name": "Data Migration Crew",
    "agents": [
      {
        "role": "Data Migration Specialist",
        "goal": "Convert Power BI measures to Databricks SQL",
        "tools": [
          {
            "name": "Measure Conversion Pipeline",
            "enabled": true
          }
        ]
      }
    ]
  }
}
```

#### Tool Parameters

**Inbound Selection:**
```python
{
  "inbound_connector": "powerbi",  # or "yaml"
}
```

**Power BI Configuration:**
```python
{
  "inbound_connector": "powerbi",
  "powerbi_semantic_model_id": "abc-123-def",
  "powerbi_group_id": "workspace-456",
  "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
  "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
  "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
  "powerbi_info_table_name": "Info Measures",  # optional
  "powerbi_include_hidden": False,  # optional
  "powerbi_filter_pattern": "^Sales.*"  # optional regex
}
```

**YAML Configuration:**
```python
{
  "inbound_connector": "yaml",
  "yaml_content": "kpis:\n  - name: Total Sales\n    ...",  # OR
  "yaml_file_path": "/path/to/measures.yaml"
}
```

**Outbound Selection:**
```python
{
  "outbound_format": "sql"  # "dax", "sql", "uc_metrics", "yaml"
}
```

**SQL Configuration:**
```python
{
  "outbound_format": "sql",
  "sql_dialect": "databricks",  # databricks, postgresql, mysql, sqlserver, snowflake, bigquery, standard
  "sql_include_comments": True,
  "sql_process_structures": True
}
```

**UC Metrics Configuration:**
```python
{
  "outbound_format": "uc_metrics",
  "uc_catalog": "main",
  "uc_schema": "default",
  "uc_process_structures": True
}
```

**DAX Configuration:**
```python
{
  "outbound_format": "dax",
  "dax_process_structures": True
}
```

---

### 2.2 Specialized YAML Tools

For YAML-specific conversions with detailed control.

#### YAML to DAX Tool
```json
{
  "name": "YAML to DAX Converter",
  "parameters": {
    "yaml_content": "...",  # OR yaml_file_path
    "process_structures": true
  }
}
```

#### YAML to SQL Tool
```json
{
  "name": "YAML to SQL Converter",
  "parameters": {
    "yaml_content": "...",
    "dialect": "databricks",
    "include_comments": true,
    "process_structures": true
  }
}
```

#### YAML to UC Metrics Tool
```json
{
  "name": "YAML to Unity Catalog Metrics Converter",
  "parameters": {
    "yaml_content": "...",
    "catalog": "main",
    "schema_name": "default",
    "process_structures": true
  }
}
```

---

### 2.3 Power BI Connector Tool

Direct Power BI dataset access for measure extraction.

```json
{
  "name": "Power BI Connector",
  "parameters": {
    "semantic_model_id": "dataset-abc-123",
    "group_id": "workspace-def-456",
    "access_token": "Bearer eyJ...",
    "info_table_name": "Info Measures",
    "include_hidden": false,
    "filter_pattern": "^Revenue.*"
  }
}
```

---

## 3. Integration Patterns

### 3.1 Standalone API Usage

Direct HTTP calls for programmatic access.

**Example: Python client**
```python
import requests

# Base URL
BASE_URL = "https://your-app.databricks.com/api/converters"

# Create conversion history
response = requests.post(
    f"{BASE_URL}/history",
    json={
        "source_format": "powerbi",
        "target_format": "sql",
        "execution_id": "manual_run_001",
        "status": "success",
        "execution_time_seconds": 2.5
    },
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

history_entry = response.json()
print(f"Created history entry: {history_entry['id']}")

# List all PowerBI → SQL conversions
response = requests.get(
    f"{BASE_URL}/history",
    params={
        "source_format": "powerbi",
        "target_format": "sql",
        "limit": 10
    },
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

conversions = response.json()
print(f"Found {conversions['total']} conversions")
```

---

### 3.2 Crew-Based Usage

Use converter tools within AI agent workflows.

**Example: Create a crew with converter tools**

```python
# Step 1: Create crew configuration with converter tools
crew_config = {
    "name": "Power BI Migration Crew",
    "agents": [
        {
            "role": "Data Analyst",
            "goal": "Extract and analyze Power BI measures",
            "tools": ["Measure Conversion Pipeline", "Power BI Connector"]
        },
        {
            "role": "SQL Developer",
            "goal": "Convert measures to SQL format",
            "tools": ["Measure Conversion Pipeline"]
        }
    ],
    "tasks": [
        {
            "description": "Extract all measures from Power BI dataset abc-123",
            "agent": "Data Analyst"
        },
        {
            "description": "Convert extracted measures to Databricks SQL format",
            "agent": "SQL Developer"
        }
    ]
}

# Step 2: Create crew via API
import requests
response = requests.post(
    "https://your-app.databricks.com/api/v1/crews",
    json=crew_config,
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
crew = response.json()

# Step 3: Execute crew
response = requests.post(
    f"https://your-app.databricks.com/api/v1/crews/{crew['id']}/execute",
    json={
        "inputs": {
            "powerbi_semantic_model_id": "abc-123",
            "powerbi_group_id": "workspace-456",
            "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
            "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
            "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
            "sql_dialect": "databricks"
        }
    },
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
execution = response.json()

# Step 4: Monitor execution
response = requests.get(
    f"https://your-app.databricks.com/api/v1/crews/executions/{execution['id']}",
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
status = response.json()
print(f"Crew status: {status['status']}")

# Step 5: View conversion history (automatic tracking)
response = requests.get(
    f"https://your-app.databricks.com/api/converters/history",
    params={"execution_id": execution['id']},
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
history = response.json()
print(f"Conversions performed: {history['total']}")
```

---

### 3.3 Combined Pattern: Crews + API Management

**Best practice for production deployments**

```python
# 1. Create reusable saved configuration
config_response = requests.post(
    f"{BASE_URL}/configs",
    json={
        "name": "Standard PowerBI to SQL Migration",
        "source_format": "powerbi",
        "target_format": "sql",
        "configuration": {
            "sql_dialect": "databricks",
            "include_comments": True,
            "process_structures": True
        },
        "is_template": True
    }
)
config_id = config_response.json()["id"]

# 2. Create crew that uses this configuration
crew_config = {
    "name": "Migration Crew",
    "agents": [{
        "role": "Migration Agent",
        "tools": ["Measure Conversion Pipeline"]
    }],
    "tasks": [{
        "description": f"Use saved config {config_id} to convert measures"
    }]
}

# 3. Execute crew
crew_response = requests.post(f"{CREWS_URL}", json=crew_config)
crew_id = crew_response.json()["id"]

# 4. Run execution
exec_response = requests.post(
    f"{CREWS_URL}/{crew_id}/execute",
    json={"inputs": {"config_id": config_id}}
)
execution_id = exec_response.json()["id"]

# 5. Query conversion history filtered by this execution
history = requests.get(
    f"{BASE_URL}/history",
    params={"execution_id": execution_id}
).json()

# 6. Get statistics
stats = requests.get(
    f"{BASE_URL}/history/statistics",
    params={"days": 7}
).json()
print(f"Success rate: {stats['success_rate']}%")
```

---

## 4. Common Workflows

### 4.1 Power BI → Databricks SQL Migration

**Using Crew:**
```python
crew_execution = {
    "crew_name": "PowerBI Migration",
    "inputs": {
        "inbound_connector": "powerbi",
        "powerbi_semantic_model_id": "abc-123",
        "powerbi_group_id": "workspace-456",
        "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
            "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
            "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
        "outbound_format": "sql",
        "sql_dialect": "databricks"
    }
}
```

**Direct API (track result):**
```python
# Execute conversion (via tool or direct converter)
# ... conversion happens ...

# Track in history
requests.post(f"{BASE_URL}/history", json={
    "source_format": "powerbi",
    "target_format": "sql",
    "status": "success",
    "execution_time_seconds": 5.2,
    "input_data": {"model_id": "abc-123"},
    "output_data": {"sql_queries": 15}
})
```

---

### 4.2 YAML → Multiple Formats

**Generate DAX, SQL, and UC Metrics from YAML:**

```python
yaml_definition = """
kpis:
  - name: Total Sales
    formula: SUM(Sales[Amount])
    aggregation_type: SUM
"""

# Use crew with multiple conversions
crew_config = {
    "agents": [{
        "role": "Format Converter",
        "tools": [
            "YAML to DAX Converter",
            "YAML to SQL Converter",
            "YAML to Unity Catalog Metrics Converter"
        ]
    }],
    "tasks": [
        {"description": "Convert YAML to DAX format"},
        {"description": "Convert YAML to Databricks SQL"},
        {"description": "Convert YAML to UC Metrics Store format"}
    ]
}
```

---

### 4.3 Bulk Migration with Job Tracking

```python
# Create job
job = requests.post(f"{BASE_URL}/jobs", json={
    "job_id": "bulk_migration_001",
    "source_format": "powerbi",
    "target_format": "sql",
    "status": "pending",
    "configuration": {
        "models": ["model1", "model2", "model3"]
    }
}).json()

# Execute crew with job tracking
crew_execution = requests.post(f"{CREWS_URL}/execute", json={
    "job_id": job["job_id"],
    "inputs": {...}
})

# Poll job status
while True:
    job_status = requests.get(f"{BASE_URL}/jobs/{job['job_id']}").json()
    print(f"Progress: {job_status['progress_percentage']}%")
    if job_status["status"] in ["completed", "failed"]:
        break
    time.sleep(2)
```

---

## 5. Best Practices

### 5.1 Error Handling

**Always track conversion outcomes:**
```python
try:
    # Execute conversion
    result = convert_measures(...)

    # Track success
    requests.post(f"{BASE_URL}/history", json={
        "status": "success",
        "execution_time_seconds": elapsed_time,
        "output_data": result
    })
except Exception as e:
    # Track failure
    requests.post(f"{BASE_URL}/history", json={
        "status": "failed",
        "error_message": str(e),
        "execution_time_seconds": elapsed_time
    })
```

### 5.2 Configuration Management

**Use saved configurations for consistency:**
```python
# Create once
config = requests.post(f"{BASE_URL}/configs", json={
    "name": "Standard Migration Config",
    "source_format": "powerbi",
    "target_format": "sql",
    "configuration": {...},
    "is_template": True
})

# Reuse many times
for dataset_id in datasets:
    crew_execution = execute_crew({
        "config_id": config["id"],
        "dataset_id": dataset_id
    })
```

### 5.3 Analytics and Monitoring

**Regularly check conversion statistics:**
```python
# Weekly review
stats = requests.get(f"{BASE_URL}/history/statistics?days=7").json()
print(f"Success rate: {stats['success_rate']}%")
print(f"Avg time: {stats['average_execution_time']}s")

# Popular paths
for path in stats["popular_conversion_paths"]:
    print(f"{path['from']} → {path['to']}: {path['count']} conversions")
```

---

## 6. Authentication

All endpoints require authentication via JWT token or Databricks OAuth.

```python
headers = {
    "Authorization": "Bearer YOUR_TOKEN",
    "Content-Type": "application/json"
}

response = requests.get(f"{BASE_URL}/history", headers=headers)
```

For Databricks Apps, authentication is handled automatically via OBO (On-Behalf-Of) tokens.

---

## 7. Rate Limits and Quotas

- **API Endpoints**: 1000 requests/hour per user
- **Crew Executions**: 100 concurrent executions per group
- **Job Duration**: 30 minutes max per job

---

## 8. Support and Troubleshooting

### Common Issues

**1. Conversion fails with authentication error:**
- Check Power BI access token validity
- Ensure token has dataset read permissions

**2. Crew doesn't use converter tools:**
- Verify tool is enabled in agent configuration
- Check tool name matches exactly

**3. History not showing conversions:**
- Ensure `execution_id` is passed correctly
- Check group context for multi-tenant isolation

### Getting Help

- **API Reference**: `/docs` (Swagger UI)
- **Health Check**: `GET /api/converters/health`
- **Logs**: Check application logs for detailed error messages

---

## 9. Migration Guide

### From Legacy API to New Converter API

**Old approach:**
```python
# Legacy: Custom conversion code
converter = PowerBIConverter(token)
measures = converter.extract_measures(model_id)
sql = converter.to_sql(measures)
```

**New approach:**
```python
# New: Use Measure Conversion Pipeline Tool in crew
crew_execution = execute_crew({
    "tools": ["Measure Conversion Pipeline"],
    "inputs": {
        "inbound_connector": "powerbi",
        "powerbi_semantic_model_id": model_id,
        "outbound_format": "sql"
    }
})

# Track in history automatically
history = requests.get(f"{BASE_URL}/history?execution_id={crew_execution['id']}")
```

---

## 10. Complete Example: End-to-End Workflow

```python
import requests
import time

BASE_URL = "https://your-app.databricks.com"
CONVERTER_API = f"{BASE_URL}/api/converters"
CREWS_API = f"{BASE_URL}/api/v1/crews"

# 1. Create saved configuration for reuse
config = requests.post(f"{CONVERTER_API}/configs", json={
    "name": "PowerBI to Databricks Migration",
    "source_format": "powerbi",
    "target_format": "sql",
    "configuration": {
        "sql_dialect": "databricks",
        "include_comments": True
    }
}).json()

# 2. Create crew with converter tools
crew = requests.post(CREWS_API, json={
    "name": "Migration Crew",
    "agents": [{
        "role": "Migration Specialist",
        "goal": "Convert Power BI measures to SQL",
        "tools": ["Measure Conversion Pipeline"]
    }],
    "tasks": [{
        "description": "Convert all measures from Power BI to SQL format"
    }]
}).json()

# 3. Execute crew with config
execution = requests.post(f"{CREWS_API}/{crew['id']}/execute", json={
    "inputs": {
        "inbound_connector": "powerbi",
        "powerbi_semantic_model_id": "your-model-id",
        "powerbi_group_id": "your-workspace-id",
        "powerbi_tenant_id": "<YOUR_AZURE_TENANT_ID>",
        "powerbi_client_id": "<YOUR_AZURE_CLIENT_ID>",
        "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
        "outbound_format": "sql",
        "sql_dialect": "databricks"
    }
}).json()

# 4. Monitor execution
while True:
    status = requests.get(f"{CREWS_API}/executions/{execution['id']}").json()
    print(f"Status: {status['status']}")
    if status["status"] in ["completed", "failed"]:
        break
    time.sleep(2)

# 5. View conversion history
history = requests.get(
    f"{CONVERTER_API}/history",
    params={"execution_id": execution["id"]}
).json()

print(f"Conversions performed: {history['total']}")
for item in history["items"]:
    print(f"  - {item['source_format']} → {item['target_format']}: {item['status']}")

# 6. Get analytics
stats = requests.get(f"{CONVERTER_API}/history/statistics?days=1").json()
print(f"Success rate: {stats['success_rate']}%")
print(f"Average execution time: {stats['average_execution_time']}s")

# 7. Track config usage
requests.post(f"{CONVERTER_API}/configs/{config['id']}/use")
```

---

## Summary

**Converter API provides:**
- ✅ Conversion history tracking and analytics
- ✅ Job management for long-running operations
- ✅ Saved configurations for reusability
- ✅ Multi-tenant isolation

**CrewAI Tools provide:**
- ✅ Intelligent agent-based conversions
- ✅ Universal measure conversion pipeline
- ✅ Specialized format converters
- ✅ Direct Power BI connector

**Together they enable:**
- ✅ Tracked crew executions with conversion history
- ✅ Reusable configurations across crews
- ✅ Analytics on conversion patterns
- ✅ Production-ready measure migration workflows
