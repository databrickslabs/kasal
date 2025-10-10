# Power BI DAX Generator - Quick Start Guide

This guide will help you get started with the Power BI DAX Generator in under 5 minutes. This guide explains how to use the PowerBITool with runtime parameters, similar to DatabricksJobsTool's `job_params` pattern.

## Overview

The PowerBITool now supports passing dataset metadata and configuration as **runtime parameters** when the agent calls the tool, rather than requiring pre-configuration in the tool settings or task configuration.

This provides maximum flexibility:
- ✅ Different datasets can be queried without changing tool configuration
- ✅ Metadata can be dynamically generated or retrieved at runtime
- ✅ Same tool instance can work with multiple Power BI datasets
- ✅ No need to update crew JSON or database for new datasets

## Prerequisites

- Kasal backend running
- Access to a Power BI workspace with XMLA endpoint enabled
- Power BI dataset metadata (tables, columns, relationships)

## Requirements
- Python 3.9+
- Node.js 18+
- Postgres (recommended) or SQLite for local dev
- Databricks access if exercising Databricks features

## Step 1: Local tests
```bash
# Backend
cd src/backend
python -m venv .venv && source .venv/bin/activate
pip install -r ../requirements.txt
./run.sh  # http://localhost:8000 (OpenAPI at /api-docs if enabled)

# Frontend
cd ../frontend
npm install
npm start  # http://localhost:3000
```

Health check:
```bash
curl http://localhost:8000/health
# {"status":"healthy"}
```

## Step 2: Deploy semi-automatically

Create static frontend:
```bash
# Within the src dir
python3 build.py
```

Deploy the app:
```bash
# Within the src dir
python3 deploy.py --app-name kasal-david --user-name david.schwarzenbacher@databricks.com
```

## Step 3: Frontend UI testing

1. Navigate to Tools section in the Kasal UI
2. Find "PowerBITool" (ID: 71)
3. Click Edit/Configure
4. Update the configuration JSON with your settings
5. Save changes
6. Create a crew like this

```json
{
  "id": "b3d302c1-7c3c-47df-8b81-bf04795c935d",
  "name": "test",
  "agent_ids": [
    "17efaadb-6c8f-45c2-b264-ebc1a17ac7f5"
  ],
  "task_ids": [
    "9dc49287-b6bd-443c-b0eb-3fd1fc4996a9"
  ],
  "nodes": [
    {
      "id": "agent-17efaadb-6c8f-45c2-b264-ebc1a17ac7f5",
      "type": "agentNode",
      "position": {
        "x": 100,
        "y": 200
      },
      "data": {
        "label": "BI Analyst",
        "role": "Business Intelligence Analyst",
        "goal": "Generate accurate DAX queries for Power BI datasets",
        "backstory": "Expert in Power BI and DAX with 10+ years of experience. You understand how to provide dataset metadata at runtime when using the Power BI DAX Generator tool. When asked to query a Power BI dataset, you provide the dataset_name and a complete metadata structure with tables and columns.",
        "tools": [
          "71"
        ],
        "agentId": "17efaadb-6c8f-45c2-b264-ebc1a17ac7f5",
        "taskId": null,
        "llm": "databricks-llama-4-maverick",
        "function_calling_llm": null,
        "max_iter": 25,
        "max_rpm": 1,
        "max_execution_time": 300,
        "verbose": false,
        "allow_delegation": false,
        "cache": true,
        "memory": true,
        "embedder_config": {
          "provider": "databricks",
          "config": {
            "model": "databricks-gte-large-en"
          }
        },
        "system_template": null,
        "prompt_template": null,
        "response_template": null,
        "allow_code_execution": false,
        "code_execution_mode": "safe",
        "max_retry_limit": 3,
        "use_system_prompt": true,
        "respect_context_window": true,
        "type": "agent",
        "description": null,
        "expected_output": null,
        "icon": null,
        "advanced_config": null,
        "config": null,
        "context": [],
        "async_execution": false,
        "knowledge_sources": [],
        "markdown": false
      },
      "width": null,
      "height": null,
      "selected": null,
      "positionAbsolute": null,
      "dragging": null,
      "style": null
    },
    {
      "id": "task-9dc49287-b6bd-443c-b0eb-3fd1fc4996a9",
      "type": "taskNode",
      "position": {
        "x": 400,
        "y": 199.2962962962963
      },
      "data": {
        "label": "NSR per Product Analysis",
        "role": null,
        "goal": null,
        "backstory": null,
        "tools": [
          "71"
        ],
        "agentId": null,
        "taskId": "9dc49287-b6bd-443c-b0eb-3fd1fc4996a9",
        "llm": null,
        "function_calling_llm": null,
        "max_iter": null,
        "max_rpm": null,
        "max_execution_time": null,
        "verbose": null,
        "allow_delegation": null,
        "cache": null,
        "memory": true,
        "embedder_config": null,
        "system_template": null,
        "prompt_template": null,
        "response_template": null,
        "allow_code_execution": null,
        "code_execution_mode": null,
        "max_retry_limit": null,
        "use_system_prompt": null,
        "respect_context_window": null,
        "type": "task",
        "description": "Generate a DaAX query to calculate Net Sales Revenue (NSR) per product from the test_pbi dataset. When using the Power BI DAX Generator tool, execute and ask for these parameters:\n\n1. dataset_name: {dataset_name}\n2. metadata: {metadata}\n\nThe tool will generate a DAX EVALUATE statement that groups by product and sums the NSR values.",
        "expected_output": "A complete, executable DAX EVALUATE statement that calculates total NSR grouped by product. The query should be ready to run against the Power BI dataset via XMLA endpoint.",
        "icon": null,
        "advanced_config": null,
        "config": {
          "cache_response": false,
          "cache_ttl": 3600,
          "retry_on_fail": true,
          "max_retries": 3,
          "timeout": null,
          "priority": 1,
          "error_handling": "default",
          "output_file": null,
          "output_json": null,
          "output_pydantic": null,
          "validation_function": null,
          "callback_function": null,
          "human_input": false,
          "markdown": false
        },
        "context": [],
        "async_execution": false,
        "knowledge_sources": null,
        "markdown": false
      },
      "width": null,
      "height": null,
      "selected": null,
      "positionAbsolute": null,
      "dragging": null,
      "style": null
    }
  ],
  "edges": [
    {
      "source": "agent-17efaadb-6c8f-45c2-b264-ebc1a17ac7f5",
      "target": "task-9dc49287-b6bd-443c-b0eb-3fd1fc4996a9",
      "id": "edge-1",
      "sourceHandle": null,
      "targetHandle": null
    }
  ],
  "created_at": "2025-10-10T07:50:13.155055",
  "updated_at": "2025-10-10T07:50:13.155057"
}
```

7. Exectue the crew and set these parameters: 
- dataset_name: test_pbi
- metadata: {'tables': [{'name': 'TestData', 'columns': [{'name': 'product', 'data_type': 'string'}, {'name': 'nsr', 'data_type': 'decimal'}, {'name': 'country', 'data_type': 'string'}]}]}

**Expected Response:**
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'TestData'[product],
    "Total NSR", SUM('TestData'[nsr])
)
```

## Summary

- **Tool Level**: Configure XMLA endpoint once (connection details)
- **Runtime Level**: Agent provides dataset_name and metadata when calling tool
- **Result**: Maximum flexibility without needing to update crew configurations
- **Pattern**: Same approach as DatabricksJobsTool's job_params

This approach gives agents the power to work with any Power BI dataset by providing the metadata at runtime, rather than requiring pre-configuration in the crew JSON or database.
