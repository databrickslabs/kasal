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
6. Create a crew like this (check all the parameters like job-id and co)

```json
{
  "id": "e890bf60-dbfa-4ebc-a833-9640d3a44037",
  "name": "e2e_pbi_crew",
  "agent_ids": [
    "7daf0c19-692a-4be3-bc14-c84cc9f0239e",
    "64efa87e-d8e7-49df-9097-733ea7135f43"
  ],
  "task_ids": [
    "189ee792-8e83-40e1-9c84-abd52bbb7b18",
    "e8b2232a-af7f-451b-a89a-072253df18b7"
  ],
  "nodes": [
    {
      "id": "agent-7daf0c19-692a-4be3-bc14-c84cc9f0239e",
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
        "agentId": "7daf0c19-692a-4be3-bc14-c84cc9f0239e",
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
      "id": "task-189ee792-8e83-40e1-9c84-abd52bbb7b18",
      "type": "taskNode",
      "position": {
        "x": 374.4179408025376,
        "y": 198.6230842121526
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
        "taskId": "189ee792-8e83-40e1-9c84-abd52bbb7b18",
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
        "description": "Generate a DAX query to calculate Net Sales Revenue (NSR) and Cost Of Goods Sold (COGS) per product from the test_pbi dataset. When using the Power BI DAX Generator tool, execute and ask for these parameters:\n\n1. dataset_name: {dataset_name}\n2. metadata: {metadata}\n\nThe tool will generate a DAX EVALUATE statement that groups by product and sums the NSR and COGS values as two separate columns.",
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
    },
    {
      "id": "agent-64efa87e-d8e7-49df-9097-733ea7135f43",
      "type": "agentNode",
      "position": {
        "x": 98.44294056195827,
        "y": 447.4736022841256
      },
      "data": {
        "label": "Databricks Job Orchestrator",
        "role": "Databricks Job Manager",
        "goal": "Orchestrate and manage Databricks jobs efficiently\n\nYou will make sure that the action run will only trigger 1 time and not more. ",
        "backstory": "Experienced in managing and optimizing Databricks workflows, with expertise in job scheduling and execution.",
        "tools": [],
        "agentId": "64efa87e-d8e7-49df-9097-733ea7135f43",
        "taskId": null,
        "llm": "databricks-llama-4-maverick",
        "function_calling_llm": null,
        "max_iter": 25,
        "max_rpm": 300,
        "max_execution_time": 300,
        "verbose": false,
        "allow_delegation": false,
        "cache": false,
        "memory": false,
        "embedder_config": {
          "provider": "openai",
          "config": {
            "model": "text-embedding-3-small"
          }
        },
        "system_template": null,
        "prompt_template": null,
        "response_template": null,
        "allow_code_execution": false,
        "code_execution_mode": "safe",
        "max_retry_limit": 2,
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
      "id": "task-e8b2232a-af7f-451b-a89a-072253df18b7",
      "type": "taskNode",
      "position": {
        "x": 343.2037683825747,
        "y": 446.5243311337546
      },
      "data": {
        "label": "Run Job with Custom City and Query Parameters",
        "role": null,
        "goal": null,
        "backstory": null,
        "tools": [
          "70"
        ],
        "agentId": null,
        "taskId": "e8b2232a-af7f-451b-a89a-072253df18b7",
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
        "description": "Execute job ID 260124535998145 with the dax_statement you get from the Business Intelligence analyst as a job_params that is passed to the job.\n\nIn addition to this, set workspace_id to \"bcb084ed-f8c9-422c-b148-29839c0f9227\" and semantic_model_id to \"a17de62e-8dc0-4a8a-acaa-2a9954de8c75\" and add them as additional job_params.\n\nNot I don't want you to list or get the job I want you to only run it but only once you are not allowed to run more than once.\n\nExample of a dax_statement that you might receive is e.g. EVALUATE\nSUMMARIZECOLUMNS(\n    'TestData'[product],\n    \"Total NSR\", SUM('TestData'[nsr])\n)\n\nYou need to replace this dax_statement with the one coming from the Business Intelligence Analyst.\n\n You will make sure that the action run will only trigger 1 time and not more.",
        "expected_output": "A job execution result containing the response data from running job ID 260124535998145 with the custom city and query parameters. The output will include any data returned by the job, execution status, and timestamps.",
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
      "source": "agent-7daf0c19-692a-4be3-bc14-c84cc9f0239e",
      "target": "task-189ee792-8e83-40e1-9c84-abd52bbb7b18",
      "id": "edge-1",
      "sourceHandle": null,
      "targetHandle": null
    },
    {
      "source": "agent-64efa87e-d8e7-49df-9097-733ea7135f43",
      "target": "task-e8b2232a-af7f-451b-a89a-072253df18b7",
      "id": "reactflow__edge-agent-a803dae4-22d3-4e74-a931-aa7ace598563-task-207293d2-a820-47b4-87e4-fe51a52bb060-default-default",
      "sourceHandle": null,
      "targetHandle": null
    },
    {
      "source": "task-189ee792-8e83-40e1-9c84-abd52bbb7b18",
      "target": "task-e8b2232a-af7f-451b-a89a-072253df18b7",
      "id": "reactflow__edge-task-f2c964e3-461f-4556-b269-53894a73f910-task-207293d2-a820-47b4-87e4-fe51a52bb060-default-default",
      "sourceHandle": null,
      "targetHandle": null
    }
  ],
  "created_at": "2025-10-13T05:03:46.986254",
  "updated_at": "2025-10-13T05:03:46.986256"
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
