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

# Step 3: Configuration of the deployed items
- From the uploaded notebook (src/backend/src/engines/crewai/tools/templates/notebooks/powerbi_full_pipeline.py) create a job
- This job will have ONE task (taskname is SUPER important): pbi_e2e_pipeline & link the notebook from above to the task
- Feel free to edit, if needed, default variables. However, we suggest to refrain from this
- This job will give you a job-ID --> copy this and put it into the configuration of the Kasal-app you have running for the PowerBI Tool, which you have to enable
- As Databricks-host please enter your host-IP address WITHOUT the https
- Don't forget to set the Databricks-API key in the the configuration plane

## Step 3: Frontend UI testing

As a reference this is one example of a crew setup that you can upload in the frontend: 
ATTENTION: In the front-end you need to make sure that the parameters set in the task match YOUR environment (aka your semantic_model_id, workspace_id, etc.). In general as authentication methodology we suggest service_principal, but you need to have this configured within your environment, thus default is device-control-flow, but please note this is nice for experiments, but NOT production ready then as it will need interactice authentication. 

Please note that if the job-fails it most likely has to do with the access - simply add the SVP from the app you spun up as a manager of your job-notebook, which should fix the issue.

```json
{
  "id": "3980587e-5a1e-44b3-a264-1dc11feb72f2",
  "name": "test",
  "agent_ids": [
    "4379d037-4a1b-4101-bb19-18f8de1be668"
  ],
  "task_ids": [
    "97b2f77c-f5a1-49ac-9e98-31f1b0c13c8f"
  ],
  "nodes": [
    {
      "id": "agent-4379d037-4a1b-4101-bb19-18f8de1be668",
      "type": "agentNode",
      "position": {
        "x": 63.96047192664271,
        "y": -341.6588050410664
      },
      "data": {
        "label": "PowerBI Job Orchestrator",
        "role": "PowerBI Job Manager",
        "goal": "Orchestrate and manage PowerBI jobs efficiently\n\nYou will make sure that the action run will only trigger 1 time and not more. ",
        "backstory": "Experienced in managing and optimizing Databricks workflows for PowerBI, with expertise in job scheduling and execution.",
        "tools": [
          "71"
        ],
        "agentId": "4379d037-4a1b-4101-bb19-18f8de1be668",
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
      "id": "task-97b2f77c-f5a1-49ac-9e98-31f1b0c13c8f",
      "type": "taskNode",
      "position": {
        "x": 303.15458030615144,
        "y": -338.9256934052662
      },
      "data": {
        "label": "Run Job with Custom Parameters",
        "role": null,
        "goal": null,
        "backstory": null,
        "tools": [
          "71"
        ],
        "agentId": null,
        "taskId": "97b2f77c-f5a1-49ac-9e98-31f1b0c13c8f",
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
        "description": "Execute job ID 365257288725339 ONE TIME ONLY. Do not retry if you receive a successful run_id. Execute the job with those parameters:\n- question: {question}\n- workspace_id: 'bcb084ed-f8c9-422c-b148-29839c0f9227'\n- semantic_model_id: 'a17de62e-8dc0-4a8a-acaa-2a9954de8c75'\n- auth_method: 'device_code'\n- tenant_id: '9f37a392-f0ae-4280-9796-f1864a10effc'\n- client_id: '1950a258-227b-4e31-a9cf-717495945fc2'\"\n- client_secret: 'TBD'\n- sample_size: 100\n- metadata: \"json\"\n- databricks_host: \"https://e2-demo-field-eng.cloud.databricks.com/\"\n- databricks_token: <YOUR_DATABRICKS_API_TOKEN>\n\nI don't want you to list or get the job; I want you to run it once; you are not allowed to run more than once. Use PowerBITool to execute this query.\n\nIMPORTANT: You will make sure that the action run will only trigger 1 time and not more.",
        "expected_output": "A job execution result containing the response data from running job ID 365257288725339 with the custom parameters. The output will include any result_data and various other parameters.",
        "icon": null,
        "advanced_config": null,
        "config": {
          "cache_response": false,
          "cache_ttl": 3600,
          "retry_on_fail": false,
          "max_retries": 0,
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
      "source": "agent-4379d037-4a1b-4101-bb19-18f8de1be668",
      "target": "task-97b2f77c-f5a1-49ac-9e98-31f1b0c13c8f",
      "id": "reactflow__edge-agent-83ec5a5f-7b9a-46ea-b12d-63bfaad2d9d0-task-ebd1eaaf-e42b-4e36-bb5b-b486be841bf2-default-default",
      "sourceHandle": null,
      "targetHandle": null
    }
  ],
  "created_at": "2025-10-16T18:39:31.203804",
  "updated_at": "2025-10-16T18:39:31.203806"
}
```

Exectue the crew and set these parameters: 
- question: Generate a DAX query to calculate Net Sales Revenue (NSR) and Cost Of Goods Sold (COGS) per product
- You can even think of ingesting the semantic_model_id dynamically as only the question and this one will change

**Expected Response:**
```json
{
  "success": true,
  "run_id": 1086410411823064,
  "question": "Generate a DAX query to calculate Net Sales Revenue (NSR) and Cost Of Goods Sold (COGS) per product",
  "elapsed_seconds": 81.7,
  "dax_query": null,
  "result_data": [
    {
      "TestData[product]": "product_a",
      "[Net Sales Revenue]": 20892722.53,
      "[Cost Of Goods Sold]": 10275850.23
    },
    {
      "TestData[product]": "product_b",
      "[Net Sales Revenue]": 20705392.63,
      "[Cost Of Goods Sold]": 10425699.56
    },
    {
      "TestData[product]": "product_c",
      "[Net Sales Revenue]": 18076406.91,
      "[Cost Of Goods Sold]": 8797758.1
    }
  ],
  "message": "Successfully executed Power BI query in 81.7s"
}
```

## Summary

- **Tool Level**: Configure XMLA endpoint once (connection details)
- **Runtime Level**: Agent provides dataset_name and metadata when calling tool
- **Result**: Maximum flexibility without needing to update crew configurations
- **Pattern**: Same approach as DatabricksJobsTool's job_params

This approach gives agents the power to work with any Power BI dataset by providing the metadata at runtime, rather than requiring pre-configuration in the crew JSON or database.
