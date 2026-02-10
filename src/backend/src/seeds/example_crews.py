"""
Example Crews Seeder

Provides pre-configured example crews for the Measure Conversion Pipeline.
These serve as templates that users can import and customize.
"""

import uuid
from typing import Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory
from src.models.crew import Crew
from src.models.agent import Agent
from src.models.task import Task
from src.core.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Example Crew 1: Dynamic Parameter Mode
# ============================================================================
# This crew uses placeholder parameters that are resolved at execution time.
# Ideal for external applications that pass credentials via execution inputs.

DYNAMIC_CREW_AGENT = {
    "id": "example-msc-dynamic-agent-001",
    "name": "Measure Converter",
    "role": "Measure Conversion Manager",
    "goal": (
        "Translate given measure definitions efficiently into the requested target dialect.\n\n"
        "You will make sure that the action run will only trigger 1 time and not more."
    ),
    "backstory": (
        "Experienced Measure Engineer with 10+ years of experience in managing, "
        "translating, and optimizing KPI (Key Performance Indicators) and translating "
        "them into various output dialects."
    ),
    "llm": "databricks-llama-4-maverick",
    "tools": [],
    "tool_configs": {},
    "max_iter": 25,
    "max_rpm": 10,  # Changed from 1 to 10 to avoid artificial rate limiting delays
    "max_execution_time": 300,
    "verbose": False,
    "allow_delegation": False,
    "cache": True,
    "memory": True,
    "embedder_config": {
        "provider": "databricks",
        "config": {
            "model": "databricks-gte-large-en"
        }
    },
    "allow_code_execution": False,
    "code_execution_mode": "safe",
    "max_retry_limit": 3,
    "use_system_prompt": True,
    "respect_context_window": True,
}

DYNAMIC_CREW_TASK = {
    "id": "example-msc-dynamic-task-001",
    "name": "Run Measure Conversion with Dynamic Parameters",
    "description": (
        "Convert Power BI measures to the target format using the Measure Conversion Pipeline tool.\n\n"
        "**Source Configuration:**\n"
        "- Dataset ID: {dataset_id}\n"
        "- Workspace ID: {workspace_id}\n"
        "- Tenant ID: {tenant_id}\n"
        "- Client ID: {client_id}\n"
        "- Client Secret: {client_secret}\n\n"
        "**Target Format:** {target}\n\n"
        "Call the Measure Conversion Pipeline tool to perform the conversion. "
        "The tool has been pre-configured to use the parameters above. "
        "Simply invoke the tool without any additional parameters - it will automatically "
        "use the provided credentials and configuration. Return the generated measures in the target format."
    ),
    "expected_output": (
        "When everything is mappable, the agent should return something like a JSON-style or "
        "object-style list. While for formulas containing errors in the parsing another JSON "
        "object with the error of the conversion should be output."
    ),
    "agent_id": "example-msc-dynamic-agent-001",
    "tools": ["73"],  # Measure Conversion Pipeline tool ID
    "tool_configs": {
        "Measure Conversion Pipeline": {
            "mode": "dynamic",
            "inbound_connector": "powerbi",
            "outbound_format": "{target}",
            "powerbi_tenant_id": "{tenant_id}",
            "powerbi_group_id": "{workspace_id}",
            "powerbi_semantic_model_id": "{dataset_id}",
            "powerbi_client_id": "{client_id}",
            "powerbi_client_secret": "{client_secret}",
            "powerbi_include_hidden": True,
        }
    },
    "async_execution": False,
    "context": [],
    "config": {
        "cache_response": False,
        "cache_ttl": 3600,
        "retry_on_fail": True,
        "max_retries": 3,
        "priority": 1,
        "error_handling": "default",
        "human_input": False,
        "markdown": False,
    },
}

DYNAMIC_CREW = {
    "id": "example-msc-dynamic-crew-001",
    "name": "Measure Converter - Dynamic Parameters",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": True,
    "verbose": True,
    "agent_ids": ["example-msc-dynamic-agent-001"],
    "task_ids": ["example-msc-dynamic-task-001"],
    "nodes": [
        {
            "id": "agent-example-msc-dynamic-agent-001",
            "type": "agentNode",
            "position": {"x": 68, "y": 73},
            "data": {
                "label": "Measure Converter",
                "role": DYNAMIC_CREW_AGENT["role"],
                "goal": DYNAMIC_CREW_AGENT["goal"],
                "backstory": DYNAMIC_CREW_AGENT["backstory"],
                "tools": [],
                "tool_configs": {},
                "agentId": "example-msc-dynamic-agent-001",
                "taskId": None,
                "llm": "databricks-llama-4-maverick",
                "max_iter": 25,
                "max_rpm": 1,
                "max_execution_time": 300,
                "verbose": False,
                "allow_delegation": False,
                "cache": True,
                "memory": True,
                "embedder_config": DYNAMIC_CREW_AGENT["embedder_config"],
                "allow_code_execution": False,
                "code_execution_mode": "safe",
                "max_retry_limit": 3,
                "use_system_prompt": True,
                "respect_context_window": True,
                "type": "agent",
            },
        },
        {
            "id": "task-example-msc-dynamic-task-001",
            "type": "taskNode",
            "position": {"x": 368, "y": 68},
            "data": {
                "label": "Run Measure Conversion with Dynamic Parameters",
                "tools": ["73"],
                "tool_configs": DYNAMIC_CREW_TASK["tool_configs"],
                "agentId": None,
                "taskId": "example-msc-dynamic-task-001",
                "memory": True,
                "type": "task",
                "description": DYNAMIC_CREW_TASK["description"],
                "expected_output": DYNAMIC_CREW_TASK["expected_output"],
                "config": DYNAMIC_CREW_TASK["config"],
                "context": [],
                "async_execution": False,
            },
        },
    ],
    "edges": [
        {
            "source": "agent-example-msc-dynamic-agent-001",
            "target": "task-example-msc-dynamic-task-001",
            "id": "edge-dynamic-agent-to-task",
        }
    ],
}


# ============================================================================
# Example Crew 2: Static Configuration Mode
# ============================================================================
# This crew has all parameters configured directly in the UI.
# Credentials are placeholders that users must replace with their own values.

STATIC_CREW_AGENT = {
    "id": "example-msc-static-agent-001",
    "name": "Measure Converter",
    "role": "Measure Conversion Manager",
    "goal": (
        "Translate given measure definitions efficiently into the requested target dialect.\n\n"
        "You will make sure that the action run will only trigger 1 time and not more."
    ),
    "backstory": (
        "Experienced Measure Engineer with 10+ years of experience in managing, "
        "translating, and optimizing KPI (Key Performance Indicators) and translating "
        "them into various output dialects."
    ),
    "llm": "databricks-llama-4-maverick",
    "tools": ["74"],  # Optional additional tool
    "tool_configs": None,
    "max_iter": 25,
    "max_rpm": 10,  # Changed from 1 to 10 to avoid artificial rate limiting delays
    "max_execution_time": 300,
    "verbose": False,
    "allow_delegation": False,
    "cache": True,
    "memory": True,
    "embedder_config": {
        "provider": "databricks",
        "config": {
            "model": "databricks-gte-large-en"
        }
    },
    "allow_code_execution": False,
    "code_execution_mode": "safe",
    "max_retry_limit": 3,
    "use_system_prompt": True,
    "respect_context_window": True,
}

STATIC_CREW_TASK = {
    "id": "example-msc-static-task-001",
    "name": "Run Measure Conversion with Static Parameters",
    "description": (
        "Use the Measure Conversion Pipeline tool to convert the measure accordingly "
        "to the FROM and TO format defined in the tool-configuration.\n\n"
        "Call the Measure Conversion Pipeline tool to perform the conversion and "
        "return the generated measures in the desired output format."
    ),
    "expected_output": (
        "When everything is mappable, the agent should return something like a JSON-style or "
        "object-style list. While for formulas containing errors in the parsing another JSON "
        "object with the error of the conversion should be output."
    ),
    "agent_id": "example-msc-static-agent-001",
    "tools": ["73"],  # Measure Conversion Pipeline tool ID
    "tool_configs": {
        "Measure Conversion Pipeline": {
            "inbound_connector": "powerbi",
            "outbound_format": "uc_metrics",
            # PLACEHOLDER VALUES - Users must replace with their own credentials
            "powerbi_tenant_id": "<YOUR_TENANT_ID>",
            "powerbi_group_id": "<YOUR_WORKSPACE_ID>",
            "powerbi_semantic_model_id": "<YOUR_DATASET_ID>",
            "powerbi_client_id": "<YOUR_CLIENT_ID>",
            "powerbi_client_secret": "<YOUR_CLIENT_SECRET>",
            "powerbi_include_hidden": True,
        }
    },
    "async_execution": False,
    "context": [],
    "config": {
        "cache_response": False,
        "cache_ttl": 3600,
        "retry_on_fail": True,
        "max_retries": 3,
        "priority": 1,
        "error_handling": "default",
        "human_input": False,
        "markdown": False,
    },
}

STATIC_CREW = {
    "id": "example-msc-static-crew-001",
    "name": "Measure Converter - Static Configuration",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": True,
    "verbose": True,
    "agent_ids": ["example-msc-static-agent-001"],
    "task_ids": ["example-msc-static-task-001"],
    "nodes": [
        {
            "id": "agent-example-msc-static-agent-001",
            "type": "agentNode",
            "position": {"x": -178, "y": 182},
            "data": {
                "label": "Measure Converter",
                "role": STATIC_CREW_AGENT["role"],
                "goal": STATIC_CREW_AGENT["goal"],
                "backstory": STATIC_CREW_AGENT["backstory"],
                "tools": ["74"],
                "tool_configs": None,
                "agentId": "example-msc-static-agent-001",
                "taskId": None,
                "llm": "databricks-llama-4-maverick",
                "max_iter": 25,
                "max_rpm": 1,
                "max_execution_time": 300,
                "verbose": False,
                "allow_delegation": False,
                "cache": True,
                "memory": True,
                "embedder_config": STATIC_CREW_AGENT["embedder_config"],
                "allow_code_execution": False,
                "code_execution_mode": "safe",
                "max_retry_limit": 3,
                "use_system_prompt": True,
                "respect_context_window": True,
                "type": "agent",
            },
        },
        {
            "id": "task-example-msc-static-task-001",
            "type": "taskNode",
            "position": {"x": 43, "y": 182},
            "data": {
                "label": "Run Measure Conversion with Static Parameters",
                "tools": ["73"],
                "tool_configs": STATIC_CREW_TASK["tool_configs"],
                "agentId": None,
                "taskId": "example-msc-static-task-001",
                "memory": True,
                "type": "task",
                "description": STATIC_CREW_TASK["description"],
                "expected_output": STATIC_CREW_TASK["expected_output"],
                "config": STATIC_CREW_TASK["config"],
                "context": [],
                "async_execution": False,
            },
        },
    ],
    "edges": [
        {
            "source": "agent-example-msc-static-agent-001",
            "target": "task-example-msc-static-task-001",
            "id": "edge-static-agent-to-task",
        }
    ],
}


# ============================================================================
# Example Crew 3: M-Query Conversion - Dynamic Parameter Mode
# ============================================================================
# This crew uses the M-Query Conversion Pipeline to extract M-Query expressions
# from Power BI semantic models and convert them to Databricks SQL.

MQUERY_DYNAMIC_CREW_AGENT = {
    "id": "example-mquery-dynamic-agent-001",
    "name": "M-Query Converter",
    "role": "M-Query Extraction and Conversion Specialist",
    "goal": (
        "Extract M-Query expressions from Power BI semantic models using the Admin API "
        "and convert them to Databricks SQL CREATE VIEW statements.\n\n"
        "You will scan the specified workspace, extract all table definitions, "
        "and generate SQL code for Unity Catalog."
    ),
    "backstory": (
        "Expert in Power BI data models and M-Query (Power Query) with deep knowledge "
        "of Databricks SQL and Unity Catalog. Specializes in migrating Power BI "
        "data models to Databricks, understanding complex M-Query transformations, "
        "and generating production-ready SQL code."
    ),
    "llm": "databricks-llama-4-maverick",
    "tools": [],
    "tool_configs": {},
    "max_iter": 25,
    "max_rpm": 1,
    "max_execution_time": 600,  # M-Query extraction can take longer
    "verbose": False,
    "allow_delegation": False,
    "cache": True,
    "memory": True,
    "embedder_config": {
        "provider": "databricks",
        "config": {
            "model": "databricks-gte-large-en"
        }
    },
    "allow_code_execution": False,
    "code_execution_mode": "safe",
    "max_retry_limit": 3,
    "use_system_prompt": True,
    "respect_context_window": True,
}

MQUERY_DYNAMIC_CREW_TASK = {
    "id": "example-mquery-dynamic-task-001",
    "name": "Extract and Convert M-Query to SQL",
    "description": (
        "Extract M-Query expressions from Power BI and convert them to Databricks SQL.\n\n"
        "**Source Configuration:**\n"
        "- Workspace ID: {workspace_id}\n"
        "- Dataset ID: {dataset_id} (optional)\n"
        "- Tenant ID: {tenant_id}\n"
        "- Client ID: {client_id}\n"
        "- Client Secret: {client_secret}\n\n"
        "**Target Configuration:**\n"
        "- Catalog: {target_catalog}\n"
        "- Schema: {target_schema}\n\n"
        "Call the M-Query Conversion Pipeline tool to perform the extraction and conversion. "
        "The tool will scan the Power BI workspace, extract M-Query expressions from all tables, "
        "and generate CREATE VIEW statements for Unity Catalog. "
        "Return the generated SQL code along with relationships and summary information."
    ),
    "expected_output": (
        "A comprehensive report containing:\n"
        "1. CREATE VIEW SQL statements for each table\n"
        "2. Foreign key constraint SQL for relationships\n"
        "3. Summary of expression types found (native_query, databricks_catalog, etc.)\n"
        "4. Any conversion notes or warnings"
    ),
    "agent_id": "example-mquery-dynamic-agent-001",
    "tools": ["74"],  # M-Query Conversion Pipeline tool ID
    "tool_configs": {
        "M-Query Conversion Pipeline": {
            "mode": "dynamic",
            "workspace_id": "{workspace_id}",
            "dataset_id": "{dataset_id}",
            "tenant_id": "{tenant_id}",
            "client_id": "{client_id}",
            "client_secret": "{client_secret}",
            "target_catalog": "{target_catalog}",
            "target_schema": "{target_schema}",
            "use_llm": True,
            "include_hidden_tables": False,
            "skip_static_tables": True,
            "include_relationships": True,
            "include_summary": True,
        }
    },
    "async_execution": False,
    "context": [],
    "config": {
        "cache_response": False,
        "cache_ttl": 3600,
        "retry_on_fail": True,
        "max_retries": 3,
        "priority": 1,
        "error_handling": "default",
        "human_input": False,
        "markdown": False,
    },
}

MQUERY_DYNAMIC_CREW = {
    "id": "example-mquery-dynamic-crew-001",
    "name": "M-Query Converter - Dynamic Parameters",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": True,
    "verbose": True,
    "agent_ids": ["example-mquery-dynamic-agent-001"],
    "task_ids": ["example-mquery-dynamic-task-001"],
    "nodes": [
        {
            "id": "agent-example-mquery-dynamic-agent-001",
            "type": "agentNode",
            "position": {"x": 68, "y": 73},
            "data": {
                "label": "M-Query Converter",
                "role": MQUERY_DYNAMIC_CREW_AGENT["role"],
                "goal": MQUERY_DYNAMIC_CREW_AGENT["goal"],
                "backstory": MQUERY_DYNAMIC_CREW_AGENT["backstory"],
                "tools": [],
                "tool_configs": {},
                "agentId": "example-mquery-dynamic-agent-001",
                "taskId": None,
                "llm": "databricks-llama-4-maverick",
                "max_iter": 25,
                "max_rpm": 1,
                "max_execution_time": 600,
                "verbose": False,
                "allow_delegation": False,
                "cache": True,
                "memory": True,
                "embedder_config": MQUERY_DYNAMIC_CREW_AGENT["embedder_config"],
                "allow_code_execution": False,
                "code_execution_mode": "safe",
                "max_retry_limit": 3,
                "use_system_prompt": True,
                "respect_context_window": True,
                "type": "agent",
            },
        },
        {
            "id": "task-example-mquery-dynamic-task-001",
            "type": "taskNode",
            "position": {"x": 368, "y": 68},
            "data": {
                "label": "Extract and Convert M-Query to SQL",
                "tools": ["74"],
                "tool_configs": MQUERY_DYNAMIC_CREW_TASK["tool_configs"],
                "agentId": None,
                "taskId": "example-mquery-dynamic-task-001",
                "memory": True,
                "type": "task",
                "description": MQUERY_DYNAMIC_CREW_TASK["description"],
                "expected_output": MQUERY_DYNAMIC_CREW_TASK["expected_output"],
                "config": MQUERY_DYNAMIC_CREW_TASK["config"],
                "context": [],
                "async_execution": False,
            },
        },
    ],
    "edges": [
        {
            "source": "agent-example-mquery-dynamic-agent-001",
            "target": "task-example-mquery-dynamic-task-001",
            "id": "edge-mquery-dynamic-agent-to-task",
        }
    ],
}


# ============================================================================
# Example Crew 4: M-Query Conversion - Static Configuration Mode
# ============================================================================
# This crew has all parameters configured directly in the UI.

MQUERY_STATIC_CREW_AGENT = {
    "id": "example-mquery-static-agent-001",
    "name": "M-Query Converter",
    "role": "M-Query Extraction and Conversion Specialist",
    "goal": (
        "Extract M-Query expressions from Power BI semantic models using the Admin API "
        "and convert them to Databricks SQL CREATE VIEW statements.\n\n"
        "You will scan the specified workspace, extract all table definitions, "
        "and generate SQL code for Unity Catalog."
    ),
    "backstory": (
        "Expert in Power BI data models and M-Query (Power Query) with deep knowledge "
        "of Databricks SQL and Unity Catalog. Specializes in migrating Power BI "
        "data models to Databricks, understanding complex M-Query transformations, "
        "and generating production-ready SQL code."
    ),
    "llm": "databricks-llama-4-maverick",
    "tools": [],
    "tool_configs": None,
    "max_iter": 25,
    "max_rpm": 1,
    "max_execution_time": 600,
    "verbose": False,
    "allow_delegation": False,
    "cache": True,
    "memory": True,
    "embedder_config": {
        "provider": "databricks",
        "config": {
            "model": "databricks-gte-large-en"
        }
    },
    "allow_code_execution": False,
    "code_execution_mode": "safe",
    "max_retry_limit": 3,
    "use_system_prompt": True,
    "respect_context_window": True,
}

MQUERY_STATIC_CREW_TASK = {
    "id": "example-mquery-static-task-001",
    "name": "Extract and Convert M-Query to SQL (Static)",
    "description": (
        "Use the M-Query Conversion Pipeline tool to extract M-Query expressions "
        "from the configured Power BI workspace and convert them to Databricks SQL.\n\n"
        "The tool has been pre-configured with workspace and authentication credentials. "
        "Call the tool to perform the extraction and conversion, then return the "
        "generated SQL code along with relationships and summary information."
    ),
    "expected_output": (
        "A comprehensive report containing:\n"
        "1. CREATE VIEW SQL statements for each table\n"
        "2. Foreign key constraint SQL for relationships\n"
        "3. Summary of expression types found\n"
        "4. Any conversion notes or warnings"
    ),
    "agent_id": "example-mquery-static-agent-001",
    "tools": ["74"],  # M-Query Conversion Pipeline tool ID
    "tool_configs": {
        "M-Query Conversion Pipeline": {
            "mode": "static",
            # PLACEHOLDER VALUES - Users must replace with their own credentials
            "workspace_id": "<YOUR_WORKSPACE_ID>",
            "dataset_id": "",
            "tenant_id": "<YOUR_TENANT_ID>",
            "client_id": "<YOUR_CLIENT_ID>",
            "client_secret": "<YOUR_CLIENT_SECRET>",
            "target_catalog": "main",
            "target_schema": "default",
            "use_llm": True,
            "include_hidden_tables": False,
            "skip_static_tables": True,
            "include_relationships": True,
            "include_summary": True,
        }
    },
    "async_execution": False,
    "context": [],
    "config": {
        "cache_response": False,
        "cache_ttl": 3600,
        "retry_on_fail": True,
        "max_retries": 3,
        "priority": 1,
        "error_handling": "default",
        "human_input": False,
        "markdown": False,
    },
}

MQUERY_STATIC_CREW = {
    "id": "example-mquery-static-crew-001",
    "name": "M-Query Converter - Static Configuration",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": True,
    "verbose": True,
    "agent_ids": ["example-mquery-static-agent-001"],
    "task_ids": ["example-mquery-static-task-001"],
    "nodes": [
        {
            "id": "agent-example-mquery-static-agent-001",
            "type": "agentNode",
            "position": {"x": -178, "y": 182},
            "data": {
                "label": "M-Query Converter",
                "role": MQUERY_STATIC_CREW_AGENT["role"],
                "goal": MQUERY_STATIC_CREW_AGENT["goal"],
                "backstory": MQUERY_STATIC_CREW_AGENT["backstory"],
                "tools": [],
                "tool_configs": None,
                "agentId": "example-mquery-static-agent-001",
                "taskId": None,
                "llm": "databricks-llama-4-maverick",
                "max_iter": 25,
                "max_rpm": 1,
                "max_execution_time": 600,
                "verbose": False,
                "allow_delegation": False,
                "cache": True,
                "memory": True,
                "embedder_config": MQUERY_STATIC_CREW_AGENT["embedder_config"],
                "allow_code_execution": False,
                "code_execution_mode": "safe",
                "max_retry_limit": 3,
                "use_system_prompt": True,
                "respect_context_window": True,
                "type": "agent",
            },
        },
        {
            "id": "task-example-mquery-static-task-001",
            "type": "taskNode",
            "position": {"x": 43, "y": 182},
            "data": {
                "label": "Extract and Convert M-Query to SQL (Static)",
                "tools": ["74"],
                "tool_configs": MQUERY_STATIC_CREW_TASK["tool_configs"],
                "agentId": None,
                "taskId": "example-mquery-static-task-001",
                "memory": True,
                "type": "task",
                "description": MQUERY_STATIC_CREW_TASK["description"],
                "expected_output": MQUERY_STATIC_CREW_TASK["expected_output"],
                "config": MQUERY_STATIC_CREW_TASK["config"],
                "context": [],
                "async_execution": False,
            },
        },
    ],
    "edges": [
        {
            "source": "agent-example-mquery-static-agent-001",
            "target": "task-example-mquery-static-task-001",
            "id": "edge-mquery-static-agent-to-task",
        }
    ],
}


# ============================================================================
# All Example Crews
# ============================================================================

EXAMPLE_CREWS = [
    # Measure Conversion Pipeline Crews
    {
        "crew": DYNAMIC_CREW,
        "agent": DYNAMIC_CREW_AGENT,
        "task": DYNAMIC_CREW_TASK,
    },
    {
        "crew": STATIC_CREW,
        "agent": STATIC_CREW_AGENT,
        "task": STATIC_CREW_TASK,
    },
    # M-Query Conversion Pipeline Crews
    {
        "crew": MQUERY_DYNAMIC_CREW,
        "agent": MQUERY_DYNAMIC_CREW_AGENT,
        "task": MQUERY_DYNAMIC_CREW_TASK,
    },
    {
        "crew": MQUERY_STATIC_CREW,
        "agent": MQUERY_STATIC_CREW_AGENT,
        "task": MQUERY_STATIC_CREW_TASK,
    },
]


async def seed_agent(session: AsyncSession, agent_data: Dict[str, Any]) -> None:
    """Seed a single agent if it doesn't exist."""
    result = await session.execute(
        select(Agent).where(Agent.id == agent_data["id"])
    )
    existing = result.scalar_one_or_none()

    if existing:
        logger.info(f"Agent '{agent_data['name']}' already exists, skipping...")
        return

    agent = Agent(
        id=agent_data["id"],
        name=agent_data["name"],
        role=agent_data["role"],
        goal=agent_data["goal"],
        backstory=agent_data.get("backstory"),
        llm=agent_data.get("llm", "databricks-llama-4-maverick"),
        tools=agent_data.get("tools", []),
        tool_configs=agent_data.get("tool_configs"),
        max_iter=agent_data.get("max_iter", 25),
        max_rpm=agent_data.get("max_rpm"),
        max_execution_time=agent_data.get("max_execution_time"),
        verbose=agent_data.get("verbose", False),
        allow_delegation=agent_data.get("allow_delegation", False),
        cache=agent_data.get("cache", True),
        memory=agent_data.get("memory", True),
        embedder_config=agent_data.get("embedder_config"),
        allow_code_execution=agent_data.get("allow_code_execution", False),
        code_execution_mode=agent_data.get("code_execution_mode", "safe"),
        max_retry_limit=agent_data.get("max_retry_limit", 2),
        use_system_prompt=agent_data.get("use_system_prompt", True),
        respect_context_window=agent_data.get("respect_context_window", True),
    )
    session.add(agent)
    logger.info(f"Created agent: {agent_data['name']}")


async def seed_task(session: AsyncSession, task_data: Dict[str, Any]) -> None:
    """Seed a single task if it doesn't exist."""
    result = await session.execute(
        select(Task).where(Task.id == task_data["id"])
    )
    existing = result.scalar_one_or_none()

    if existing:
        logger.info(f"Task '{task_data['name']}' already exists, skipping...")
        return

    task = Task(
        id=task_data["id"],
        name=task_data["name"],
        description=task_data["description"],
        expected_output=task_data["expected_output"],
        agent_id=task_data.get("agent_id"),
        tools=task_data.get("tools", []),
        tool_configs=task_data.get("tool_configs"),
        async_execution=task_data.get("async_execution", False),
        context=task_data.get("context", []),
        config=task_data.get("config", {}),
    )
    session.add(task)
    logger.info(f"Created task: {task_data['name']}")


async def seed_crew(session: AsyncSession, crew_data: Dict[str, Any]) -> None:
    """Seed a single crew if it doesn't exist."""
    crew_id = uuid.UUID(crew_data["id"]) if isinstance(crew_data["id"], str) else crew_data["id"]

    result = await session.execute(
        select(Crew).where(Crew.id == crew_id)
    )
    existing = result.scalar_one_or_none()

    if existing:
        logger.info(f"Crew '{crew_data['name']}' already exists, skipping...")
        return

    crew = Crew(
        id=crew_id,
        name=crew_data["name"],
        process=crew_data.get("process", "sequential"),
        planning=crew_data.get("planning", False),
        planning_llm=crew_data.get("planning_llm"),
        reasoning=crew_data.get("reasoning", False),
        reasoning_llm=crew_data.get("reasoning_llm"),
        manager_llm=crew_data.get("manager_llm"),
        memory=crew_data.get("memory", True),
        verbose=crew_data.get("verbose", True),
        max_rpm=crew_data.get("max_rpm"),
        agent_ids=crew_data.get("agent_ids", []),
        task_ids=crew_data.get("task_ids", []),
        nodes=crew_data.get("nodes", []),
        edges=crew_data.get("edges", []),
    )
    session.add(crew)
    logger.info(f"Created crew: {crew_data['name']}")


async def seed() -> None:
    """
    Seed example crews for the Measure Conversion Pipeline.

    This creates two example crews:
    1. Dynamic Parameters Mode - Uses placeholders resolved at execution time
    2. Static Configuration Mode - Uses pre-configured parameters (with placeholder credentials)
    """
    logger.info("Starting example crews seeder...")

    async with async_session_factory() as session:
        try:
            for example in EXAMPLE_CREWS:
                # Seed agent first (crews reference agents)
                await seed_agent(session, example["agent"])

                # Seed task (tasks reference agents)
                await seed_task(session, example["task"])

                # Seed crew (crews reference agents and tasks)
                await seed_crew(session, example["crew"])

            await session.commit()
            logger.info("Example crews seeder completed successfully.")

        except Exception as e:
            await session.rollback()
            logger.error(f"Error seeding example crews: {e}")
            raise


if __name__ == "__main__":
    import asyncio
    asyncio.run(seed())
