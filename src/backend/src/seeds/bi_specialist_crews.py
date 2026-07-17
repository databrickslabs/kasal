"""
BI Specialist Workspace Seeder

Pre-seeds a "bi-specialist" group (workspace) with a complete set of
boilerplate crew templates for the Power BI → Databricks migration pipeline.

The seeded crews cover the full E2E flow:
  1. Pipeline Config Generator       (tool 90)
  2. UC Metric View Generator        (tool 86)
  3. UCMV Quality Validator          (tool 91)
  4. Metric View Deployer            (tool 88)
  5. Report References               (tool 78)
  6. PBI Visual-UCMV Mapper          (tool 94)
  7. Databricks Dashboard Creator    (tool 95)
  8. UCMV Genie Space Config Gen.    (tool 93)
  9. Genie Space Generator           (tool 92)

Plus the E2E flow that wires them all together.

Credential/input placeholders are left empty so the user only needs to fill in
their workspace_id, client_id, client_secret, catalog, schema, warehouse_id etc.
All tool logic and agent configurations are ready to run.
"""

import uuid
import logging
from sqlalchemy import select

from src.db.session import async_session_factory
from src.models.crew import Crew
from src.models.agent import Agent
from src.models.task import Task
from src.models.group import Group
from src.models.group_tool import GroupTool

# Tools required by the PBI migration pipeline — pre-enabled for bi-specialist
BI_TOOLS = [78, 86, 88, 90, 91, 92, 93, 94, 95]

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Group / Workspace
# ─────────────────────────────────────────────────────────────────────────────

BI_GROUP_ID = "bi-specialist"
BI_GROUP = {
    "id": BI_GROUP_ID,
    "name": "BI Specialist",
    "status": "active",
    "description": (
        "Pre-configured workspace for the Power BI → Databricks migration pipeline. "
        "Contains ready-to-run crew templates for UC Metric View generation, "
        "Genie Space deployment, and AI/BI Dashboard creation. "
        "Fill in your workspace credentials to get started."
    ),
    "auto_created": False,
    "created_by_email": "system@kasal.ai",
}

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _agent_node(agent_id: str, agent_data: dict, x: int, y: int) -> dict:
    """Build a ReactFlow agentNode for the crew's visual graph."""
    return {
        "id": f"agent-{agent_id}",
        "type": "agentNode",
        "position": {"x": x, "y": y},
        "data": {
            "label": agent_data["role"],
            "role": agent_data["role"],
            "goal": agent_data["goal"],
            "backstory": agent_data["backstory"],
            "tools": agent_data.get("tools", []),
            "tool_configs": agent_data.get("tool_configs"),
            "agentId": agent_id,
            "taskId": None,
            "llm": agent_data.get("llm", "databricks-claude-sonnet-4"),
            "function_calling_llm": None,
            "max_iter": agent_data.get("max_iter", 25),
            "max_rpm": agent_data.get("max_rpm", 10),
            "max_execution_time": agent_data.get("max_execution_time", 300),
            "verbose": agent_data.get("verbose", True),
            "allow_delegation": agent_data.get("allow_delegation", False),
            "cache": agent_data.get("cache", True),
            "memory": agent_data.get("memory", True),
            "embedder_config": agent_data.get("embedder_config"),
            "allow_code_execution": False,
            "code_execution_mode": "safe",
            "max_retry_limit": agent_data.get("max_retry_limit", 3),
            "use_system_prompt": True,
            "respect_context_window": True,
            "type": "agent",
            "inject_date": True,
        },
    }


def _task_node(task_id: str, agent_id: str, task_data: dict, x: int, y: int) -> dict:
    """Build a ReactFlow taskNode for the crew's visual graph."""
    return {
        "id": f"task-{task_id}",
        "type": "taskNode",
        "position": {"x": x, "y": y},
        "data": {
            "label": task_data["name"],
            "tools": task_data.get("tools", []),
            "tool_configs": task_data.get("tool_configs"),
            "agentId": None,
            "taskId": task_id,
            "llm": None,
            "type": "task",
            "description": task_data["description"],
            "expected_output": task_data["expected_output"],
            "config": task_data.get("config", {}),
            "async_execution": task_data.get("async_execution", False),
            "context": task_data.get("context", []),
            "human_input": task_data.get("config", {}).get("human_input", False),
            "markdown": False,
        },
    }


def _agent_to_task_edge(crew_slug: str, agent_id: str, task_id: str) -> dict:
    return {
        "id": f"edge-{crew_slug}-agent-to-task",
        "source": f"agent-{agent_id}",
        "target": f"task-{task_id}",
    }


DEFAULT_EMBEDDER = {
    "provider": "databricks",
    "config": {"model": "databricks-gte-large-en"},
}

DEFAULT_TASK_CONFIG = {
    "cache_response": False,
    "cache_ttl": 3600,
    "retry_on_fail": True,
    "max_retries": 3,
    "priority": 1,
    "error_handling": "default",
    "human_input": False,
    "markdown": False,
    "output_json": "true",
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 1 — Pipeline Config Generator
# ─────────────────────────────────────────────────────────────────────────────

PIPELINE_CONFIG_AGENT_ID = "bi-pipeline-config-agent-001"
PIPELINE_CONFIG_TASK_ID = "bi-pipeline-config-task-001"
PIPELINE_CONFIG_CREW_ID = "bi-pipeline-config-crew-001"

PIPELINE_CONFIG_AGENT = {
    "id": PIPELINE_CONFIG_AGENT_ID,
    "name": "Pipeline Config Agent",
    "role": "Pipeline Configuration Specialist",
    "goal": (
        "Generate a complete pipeline_config.json by calling the Fabric APIs directly. "
        "Produce all required configuration keys with auto-filled values and TODO markers "
        "for manual review items."
    ),
    "backstory": (
        "You are a Databricks specialist who extracts full pipeline configuration from "
        "Microsoft Fabric workspaces using the Power BI Admin and Execute Queries APIs. "
        "Call the Pipeline Config Generator tool with ZERO arguments — all credentials "
        "are pre-configured in the tool task form."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

PIPELINE_CONFIG_TASK = {
    "id": PIPELINE_CONFIG_TASK_ID,
    "name": "Generate Pipeline Configuration",
    "description": (
        "Call the Pipeline Config Generator tool with NO arguments — credentials and PBI "
        "API parameters are ALREADY PRE-CONFIGURED in the tool-task-form.\n\n"
        "DO NOT ask the user for any information. Just call the tool NOW.\n\n"
        "The tool will:\n"
        "- Connect to the Fabric workspace using the configured Service Principals\n"
        "- Extract join keys, fact tables, dimension tables, and measure allocations\n"
        "- Generate a complete pipeline_config.json ready for the UC Metric View Generator\n\n"
        "Return the tool output directly."
    ),
    "expected_output": (
        "A JSON object (pipeline_config.json) containing join_key_map, fact_join_map, "
        "and all 26 configuration keys. Ready to pass to the UC Metric View Generator."
    ),
    "agent_id": PIPELINE_CONFIG_AGENT_ID,
    "tools": ["90"],
    "tool_configs": {
        "Pipeline Config Generator": {
            "result_as_answer": True,
            "workspace_id": "",
            "dataset_id": "",
            "report_id": "",
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "admin_client_id": "",
            "admin_client_secret": "",
            "catalog": "",
            "schema_name": "",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

PIPELINE_CONFIG_CREW = {
    "id": PIPELINE_CONFIG_CREW_ID,
    "name": "UCMV — Generate Pipeline Config (API-Direct)",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [PIPELINE_CONFIG_AGENT_ID],
    "task_ids": [PIPELINE_CONFIG_TASK_ID],
    "nodes": [
        _agent_node(PIPELINE_CONFIG_AGENT_ID, PIPELINE_CONFIG_AGENT, 68, 68),
        _task_node(PIPELINE_CONFIG_TASK_ID, PIPELINE_CONFIG_AGENT_ID, PIPELINE_CONFIG_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("pipeline-config", PIPELINE_CONFIG_AGENT_ID, PIPELINE_CONFIG_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 2 — UC Metric View Generator
# ─────────────────────────────────────────────────────────────────────────────

UCMV_GEN_AGENT_ID = "bi-ucmv-gen-agent-001"
UCMV_GEN_TASK_ID = "bi-ucmv-gen-task-001"
UCMV_GEN_CREW_ID = "bi-ucmv-gen-crew-001"

UCMV_GEN_AGENT = {
    "id": UCMV_GEN_AGENT_ID,
    "name": "UC Metric View Generator Agent",
    "role": "UC Metric View Generator",
    "goal": (
        "Generate UC Metric View YAML definitions and deploy SQL for all fact tables "
        "from the Power BI semantic model."
    ),
    "backstory": (
        "You are a Databricks UC Metric View specialist. You translate Power BI DAX measures "
        "into UCMV YAML definitions using the UC Metric View Generator tool. "
        "Call the tool with ZERO arguments — all parameters are pre-configured."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 10,
    "max_rpm": 10,
    "max_execution_time": 600,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

UCMV_GEN_TASK = {
    "id": UCMV_GEN_TASK_ID,
    "name": "Generate UC Metric View YAML definitions",
    "description": (
        "Generate UC Metric View YAML definitions and deploy SQL for all fact tables.\n\n"
        "⚠️ CRITICAL: Call the UC Metric View Generator tool with ZERO arguments. "
        "ALL required parameters (workspace_id, dataset_id, credentials, catalog, schema) "
        "are ALREADY PRE-CONFIGURED in the tool-task-form.\n\n"
        "Return the complete tool output."
    ),
    "expected_output": (
        "A JSON object with YAML definitions and deploy SQL for each fact table, "
        "plus generation statistics (measures translated, join keys detected)."
    ),
    "agent_id": UCMV_GEN_AGENT_ID,
    "tools": ["86"],
    "tool_configs": {
        "UC Metric View Generator": {
            "result_as_answer": True,
            "workspace_id": "",
            "dataset_id": "",
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "catalog": "",
            "schema_name": "",
            "use_llm_fallback": True,
            "llm_model": "databricks-claude-sonnet-4",
            # JSON mode: the flow injects the preceding Pipeline Config crew's
            # output into these fields (config_json ← proposed_config,
            # measures_json/mquery_json ← the handoff arrays it now emits). They
            # start empty so the injection fills them; UCMV builds views from
            # measures_json + mquery_json, so both MUST be present for the
            # handoff to produce any views.
            "config_json": "{}",
            "measures_json": "[]",
            "mquery_json": "[]",
            "relationships_json": "[]",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

UCMV_GEN_CREW = {
    "id": UCMV_GEN_CREW_ID,
    "name": "UC Metric View Generator — JSON Mode",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [UCMV_GEN_AGENT_ID],
    "task_ids": [UCMV_GEN_TASK_ID],
    "nodes": [
        _agent_node(UCMV_GEN_AGENT_ID, UCMV_GEN_AGENT, 68, 68),
        _task_node(UCMV_GEN_TASK_ID, UCMV_GEN_AGENT_ID, UCMV_GEN_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("ucmv-gen", UCMV_GEN_AGENT_ID, UCMV_GEN_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 3 — UCMV Quality Validator
# ─────────────────────────────────────────────────────────────────────────────

UCMV_VAL_AGENT_ID = "bi-ucmv-val-agent-001"
UCMV_VAL_TASK_ID = "bi-ucmv-val-task-001"
UCMV_VAL_CREW_ID = "bi-ucmv-val-crew-001"

UCMV_VAL_AGENT = {
    "id": UCMV_VAL_AGENT_ID,
    "name": "UCMV Validator Agent",
    "role": "Metric View Quality Validator",
    "goal": (
        "Validate the generated UC Metric View YAML definitions against the original "
        "DAX expressions to detect semantic mismatches."
    ),
    "backstory": (
        "You are a quality assurance specialist for UC Metric Views. "
        "You validate each measure's SQL against the source DAX and flag mismatches. "
        "Call the Metric View Validator tool with ZERO arguments."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

UCMV_VAL_TASK = {
    "id": UCMV_VAL_TASK_ID,
    "name": "Validate UC Metric View YAML definitions",
    "description": (
        "Validate the generated UC Metric View YAML definitions against the original "
        "DAX expressions from the Power BI report.\n\n"
        "⚠️ CRITICAL: Call the Metric View Validator tool with ZERO arguments — "
        "yaml_content and measures_json are auto-injected from the flow.\n\n"
        "Return the validation summary with VALID/EQUIVALENT/REVIEW/INVALID per measure."
    ),
    "expected_output": (
        "A JSON validation report with per-table and per-measure results "
        "(valid, equivalent, review, invalid counts) and recommendations."
    ),
    "agent_id": UCMV_VAL_AGENT_ID,
    "tools": ["91"],
    "tool_configs": {
        "Metric View Validator": {
            "result_as_answer": True,
            "yaml_content": None,
            "measures_json": None,
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

UCMV_VAL_CREW = {
    "id": UCMV_VAL_CREW_ID,
    "name": "UCMV Quality Validator",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [UCMV_VAL_AGENT_ID],
    "task_ids": [UCMV_VAL_TASK_ID],
    "nodes": [
        _agent_node(UCMV_VAL_AGENT_ID, UCMV_VAL_AGENT, 68, 68),
        _task_node(UCMV_VAL_TASK_ID, UCMV_VAL_AGENT_ID, UCMV_VAL_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("ucmv-val", UCMV_VAL_AGENT_ID, UCMV_VAL_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 4 — Metric View Deployer
# ─────────────────────────────────────────────────────────────────────────────

DEPLOYER_AGENT_ID = "bi-deployer-agent-001"
DEPLOYER_TASK_ID = "bi-deployer-task-001"
DEPLOYER_CREW_ID = "bi-deployer-crew-001"

DEPLOYER_AGENT = {
    "id": DEPLOYER_AGENT_ID,
    "name": "Metric View Deployer Agent",
    "role": "Databricks Platform Engineer",
    "goal": "Deploy the UC Metric View definitions to the Databricks workspace.",
    "backstory": (
        "You are a Databricks platform engineer who deploys UC Metric View definitions. "
        "Call the Metric View Deployer tool with ZERO arguments — "
        "ucmv_output is auto-injected from the flow."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

DEPLOYER_TASK = {
    "id": DEPLOYER_TASK_ID,
    "name": "Deploy UC Metric View definitions",
    "description": (
        "Deploy the UC Metric View definitions from the previous step to Databricks.\n\n"
        "⚠️ CRITICAL: Call the Metric View Deployer tool with ZERO arguments — "
        "ucmv_output is auto-injected from the flow.\n\n"
        "Return the deployment status for each metric view."
    ),
    "expected_output": (
        "A JSON deployment report showing the status of each metric view "
        "(deployed/failed) with the full view names."
    ),
    "agent_id": DEPLOYER_AGENT_ID,
    "tools": ["88"],
    "tool_configs": {
        "Metric View Deployer": {
            "result_as_answer": True,
            "ucmv_output": None,
            "catalog": "",
            "schema_name": "",
            "dry_run": False,
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

DEPLOYER_CREW = {
    "id": DEPLOYER_CREW_ID,
    "name": "Metric View Deployer",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [DEPLOYER_AGENT_ID],
    "task_ids": [DEPLOYER_TASK_ID],
    "nodes": [
        _agent_node(DEPLOYER_AGENT_ID, DEPLOYER_AGENT, 68, 68),
        _task_node(DEPLOYER_TASK_ID, DEPLOYER_AGENT_ID, DEPLOYER_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("deployer", DEPLOYER_AGENT_ID, DEPLOYER_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 5 — Report References (Power BI)
# ─────────────────────────────────────────────────────────────────────────────

REFERENCES_AGENT_ID = "bi-references-agent-001"
REFERENCES_TASK_ID = "bi-references-task-001"
REFERENCES_CREW_ID = "bi-references-crew-001"

REFERENCES_AGENT = {
    "id": REFERENCES_AGENT_ID,
    "name": "Report References Agent",
    "role": "Report Conversion Manager",
    "goal": "Extract visual-to-measure references from the Power BI Fabric report.",
    "backstory": (
        "You extract measure and table references from Microsoft Fabric reports "
        "using the Power BI Report References Tool. "
        "IMMEDIATELY call the tool with ZERO arguments — all parameters are pre-configured."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 3,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

REFERENCES_TASK = {
    "id": REFERENCES_TASK_ID,
    "name": "Extract Power BI Report References",
    "description": (
        "IMMEDIATELY call the \"Power BI Report References Tool\" with parameters defined "
        "in the input task form.\n"
        "ALL required parameters (workspace_id, dataset_id, authentication credentials) "
        "are ALREADY PRE-CONFIGURED in the tool-task-form. "
        "You do NOT need to ask for them or provide them.\n\n"
        "DO NOT ask the user for any information. Just call the tool NOW.\n\n"
        "Return the tool's output directly."
    ),
    "expected_output": (
        "A structured report of visual-to-measure and visual-to-table references "
        "per report page, ready to pass to the PBI Visual-UCMV Mapper."
    ),
    "agent_id": REFERENCES_AGENT_ID,
    "tools": ["78"],
    "tool_configs": {
        "Power BI Report References Tool": {
            "result_as_answer": True,
            "workspace_id": "",
            "dataset_id": "",
            "report_id": "",
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "output_format": "json",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

REFERENCES_CREW = {
    "id": REFERENCES_CREW_ID,
    "name": "references",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [REFERENCES_AGENT_ID],
    "task_ids": [REFERENCES_TASK_ID],
    "nodes": [
        _agent_node(REFERENCES_AGENT_ID, REFERENCES_AGENT, 68, 68),
        _task_node(REFERENCES_TASK_ID, REFERENCES_AGENT_ID, REFERENCES_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("references", REFERENCES_AGENT_ID, REFERENCES_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 6 — PBI Visual-UCMV Mapper
# ─────────────────────────────────────────────────────────────────────────────

MAPPER_AGENT_ID = "bi-mapper-agent-001"
MAPPER_TASK_ID = "bi-mapper-task-001"
MAPPER_CREW_ID = "bi-mapper-crew-001"

MAPPER_AGENT = {
    "id": MAPPER_AGENT_ID,
    "name": "PBI Visual-UCMV Mapper Agent",
    "role": "Databricks Analytics Engineer",
    "goal": "Map Power BI report visuals to deployed UC Metric View metric views.",
    "backstory": (
        "You map Power BI visuals to UC Metric Views using the PBI Visual-UCMV Mapper tool. "
        "report_references_json and ucmv_output are pre-configured or auto-injected. "
        "Call the tool with ZERO arguments."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

MAPPER_TASK = {
    "id": MAPPER_TASK_ID,
    "name": "Map PBI visuals to UC Metric Views",
    "description": (
        "Map the pre-configured visual definitions to the deployed UC Metric View.\n\n"
        "⚠️ CRITICAL: Call the PBI Visual-UCMV Mapper tool with ZERO arguments. "
        "report_references_json and ucmv_output are pre-configured in the tool-task-form "
        "or auto-injected from the flow.\n\n"
        "Return the visual_mappings JSON array."
    ),
    "expected_output": (
        "A JSON object with visual_mappings array: each entry maps a PBI visual to its "
        "UCMV view, dimensions, measures, and executable Databricks SQL."
    ),
    "agent_id": MAPPER_AGENT_ID,
    "tools": ["94"],
    "tool_configs": {
        "PBI Visual-UCMV Mapper": {
            "result_as_answer": True,
            "report_references_json": None,
            "ucmv_output": None,
            "measures_json": None,
            "catalog": "",
            "schema_name": "",
            "dashboard_title": "",
            "databricks_host": "",
            "llm_model": "databricks-claude-sonnet-4",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

MAPPER_CREW = {
    "id": MAPPER_CREW_ID,
    "name": "PBI Visual-UCMV Mapper",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [MAPPER_AGENT_ID],
    "task_ids": [MAPPER_TASK_ID],
    "nodes": [
        _agent_node(MAPPER_AGENT_ID, MAPPER_AGENT, 68, 68),
        _task_node(MAPPER_TASK_ID, MAPPER_AGENT_ID, MAPPER_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("mapper", MAPPER_AGENT_ID, MAPPER_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 7 — Databricks Dashboard Creator
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_AGENT_ID = "bi-dashboard-agent-001"
DASHBOARD_TASK_ID = "bi-dashboard-task-001"
DASHBOARD_CREW_ID = "bi-dashboard-crew-001"

DASHBOARD_AGENT = {
    "id": DASHBOARD_AGENT_ID,
    "name": "Dashboard Creator Agent",
    "role": "Databricks Dashboard Specialist",
    "goal": "Create a Databricks AI/BI (Lakeview) dashboard from the PBI visual mappings.",
    "backstory": (
        "You create Databricks AI/BI dashboards from the PBI visual-to-UCMV mappings. "
        "visual_mappings_json is auto-injected from the flow. "
        "Call the Databricks Dashboard Creator tool with ZERO arguments."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 3,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

DASHBOARD_TASK = {
    "id": DASHBOARD_TASK_ID,
    "name": "Create Databricks AI/BI Dashboard",
    "description": (
        "Create a Databricks AI/BI (Lakeview) dashboard from the PBI visual mappings.\n\n"
        "⚠️ CRITICAL: Call the Databricks Dashboard Creator tool with ZERO arguments. "
        "visual_mappings_json is auto-injected from flow. "
        "All other parameters (title, catalog, schema, warehouse_id) are pre-configured.\n\n"
        "Return the dashboard URL."
    ),
    "expected_output": (
        "A JSON object with dashboard_id, dashboard_url, display_name, status, "
        "widget_count, page_count, and cicd_download_url."
    ),
    "agent_id": DASHBOARD_AGENT_ID,
    "tools": ["95"],
    "tool_configs": {
        "Databricks Dashboard Creator": {
            "result_as_answer": True,
            "visual_mappings_json": None,
            "dashboard_title": "",
            "catalog": "",
            "schema_name": "",
            "warehouse_id": "",
            "databricks_host": "",
            "parent_path": "/Workspace/Shared",
            "publish_dashboard": True,
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

DASHBOARD_CREW = {
    "id": DASHBOARD_CREW_ID,
    "name": "Databricks Dashboard Creator",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [DASHBOARD_AGENT_ID],
    "task_ids": [DASHBOARD_TASK_ID],
    "nodes": [
        _agent_node(DASHBOARD_AGENT_ID, DASHBOARD_AGENT, 68, 68),
        _task_node(DASHBOARD_TASK_ID, DASHBOARD_AGENT_ID, DASHBOARD_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("dashboard", DASHBOARD_AGENT_ID, DASHBOARD_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 8 — UCMV Genie Space Config Generator
# ─────────────────────────────────────────────────────────────────────────────

GENIE_CFG_AGENT_ID = "bi-genie-cfg-agent-001"
GENIE_CFG_TASK_ID = "bi-genie-cfg-task-001"
GENIE_CFG_CREW_ID = "bi-genie-cfg-crew-001"

GENIE_CFG_AGENT = {
    "id": GENIE_CFG_AGENT_ID,
    "name": "Genie Config Generator Agent",
    "role": "Genie Space Configuration Specialist",
    "goal": "Generate a complete Genie Space configuration from the deployed UC Metric Views.",
    "backstory": (
        "You auto-generate Genie Space configuration from deployed UCMV metric views. "
        "ucmv_output is auto-injected from the flow. "
        "Call the UCMV Genie Space Config Generator tool with ZERO arguments."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

GENIE_CFG_TASK = {
    "id": GENIE_CFG_TASK_ID,
    "name": "Generate Genie Space Configuration",
    "description": (
        "Generate a complete Genie Space configuration from the deployed UC Metric Views.\n\n"
        "⚠️ CRITICAL: Call the UCMV Genie Space Config Generator tool with ZERO arguments. "
        "ucmv_output is auto-injected from the flow. "
        "All other parameters (space_title, catalog, schema, warehouse_id) are pre-configured.\n\n"
        "Return the complete Genie Space configuration JSON."
    ),
    "expected_output": (
        "A JSON object with space_title, text_instructions, sample_questions, "
        "example_sqls_json, and join_specs_json — ready to pass to the Genie Space Generator."
    ),
    "agent_id": GENIE_CFG_AGENT_ID,
    "tools": ["93"],
    "tool_configs": {
        "UCMV Genie Space Config Generator": {
            "result_as_answer": True,
            "ucmv_output": None,
            "genie_config_override": None,
            "space_title": "",
            "catalog": "",
            "schema_name": "",
            "warehouse_id": "",
            "databricks_host": "",
            "llm_model": "databricks-claude-sonnet-4",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

GENIE_CFG_CREW = {
    "id": GENIE_CFG_CREW_ID,
    "name": "UCMV Genie Space Config Generator",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [GENIE_CFG_AGENT_ID],
    "task_ids": [GENIE_CFG_TASK_ID],
    "nodes": [
        _agent_node(GENIE_CFG_AGENT_ID, GENIE_CFG_AGENT, 68, 68),
        _task_node(GENIE_CFG_TASK_ID, GENIE_CFG_AGENT_ID, GENIE_CFG_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("genie-cfg", GENIE_CFG_AGENT_ID, GENIE_CFG_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 9 — Genie Space Generator
# ─────────────────────────────────────────────────────────────────────────────

GENIE_GEN_AGENT_ID = "bi-genie-gen-agent-001"
GENIE_GEN_TASK_ID = "bi-genie-gen-task-001"
GENIE_GEN_CREW_ID = "bi-genie-gen-crew-001"

GENIE_GEN_AGENT = {
    "id": GENIE_GEN_AGENT_ID,
    "name": "Genie Space Deployer Agent",
    "role": "Genie Space Deployer",
    "goal": "Deploy a Databricks Genie Space from the generated configuration.",
    "backstory": (
        "You deploy Databricks Genie Spaces from UC Metric View configurations. "
        "All parameters are auto-injected or pre-configured. "
        "Call the Genie Space Generator tool with ZERO arguments."
    ),
    "llm": "databricks-claude-sonnet-4",
    "tools": [],
    "tool_configs": {},
    "max_iter": 3,
    "max_rpm": 10,
    "max_execution_time": 300,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

GENIE_GEN_TASK = {
    "id": GENIE_GEN_TASK_ID,
    "name": "Deploy Genie Space",
    "description": (
        "Deploy a Databricks Genie Space from the generated configuration.\n\n"
        "⚠️ CRITICAL: Call the Genie Space Generator tool with ZERO arguments. "
        "All parameters (ucmv_output, space_title, warehouse_id, catalog, schema, "
        "instructions, questions, SQL snippets) are pre-configured or auto-injected.\n\n"
        "Return the space_id and URL."
    ),
    "expected_output": (
        "A JSON object with space_id, url, operation (created/updated), "
        "table_count, question_count, and cicd_download_url."
    ),
    "agent_id": GENIE_GEN_AGENT_ID,
    "tools": ["92"],
    "tool_configs": {
        "Genie Space Generator": {
            "result_as_answer": True,
            "ucmv_output": None,
            "space_title": "",
            "catalog": "",
            "schema_name": "",
            "warehouse_id": "",
            "databricks_host": "",
            "additional_tables": "",
            "text_instructions": "",
            "join_specs_json": "",
            "sample_questions": "",
            "sql_expressions_json": "",
            "sql_measures_json": "",
            "sql_filters_json": "",
            "example_sqls_json": "",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

GENIE_GEN_CREW = {
    "id": GENIE_GEN_CREW_ID,
    "name": "Genie Space Generator",
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [GENIE_GEN_AGENT_ID],
    "task_ids": [GENIE_GEN_TASK_ID],
    "nodes": [
        _agent_node(GENIE_GEN_AGENT_ID, GENIE_GEN_AGENT, 68, 68),
        _task_node(GENIE_GEN_TASK_ID, GENIE_GEN_AGENT_ID, GENIE_GEN_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("genie-gen", GENIE_GEN_AGENT_ID, GENIE_GEN_TASK_ID)],
}

# ─────────────────────────────────────────────────────────────────────────────
# All crews (ordered for seeding)
# ─────────────────────────────────────────────────────────────────────────────

ALL_CREWS = [
    {"crew": PIPELINE_CONFIG_CREW, "agent": PIPELINE_CONFIG_AGENT, "task": PIPELINE_CONFIG_TASK},
    {"crew": UCMV_GEN_CREW,        "agent": UCMV_GEN_AGENT,        "task": UCMV_GEN_TASK},
    {"crew": UCMV_VAL_CREW,        "agent": UCMV_VAL_AGENT,        "task": UCMV_VAL_TASK},
    {"crew": DEPLOYER_CREW,        "agent": DEPLOYER_AGENT,        "task": DEPLOYER_TASK},
    {"crew": REFERENCES_CREW,      "agent": REFERENCES_AGENT,      "task": REFERENCES_TASK},
    {"crew": MAPPER_CREW,          "agent": MAPPER_AGENT,          "task": MAPPER_TASK},
    {"crew": DASHBOARD_CREW,       "agent": DASHBOARD_AGENT,       "task": DASHBOARD_TASK},
    {"crew": GENIE_CFG_CREW,       "agent": GENIE_CFG_AGENT,       "task": GENIE_CFG_TASK},
    {"crew": GENIE_GEN_CREW,       "agent": GENIE_GEN_AGENT,       "task": GENIE_GEN_TASK},
]

# ─────────────────────────────────────────────────────────────────────────────
# Seeder helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _seed_group(session) -> None:
    result = await session.execute(select(Group).where(Group.id == BI_GROUP_ID))
    if result.scalars().first():
        logger.info(f"Group '{BI_GROUP_ID}' already exists — skipping")
        return
    group = Group(
        id=BI_GROUP["id"],
        name=BI_GROUP["name"],
        status=BI_GROUP["status"],
        description=BI_GROUP["description"],
        auto_created=BI_GROUP["auto_created"],
        created_by_email=BI_GROUP["created_by_email"],
    )
    session.add(group)
    logger.info(f"Created group: {BI_GROUP_ID}")


async def _seed_agent(session, data: dict) -> None:
    result = await session.execute(select(Agent).where(Agent.id == data["id"]))
    if result.scalars().first():
        return
    agent = Agent(
        id=data["id"],
        name=data["name"],
        role=data["role"],
        goal=data["goal"],
        backstory=data.get("backstory", ""),
        group_id=BI_GROUP_ID,
        llm=data.get("llm", "databricks-claude-sonnet-4"),
        tools=data.get("tools", []),
        tool_configs=data.get("tool_configs"),
        max_iter=data.get("max_iter", 25),
        max_rpm=data.get("max_rpm", 10),
        max_execution_time=data.get("max_execution_time", 300),
        verbose=data.get("verbose", True),
        allow_delegation=data.get("allow_delegation", False),
        cache=data.get("cache", True),
        memory=data.get("memory", True),
        embedder_config=data.get("embedder_config"),
        allow_code_execution=False,
        code_execution_mode="safe",
        max_retry_limit=data.get("max_retry_limit", 3),
        use_system_prompt=True,
        respect_context_window=True,
    )
    session.add(agent)


async def _seed_task(session, data: dict) -> None:
    result = await session.execute(select(Task).where(Task.id == data["id"]))
    if result.scalars().first():
        return
    task = Task(
        id=data["id"],
        name=data["name"],
        description=data["description"],
        expected_output=data["expected_output"],
        agent_id=data.get("agent_id"),
        group_id=BI_GROUP_ID,
        tools=data.get("tools", []),
        tool_configs=data.get("tool_configs"),
        async_execution=data.get("async_execution", False),
        context=data.get("context", []),
        config=data.get("config", {}),
        human_input=data.get("config", {}).get("human_input", False),
        markdown=False,
    )
    session.add(task)


async def _seed_crew(session, data: dict) -> None:
    crew_id = uuid.uuid5(uuid.NAMESPACE_DNS, data["id"])
    result = await session.execute(select(Crew).where(Crew.id == crew_id))
    if result.scalars().first():
        return
    crew = Crew(
        id=crew_id,
        name=data["name"],
        group_id=BI_GROUP_ID,
        agent_ids=data.get("agent_ids", []),
        task_ids=data.get("task_ids", []),
        nodes=data.get("nodes", []),
        edges=data.get("edges", []),
        process=data.get("process", "sequential"),
        planning=data.get("planning", False),
        reasoning=data.get("reasoning", False),
        memory=data.get("memory", True),
        verbose=data.get("verbose", True),
    )
    session.add(crew)
    logger.info(f"Created crew: {data['name']}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

async def seed() -> None:
    """Seed the bi-specialist workspace with PBI migration crew templates."""
    logger.info("🚀 Seeding bi-specialist workspace with PBI migration crews...")
    async with async_session_factory() as session:
        try:
            await _seed_group(session)
            for entry in ALL_CREWS:
                await _seed_agent(session, entry["agent"])
                await _seed_task(session, entry["task"])
                await _seed_crew(session, entry["crew"])
            # Enable required tools for the bi-specialist workspace
            from sqlalchemy import select as _select
            from datetime import datetime as _dt
            for _tool_id in BI_TOOLS:
                _exists = await session.execute(
                    _select(GroupTool).where(
                        GroupTool.tool_id == _tool_id,
                        GroupTool.group_id == BI_GROUP_ID,
                    )
                )
                if not _exists.scalar_one_or_none():
                    session.add(GroupTool(
                        tool_id=_tool_id,
                        group_id=BI_GROUP_ID,
                        enabled=True,
                        config={},
                        credentials_status="unknown",
                        created_at=_dt.utcnow(),
                        updated_at=_dt.utcnow(),
                    ))

            await session.commit()
            logger.info(
                f"✅ bi-specialist workspace seeded: "
                f"{len(ALL_CREWS)} crews + {len(BI_TOOLS)} tools enabled"
            )
        except Exception as exc:
            await session.rollback()
            logger.error(f"❌ bi-specialist seeder failed: {exc}")
            raise
