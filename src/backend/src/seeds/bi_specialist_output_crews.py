"""BI Specialist output-stage crews: Databricks Dashboard Creator and the Genie Space
config generator / Genie Space generator.

These run after the UC Metric View crews have been deployed. Split out of
``bi_specialist_crews`` for the file-size ratchet; that module re-exports every
name defined here so the public import path stays stable.
"""

from src.seeds.bi_specialist_crew_builders import (
    DEFAULT_EMBEDDER,
    DEFAULT_TASK_CONFIG,
    _agent_node,
    _agent_to_task_edge,
    _task_node,
)

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
    "llm": "databricks-claude-opus-4-8",
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
    "name": "Rebuild Report as AI/BI Dashboard",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["Databricks Dashboard Creator"],
    "process": "sequential",
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
    "llm": "databricks-claude-opus-4-8",
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
            "llm_model": "databricks-claude-sonnet-4-5",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

GENIE_CFG_CREW = {
    "id": GENIE_CFG_CREW_ID,
    "name": "Prepare Genie Space",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UCMV Genie Space Config Generator"],
    "process": "sequential",
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
    "llm": "databricks-claude-opus-4-8",
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
    "name": "Create Genie Space",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["Genie Space Generator"],
    "process": "sequential",
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
