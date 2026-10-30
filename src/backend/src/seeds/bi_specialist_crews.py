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

And one STANDALONE crew, outside that flow:
 10. UCMV Re-evaluation              (tool 97) — looks BACKWARDS at already-converted
     models and re-tries only the measures that failed then, using today's improved
     transpiler. Run manually or on a schedule after shipping transpiler improvements.
     Read-only: it proposes re-transpilation candidates, never modifies metric views.
 11. UCMV Drift Monitor              (tool 98) — compares DEPLOYED metric views with
     the CURRENT Power BI model and proposes a patch that only adds new / updates
     changed measures on top of the verified YAML. Read-only.

Credential/input placeholders are left empty so the user only needs to fill in
their workspace_id, client_id, client_secret, catalog, schema, warehouse_id etc.
All tool logic and agent configurations are ready to run.
"""

import logging
import uuid
from typing import Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory
from src.models.agent import Agent
from src.models.crew import Crew
from src.models.group import Group
from src.models.group_tool import GroupTool
from src.models.task import Task
from src.seeds.bi_specialist_crew_builders import (
    DEFAULT_EMBEDDER,
    DEFAULT_TASK_CONFIG,
    _agent_node,
    _agent_to_task_edge,
    _task_node,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_DRIFT_AGENT as UCMV_DRIFT_AGENT,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_DRIFT_AGENT_ID as UCMV_DRIFT_AGENT_ID,
)
from src.seeds.bi_specialist_maintenance_crews import UCMV_DRIFT_CREW as UCMV_DRIFT_CREW
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_DRIFT_CREW_ID as UCMV_DRIFT_CREW_ID,
)
from src.seeds.bi_specialist_maintenance_crews import UCMV_DRIFT_TASK as UCMV_DRIFT_TASK
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_DRIFT_TASK_ID as UCMV_DRIFT_TASK_ID,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_REEVAL_AGENT as UCMV_REEVAL_AGENT,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_REEVAL_AGENT_ID as UCMV_REEVAL_AGENT_ID,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_REEVAL_CREW as UCMV_REEVAL_CREW,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_REEVAL_CREW_ID as UCMV_REEVAL_CREW_ID,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_REEVAL_TASK as UCMV_REEVAL_TASK,
)
from src.seeds.bi_specialist_maintenance_crews import (
    UCMV_REEVAL_TASK_ID as UCMV_REEVAL_TASK_ID,
)
from src.seeds.bi_specialist_output_crews import DASHBOARD_AGENT as DASHBOARD_AGENT
from src.seeds.bi_specialist_output_crews import (
    DASHBOARD_AGENT_ID as DASHBOARD_AGENT_ID,
)
from src.seeds.bi_specialist_output_crews import DASHBOARD_CREW as DASHBOARD_CREW
from src.seeds.bi_specialist_output_crews import DASHBOARD_CREW_ID as DASHBOARD_CREW_ID
from src.seeds.bi_specialist_output_crews import DASHBOARD_TASK as DASHBOARD_TASK
from src.seeds.bi_specialist_output_crews import DASHBOARD_TASK_ID as DASHBOARD_TASK_ID
from src.seeds.bi_specialist_output_crews import GENIE_CFG_AGENT as GENIE_CFG_AGENT
from src.seeds.bi_specialist_output_crews import (
    GENIE_CFG_AGENT_ID as GENIE_CFG_AGENT_ID,
)
from src.seeds.bi_specialist_output_crews import GENIE_CFG_CREW as GENIE_CFG_CREW
from src.seeds.bi_specialist_output_crews import GENIE_CFG_CREW_ID as GENIE_CFG_CREW_ID
from src.seeds.bi_specialist_output_crews import GENIE_CFG_TASK as GENIE_CFG_TASK
from src.seeds.bi_specialist_output_crews import GENIE_CFG_TASK_ID as GENIE_CFG_TASK_ID
from src.seeds.bi_specialist_output_crews import GENIE_GEN_AGENT as GENIE_GEN_AGENT
from src.seeds.bi_specialist_output_crews import (
    GENIE_GEN_AGENT_ID as GENIE_GEN_AGENT_ID,
)
from src.seeds.bi_specialist_output_crews import GENIE_GEN_CREW as GENIE_GEN_CREW
from src.seeds.bi_specialist_output_crews import GENIE_GEN_CREW_ID as GENIE_GEN_CREW_ID
from src.seeds.bi_specialist_output_crews import GENIE_GEN_TASK as GENIE_GEN_TASK
from src.seeds.bi_specialist_output_crews import GENIE_GEN_TASK_ID as GENIE_GEN_TASK_ID

# Tools required by the PBI migration pipeline — pre-enabled for bi-specialist.
# These become GroupTool rows (explicit (group_id, tool_id) eligibility), so a
# tool missing here is filtered out of the workspace's Tools list and the crew UI.
# 97 = UCMV Re-evaluation, 98 = UCMV Drift Monitor (the standalone crews below).
BI_TOOLS = [78, 86, 88, 90, 91, 92, 93, 94, 95, 97, 98]

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

# Neutral, ready-to-edit example of the UC Metric View Generator's optional
# `domain_context` field: free-text model knowledge fed verbatim into the DAX→SQL
# translation prompt so the LLM translates with the model's business semantics.
# This is a TEMPLATE showing the shape — replace each line with your own model's
# calendar, vocabulary, naming conventions and dimension members.
EXAMPLE_DOMAIN_CONTEXT = """# Domain context — <your model / report name>

Fiscal calendar: describe it (e.g. 4-4-5). Define how "YTD <year>" is computed
(e.g. periods 001..latest closed month) and any period-column overrides
(e.g. latest_month_label → IsCurrentMonth).

Vocabulary / acronyms (one per line):
- <ACRONYM> = <what it means in this model>
- <ACRONYM> = <what it means in this model>

Measure / naming conventions:
- Scenario suffixes, e.g. "… PY" = prior year, "… Bud" = budget, "… Act" = actual.
- Units, e.g. volumes are counts unless suffixed _hl (hectolitres).
- Business rules, e.g. prefer booked/stored columns over recomputing from components.

Key dimensions and their members:
- <dimension>: '<member1>','<member2>', …
- Regions / entities: '<region1>','<region2>', …
"""

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
    "name": "1 · Extract Power BI Model",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UCMV — Generate Pipeline Config (API-Direct)"],
    "process": "sequential",
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [PIPELINE_CONFIG_AGENT_ID],
    "task_ids": [PIPELINE_CONFIG_TASK_ID],
    "nodes": [
        _agent_node(PIPELINE_CONFIG_AGENT_ID, PIPELINE_CONFIG_AGENT, 68, 68),
        _task_node(
            PIPELINE_CONFIG_TASK_ID,
            PIPELINE_CONFIG_AGENT_ID,
            PIPELINE_CONFIG_TASK,
            368,
            68,
        ),
    ],
    "edges": [
        _agent_to_task_edge(
            "pipeline-config", PIPELINE_CONFIG_AGENT_ID, PIPELINE_CONFIG_TASK_ID
        )
    ],
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
    "llm": "databricks-claude-opus-4-8",
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
            # Distinct admin Service Principal for the MQuery Admin Scanner's
            # 3rd fallback tier (retries the scan itself with admin rights when
            # client_id/client_secret and Fabric TMDL both fail) — mirrors
            # Pipeline Config Generator's admin_client_id/admin_client_secret.
            "admin_client_id": "",
            "admin_client_secret": "",
            "catalog": "",
            "schema_name": "",
            "use_llm_fallback": True,
            "llm_model": "databricks-claude-sonnet-4-5",
            # Visual-usage triage: set True to skip measures not shown/filtered in
            # any visual (directly or via a dependency chain). Needs the report's
            # visual-usage data; no-op without it. Base measures always kept.
            "prune_non_visual_measures": False,
            # Optional free-text model knowledge fed into the DAX→SQL translation.
            # Ships as a neutral template to edit (or clear) per model.
            "domain_context": EXAMPLE_DOMAIN_CONTEXT,
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
    "name": "2 · Convert to Metric Views",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UC Metric View Generator — JSON Mode"],
    "process": "sequential",
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
# Crew 2b — UC Metric View Generator (Reconciled)
# Same generator, but with the iterative reconciliation loop turned ON: after each
# generation cycle it deploys the metric view and reconciles it cell-by-cell
# against the live Power BI model, feeding mismatches back to refine failing
# measures (up to max_reconciliation_cycles or the target %). Because the
# generator deploys internally each cycle, this crew does NOT need the separate
# Metric View Deployer step. The operator must fill warehouse_id + reference_years
# (+ the Power BI credentials the plain generator already needs).
# ─────────────────────────────────────────────────────────────────────────────
UCMV_GEN_RECON_AGENT_ID = "bi-ucmv-gen-recon-agent-001"
UCMV_GEN_RECON_TASK_ID = "bi-ucmv-gen-recon-task-001"
UCMV_GEN_RECON_CREW_ID = "bi-ucmv-gen-recon-crew-001"

UCMV_GEN_RECON_AGENT = {
    "id": UCMV_GEN_RECON_AGENT_ID,
    "name": "UC Metric View Generator (Reconciled) Agent",
    "role": "UC Metric View Generator (Reconciled)",
    "goal": (
        "Generate UC Metric View YAML definitions for all fact tables and refine "
        "them against the live Power BI model until they reconcile."
    ),
    "backstory": (
        "You are a Databricks UC Metric View specialist. You translate Power BI DAX "
        "measures into UCMV YAML with the UC Metric View Generator tool, which — in "
        "this crew — iteratively deploys and reconciles each metric view against the "
        "live Power BI model and refines the measures that do not match. "
        "Call the tool with ZERO arguments — all parameters are pre-configured."
    ),
    "llm": "databricks-claude-opus-4-8",
    "tools": [],
    "tool_configs": {},
    "max_iter": 10,
    "max_rpm": 10,
    # The loop deploys + reconciles every cycle, so allow more wall-clock time.
    "max_execution_time": 1800,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

UCMV_GEN_RECON_TASK = {
    "id": UCMV_GEN_RECON_TASK_ID,
    "name": "Generate + reconcile UC Metric View definitions",
    "description": (
        "Generate UC Metric View YAML definitions for all fact tables AND reconcile "
        "them against the live Power BI model.\n\n"
        "⚠️ CRITICAL: Call the UC Metric View Generator tool with ZERO arguments. "
        "ALL parameters (credentials, catalog, schema, warehouse_id, reference_years, "
        "reconciliation settings) are ALREADY PRE-CONFIGURED in the tool-task-form.\n\n"
        "The tool runs the iterative loop internally (generate → deploy → reconcile → "
        "refine) and deploys each cycle, so NO separate deployer step is needed. "
        "Return the complete tool output (per-cycle history + final reconciliation)."
    ),
    "expected_output": (
        "A JSON object with the final UCMV YAML per fact table, the per-cycle "
        "reconciliation history (overall cell-alignment %, measures at 100%), and "
        "the stop reason."
    ),
    "agent_id": UCMV_GEN_RECON_AGENT_ID,
    "tools": ["86"],
    "tool_configs": {
        "UC Metric View Generator": {
            "result_as_answer": True,
            "workspace_id": "",
            "dataset_id": "",
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "admin_client_id": "",
            "admin_client_secret": "",
            "catalog": "",
            "schema_name": "",
            "use_llm_fallback": True,
            "llm_model": "databricks-claude-sonnet-4-5",
            # Iterative reconciliation — the operator fills warehouse_id; without
            # it the tool degrades to single-pass. reference_years is OPTIONAL
            # (empty = compare all years).
            "enable_reconciliation": True,
            "max_reconciliation_cycles": 5,
            "reconciliation_target_pct": 100,
            "warehouse_id": "",
            "reference_years": "",  # optional, e.g. "[2025, 2026]" (empty = all years)
            # Visual-usage triage: set True to skip measures not shown/filtered in
            # any visual (directly or via a dependency chain) — needs the report's
            # visual-usage data; no-op without it. Base measures always kept.
            "prune_non_visual_measures": False,
            # Optional free-text model knowledge fed into the DAX→SQL translation.
            # Ships as a neutral template to edit (or clear) per model.
            "domain_context": EXAMPLE_DOMAIN_CONTEXT,
            # JSON-mode handoff fields (filled by the preceding Pipeline Config crew).
            "config_json": "{}",
            "measures_json": "[]",
            "mquery_json": "[]",
            "relationships_json": "[]",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

UCMV_GEN_RECON_CREW = {
    "id": UCMV_GEN_RECON_CREW_ID,
    "name": "2 · Convert to Metric Views + Verify Numbers",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UC Metric View Generator — Reconciled (iterative PBI check)"],
    "process": "sequential",
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [UCMV_GEN_RECON_AGENT_ID],
    "task_ids": [UCMV_GEN_RECON_TASK_ID],
    "nodes": [
        _agent_node(UCMV_GEN_RECON_AGENT_ID, UCMV_GEN_RECON_AGENT, 68, 68),
        _task_node(
            UCMV_GEN_RECON_TASK_ID,
            UCMV_GEN_RECON_AGENT_ID,
            UCMV_GEN_RECON_TASK,
            368,
            68,
        ),
    ],
    "edges": [
        _agent_to_task_edge(
            "ucmv-gen-recon", UCMV_GEN_RECON_AGENT_ID, UCMV_GEN_RECON_TASK_ID
        )
    ],
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
    "name": "3 · Review Metric View Quality",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UCMV Quality Validator"],
    "process": "sequential",
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
    "name": "4 · Deploy Metric Views",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["Metric View Deployer"],
    "process": "sequential",
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

REFERENCES_TASK = {
    "id": REFERENCES_TASK_ID,
    "name": "Extract Power BI Report References",
    "description": (
        'IMMEDIATELY call the "Power BI Report References Tool" with parameters defined '
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
    "name": "Find Report Fields & Visuals",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["references"],
    "process": "sequential",
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [REFERENCES_AGENT_ID],
    "task_ids": [REFERENCES_TASK_ID],
    "nodes": [
        _agent_node(REFERENCES_AGENT_ID, REFERENCES_AGENT, 68, 68),
        _task_node(REFERENCES_TASK_ID, REFERENCES_AGENT_ID, REFERENCES_TASK, 368, 68),
    ],
    "edges": [
        _agent_to_task_edge("references", REFERENCES_AGENT_ID, REFERENCES_TASK_ID)
    ],
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
            "llm_model": "databricks-claude-sonnet-4-5",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

MAPPER_CREW = {
    "id": MAPPER_CREW_ID,
    "name": "Map Report Visuals to Metric Views",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["PBI Visual-UCMV Mapper"],
    "process": "sequential",
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
# All crews (ordered for seeding)
# ─────────────────────────────────────────────────────────────────────────────

ALL_CREWS: List[Dict[str, dict]] = [
    {
        "crew": PIPELINE_CONFIG_CREW,
        "agent": PIPELINE_CONFIG_AGENT,
        "task": PIPELINE_CONFIG_TASK,
    },
    {"crew": UCMV_GEN_CREW, "agent": UCMV_GEN_AGENT, "task": UCMV_GEN_TASK},
    {
        "crew": UCMV_GEN_RECON_CREW,
        "agent": UCMV_GEN_RECON_AGENT,
        "task": UCMV_GEN_RECON_TASK,
    },
    {"crew": UCMV_VAL_CREW, "agent": UCMV_VAL_AGENT, "task": UCMV_VAL_TASK},
    {"crew": DEPLOYER_CREW, "agent": DEPLOYER_AGENT, "task": DEPLOYER_TASK},
    {"crew": REFERENCES_CREW, "agent": REFERENCES_AGENT, "task": REFERENCES_TASK},
    {"crew": MAPPER_CREW, "agent": MAPPER_AGENT, "task": MAPPER_TASK},
    {"crew": DASHBOARD_CREW, "agent": DASHBOARD_AGENT, "task": DASHBOARD_TASK},
    {"crew": GENIE_CFG_CREW, "agent": GENIE_CFG_AGENT, "task": GENIE_CFG_TASK},
    {"crew": GENIE_GEN_CREW, "agent": GENIE_GEN_AGENT, "task": GENIE_GEN_TASK},
    # Standalone (not part of the conversion flow) — run manually or on a schedule.
    {"crew": UCMV_REEVAL_CREW, "agent": UCMV_REEVAL_AGENT, "task": UCMV_REEVAL_TASK},
    {"crew": UCMV_DRIFT_CREW, "agent": UCMV_DRIFT_AGENT, "task": UCMV_DRIFT_TASK},
]

# ─────────────────────────────────────────────────────────────────────────────
# Seeder helpers
# ─────────────────────────────────────────────────────────────────────────────


async def _seed_group(session: AsyncSession) -> None:
    result = await session.execute(select(Group).where(Group.id == BI_GROUP_ID))
    if result.scalars().first():
        logger.info("Group '%s' already exists — skipping", BI_GROUP_ID)
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
    logger.info("Created group: %s", BI_GROUP_ID)


async def _seed_agent(session: AsyncSession, data: dict) -> None:
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
        llm=data.get("llm", "databricks-claude-sonnet-4-5"),
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


async def _seed_task(session: AsyncSession, data: dict) -> None:
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


async def _seed_crew(session: AsyncSession, data: dict) -> None:
    crew_id = uuid.uuid5(uuid.NAMESPACE_DNS, data["id"])
    result = await session.execute(select(Crew).where(Crew.id == crew_id))
    existing = result.scalars().first()
    seed_nodes = data.get("nodes", [])
    seed_edges = data.get("edges", [])
    if existing:
        # Self-heal. This seeder is otherwise create-only, so a crew that first
        # landed with EMPTY canvas nodes (an older build before the nodes were
        # added here, or a save path that dropped them) would open blank on the
        # Agent Builder canvas forever — re-seeding never backfilled it. Fill the
        # nodes ONLY when the stored crew has none, so a crew the user has since
        # edited (and which therefore has nodes) is never clobbered.
        # Rename a crew still carrying an OLDER seeded name. Matching the exact
        # old name means a crew the user renamed themselves is left alone.
        if getattr(existing, "name", None) in data.get("previous_names", []):
            logger.info("Renamed crew: %r -> %r", existing.name, data["name"])
            existing.name = data["name"]
        if not existing.nodes and seed_nodes:
            existing.nodes = seed_nodes
            existing.edges = seed_edges
            if not existing.agent_ids:
                existing.agent_ids = data.get("agent_ids", [])
            if not existing.task_ids:
                existing.task_ids = data.get("task_ids", [])
            logger.info("Backfilled canvas nodes for existing crew: %s", data["name"])
        return
    crew = Crew(
        id=crew_id,
        name=data["name"],
        group_id=BI_GROUP_ID,
        agent_ids=data.get("agent_ids", []),
        task_ids=data.get("task_ids", []),
        nodes=seed_nodes,
        edges=seed_edges,
        process=data.get("process", "sequential"),
        reasoning=data.get("reasoning", False),
        memory=data.get("memory", True),
        verbose=data.get("verbose", True),
    )
    session.add(crew)
    logger.info("Created crew: %s", data["name"])


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
            from datetime import datetime as _dt

            from sqlalchemy import select as _select

            for _tool_id in BI_TOOLS:
                _exists = await session.execute(
                    _select(GroupTool).where(
                        GroupTool.tool_id == _tool_id,
                        GroupTool.group_id == BI_GROUP_ID,
                    )
                )
                if not _exists.scalar_one_or_none():
                    session.add(
                        GroupTool(
                            tool_id=_tool_id,
                            group_id=BI_GROUP_ID,
                            enabled=True,
                            config={},
                            credentials_status="unknown",
                            created_at=_dt.utcnow(),
                            updated_at=_dt.utcnow(),
                        )
                    )

            await session.commit()
            logger.info(
                "✅ bi-specialist workspace seeded: %s crews + %s tools enabled",
                len(ALL_CREWS),
                len(BI_TOOLS),
            )
        except Exception as exc:
            await session.rollback()
            logger.error("❌ bi-specialist seeder failed: %s", exc)
            raise
