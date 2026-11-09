"""BI Specialist crew 4: finish the generated metric views to 100 % reconciled.

Sits between "3 · Review Metric View Quality" and "5 · Deploy Metric Views". The UC Metric View
Generator gets most measures right in one pass; this crew's agent closes the rest with the UCMV
Workbench (tool 101): per view it works the open measures (untranslated, or below 100 % in the
reconciliation), writes their SQL, deploys to a SANDBOX schema, reconciles against Power BI and
repeats until every measure is at 100 % or its gap is classified with a note. The agent carries
the ``powerbi-to-metric-views`` skill (the reference migration process). Its final answer is the
workbench export, which "5 · Deploy Metric Views" deploys to the target schema.
"""

from src.seeds.bi_specialist_crew_builders import (
    DEFAULT_EMBEDDER,
    DEFAULT_TASK_CONFIG,
    _agent_node,
    _agent_to_task_edge,
    _task_node,
)

FINISH_AGENT_ID = "bi-ucmv-finish-agent-001"
FINISH_TASK_ID = "bi-ucmv-finish-task-001"
FINISH_CREW_ID = "bi-ucmv-finish-crew-001"

FINISH_AGENT = {
    "id": FINISH_AGENT_ID,
    "name": "Metric View Finisher Agent",
    "role": "Metric View Migration Engineer",
    "goal": (
        "Bring every generated UC metric view to 100 % agreement with Power BI, or classify each "
        "remaining gap with evidence, without ever changing a measure that already reconciles."
    ),
    "backstory": (
        "You finish Power BI to UC metric view migrations. You work with the UCMV Workbench tool, "
        "one small step at a time, and you follow the powerbi-to-metric-views skill (load it first). "
        "You translate DAX into metric view SQL carefully: filter context, BLANK vs zero, DIVIDE, "
        "time intelligence and relationships matter. You never invent a reconciliation result and "
        "never classify a gap as data drift or a Power BI defect without a reason you can state."
    ),
    "llm": "databricks-claude-opus-4-8",
    "skills": ["powerbi-to-metric-views"],
    "tools": [],
    "tool_configs": {},
    "max_iter": 80,
    "max_rpm": 20,
    "max_execution_time": 3600,
    "verbose": True,
    "allow_delegation": False,
    "cache": False,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

FINISH_TASK = {
    "id": FINISH_TASK_ID,
    "name": "Finish metric views to 100 % reconciled",
    "description": (
        "Finish the metric views generated in the previous step with the UCMV Workbench tool. "
        "Credentials, the SANDBOX catalog/schema and the warehouse are in the tool task form; you only "
        "pass action, view, measure and expression arguments.\n\n"
        "1. action=overview: list the views.\n"
        "2. For each view, largest gaps first:\n"
        "   a. action=deploy, then action=reconcile: the baseline (which measures are already at 100 %).\n"
        "   b. action=gaps: untranslated measures (DAX, reason, proposal) and measures below 100 %.\n"
        "   c. For one measure at a time: action=show (its DAX and current entry), then "
        "action=set_measure with name, expr and pbi_measure (the Power BI measure it reproduces).\n"
        "   d. action=deploy (fix any error it reports), action=reconcile, and look at that measure again.\n"
        "   e. A measure that cannot reach 100 % after a few honest attempts: action=classify with "
        "data_drift | pbi_defect | structural | out_of_scope and a note saying what you checked.\n"
        "   f. Never change a measure that is already at 100 %.\n"
        "3. action=report: continue until open_gaps is 0 and every view is reconciled, or until you "
        "have classified everything you cannot fix.\n"
        "4. Finish with action=export and return its output UNCHANGED as your final answer."
    ),
    "expected_output": (
        "The UCMV Workbench export JSON: yaml (final metric view YAML per view), pbi_ucmv_mapping and "
        "finishing_report (open gaps, reconciliation per view, classified gaps with notes, changes)."
    ),
    "agent_id": FINISH_AGENT_ID,
    "tools": ["101"],
    "tool_configs": {
        "UCMV Workbench": {
            "result_as_answer": False,
            # Flow-injected from "2 · Convert to Metric Views" (or read from this run / the
            # latest generator output in the database).
            "ucmv_output": None,
            # SANDBOX target: the workbench deploys here on every iteration.
            "catalog": "",
            "schema_name": "",
            "warehouse_id": "",
            "databricks_host": "",
            # Power BI side of the reconciliation.
            "workspace_id": "",
            "dataset_id": "",
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "reference_years": "",  # optional, e.g. "[2025, 2026]"
            "statistics_only": True,
            "confidential_values": False,
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

FINISH_CREW = {
    "id": FINISH_CREW_ID,
    "name": "4 · Finish Metric Views to 100 %",
    "process": "sequential",
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [FINISH_AGENT_ID],
    "task_ids": [FINISH_TASK_ID],
    "nodes": [
        _agent_node(FINISH_AGENT_ID, FINISH_AGENT, 68, 68),
        _task_node(FINISH_TASK_ID, FINISH_AGENT_ID, FINISH_TASK, 368, 68),
    ],
    "edges": [_agent_to_task_edge("ucmv-finish", FINISH_AGENT_ID, FINISH_TASK_ID)],
}
