"""BI Specialist standalone maintenance crews: UCMV Re-evaluation and Drift Monitor.

Both crews sit OUTSIDE the PBI conversion flow and are read-only: they run manually
or on a schedule and only propose changes. Split out of ``bi_specialist_crews``
for the file-size ratchet; that module re-exports every name defined here so the
public import path stays stable.
"""

from src.seeds.bi_specialist_crew_builders import (
    DEFAULT_EMBEDDER,
    DEFAULT_TASK_CONFIG,
    _agent_node,
    _agent_to_task_edge,
    _task_node,
)

# ─────────────────────────────────────────────────────────────────────────────
# Crew 10 — UCMV Re-evaluation (recoverable measures)
# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE, not part of the conversion flow: it looks BACKWARDS at models that
# were already converted and re-tries only the measures that failed back then,
# using today's (improved) transpiler. Run it manually or on a schedule after
# shipping transpiler improvements. Read-only — it proposes re-transpilation
# candidates and never modifies metric views.
# Docs: src/docs/powerbi/ucmv-reevaluation-recoverable-measures.md

UCMV_REEVAL_AGENT_ID = "bi-ucmv-reeval-agent-001"
UCMV_REEVAL_TASK_ID = "bi-ucmv-reeval-task-001"
UCMV_REEVAL_CREW_ID = "bi-ucmv-reeval-crew-001"

UCMV_REEVAL_AGENT = {
    "id": UCMV_REEVAL_AGENT_ID,
    "name": "UCMV Re-evaluation Agent",
    "role": "Metric View Recovery Analyst",
    "goal": (
        "Find previously-untranslatable DAX measures that today's improved "
        "transpiler can now recover, and report them for human review."
    ),
    "backstory": (
        "You track how Kasal's DAX→UC Metric View capability improves over time. "
        "Models converted months ago still carry measures that failed back then but "
        "are translatable today. You replay the stored conversion history and report "
        "which measures are now recoverable. "
        "Call the UCMV Re-evaluation tool with ZERO arguments — the group scope and "
        "settings come from the tool config. Report the findings verbatim; do NOT "
        "invent SQL and do NOT claim anything was applied — the tool only proposes."
    ),
    "llm": "databricks-claude-opus-4-8",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 600,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

UCMV_REEVAL_TASK = {
    "id": UCMV_REEVAL_TASK_ID,
    "name": "Find measures the improved transpiler can now recover",
    "description": (
        "Scan the stored UCMV conversion history and re-try ONLY the measures that "
        "previously failed to transpile, using today's transpiler.\n\n"
        "⚠️ CRITICAL: Call the UCMV Re-evaluation tool with ZERO arguments — the "
        "group scope and settings are supplied by the tool config.\n\n"
        "The tool is READ-ONLY. It proposes re-transpilation candidates; it never "
        "modifies metric views. Return its report as-is, highlighting per dataset "
        "which measures are now recoverable, the reason each failed before, and the "
        "new SQL. If nothing is recoverable, say so plainly — that is a valid result "
        "(either the transpiler has not changed since those runs, or the remaining "
        "gaps are permanent limitations)."
    ),
    "expected_output": (
        "A JSON report with the capability fingerprint, per-dataset lists of "
        "newly-recoverable measures (name, previous skip reason, new SQL, impact), "
        "and a summary of datasets scanned / measures retried / measures recovered."
    ),
    "agent_id": UCMV_REEVAL_AGENT_ID,
    "tools": ["97"],
    "tool_configs": {
        "UCMV Re-evaluation": {
            "result_as_answer": True,
            # Scope: leave group_id empty to have the runtime group context apply;
            # set dataset_ids to target specific models.
            "group_id": None,
            "dataset_ids": None,
            # Cost guards. use_llm=False keeps a scheduled sweep free (deterministic
            # fast-path only); flip it on for a deeper, token-spending pass.
            "use_llm": False,
            "include_impossible": False,
            "max_measures_per_dataset": 200,
            "max_datasets": 50,
            # force=True re-tries even when the transpiler has NOT changed since the
            # stored run. Useful to verify the sweep end-to-end; leave False in
            # production so the sweep only reports genuine capability gains.
            "force": False,
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

UCMV_REEVAL_CREW = {
    "id": UCMV_REEVAL_CREW_ID,
    "name": "Retry Failed Measures",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UCMV Re-evaluation"],
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [UCMV_REEVAL_AGENT_ID],
    "task_ids": [UCMV_REEVAL_TASK_ID],
    "nodes": [
        _agent_node(UCMV_REEVAL_AGENT_ID, UCMV_REEVAL_AGENT, 68, 68),
        _task_node(
            UCMV_REEVAL_TASK_ID, UCMV_REEVAL_AGENT_ID, UCMV_REEVAL_TASK, 368, 68
        ),
    ],
    "edges": [
        _agent_to_task_edge("ucmv-reeval", UCMV_REEVAL_AGENT_ID, UCMV_REEVAL_TASK_ID)
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Crew 11 — UCMV Drift Monitor (deployed views vs today's Power BI model)
# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE, run manually or on a schedule: compares 1..N deployed metric views
# of one report with the CURRENT Power BI model, and proposes a patch that only
# adds new / re-translates changed measures (and the joins they need) on top of
# the verified deployed YAML. Read-only — deploy the reviewed proposal with the
# "4 · Deploy Metric Views" crew.

UCMV_DRIFT_AGENT_ID = "bi-ucmv-drift-agent-001"
UCMV_DRIFT_TASK_ID = "bi-ucmv-drift-task-001"
UCMV_DRIFT_CREW_ID = "bi-ucmv-drift-crew-001"

UCMV_DRIFT_AGENT = {
    "id": UCMV_DRIFT_AGENT_ID,
    "name": "UCMV Drift Monitor Agent",
    "role": "Metric View Drift Analyst",
    "goal": (
        "Detect where deployed UC metric views have drifted from their Power BI model "
        "and propose a minimal patch that brings them back in sync."
    ),
    "backstory": (
        "You keep governed metric views aligned with the Power BI reports they were "
        "migrated from. The deployed metric view is the verified baseline: you never "
        "change what a human already verified, you only add what Power BI gained and "
        "update what Power BI changed. "
        "Call the UCMV Drift Monitor tool with ZERO arguments — the metric views, Power "
        "BI credentials and warehouse are pre-configured in the tool task form. Report "
        "the findings verbatim; do NOT invent SQL and do NOT claim anything was deployed."
    ),
    "llm": "databricks-claude-opus-4-8",
    "tools": [],
    "tool_configs": {},
    "max_iter": 5,
    "max_rpm": 10,
    "max_execution_time": 1800,
    "verbose": True,
    "allow_delegation": False,
    "cache": True,
    "memory": False,
    "embedder_config": DEFAULT_EMBEDDER,
    "max_retry_limit": 3,
}

UCMV_DRIFT_TASK = {
    "id": UCMV_DRIFT_TASK_ID,
    "name": "Detect drift between deployed metric views and Power BI",
    "description": (
        "Compare the deployed UC metric views with the CURRENT Power BI semantic model "
        "and propose the changes needed to bring them back in sync.\n\n"
        "⚠️ CRITICAL: Call the UCMV Drift Monitor tool with ZERO arguments — the metric "
        "view names, Power BI credentials and SQL warehouse are supplied by the tool "
        "config.\n\n"
        "The tool is READ-ONLY. It returns, per metric view, which measures are "
        "unchanged, changed in Power BI, new in Power BI, removed from Power BI or "
        "unassigned, plus a proposed YAML that keeps the deployed definition intact and "
        "only appends new / re-translated measures and needed joins. Return its report "
        "as-is. If nothing drifted, say so plainly — that is a valid result."
    ),
    "expected_output": (
        "A JSON drift report: per-view measure statuses (with today's DAX, the deployed "
        "SQL and any proposed SQL), the proposed YAML per view, unassigned measures, and "
        "a summary of what changed vs what was added."
    ),
    "agent_id": UCMV_DRIFT_AGENT_ID,
    "tools": ["98"],
    "tool_configs": {
        "UCMV Drift Monitor": {
            "result_as_answer": True,
            # 1..N deployed metric views of ONE report: catalog.schema.view, one per line.
            "ucmv_names": "",
            "warehouse_id": "",
            "databricks_host": "",
            # Power BI connection — same fields as the Pipeline Config Generator.
            "workspace_id": "",
            "dataset_id": "",
            "report_id": "",
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "admin_client_id": "",
            "admin_client_secret": "",
            # Views deployed before DAX fingerprinting: batched LLM comparison of
            # today's DAX vs the deployed SQL. Costs tokens; turn off for a free sweep.
            "llm_compare_legacy": True,
            # LLM-suspected changes are shown with a suggested replacement but only
            # patched into the proposal when this is on.
            "apply_suspected_changes": False,
            "llm_model": "databricks-claude-sonnet-4-5",
        }
    },
    "config": DEFAULT_TASK_CONFIG,
}

UCMV_DRIFT_CREW = {
    "id": UCMV_DRIFT_CREW_ID,
    "name": "Continuous Drift Monitor",
    # Older seeded name(s) — renamed in place on existing installs.
    "previous_names": ["UCMV Drift Monitor"],
    "process": "sequential",
    "planning": False,
    "reasoning": False,
    "memory": False,
    "verbose": True,
    "agent_ids": [UCMV_DRIFT_AGENT_ID],
    "task_ids": [UCMV_DRIFT_TASK_ID],
    "nodes": [
        _agent_node(UCMV_DRIFT_AGENT_ID, UCMV_DRIFT_AGENT, 68, 68),
        _task_node(UCMV_DRIFT_TASK_ID, UCMV_DRIFT_AGENT_ID, UCMV_DRIFT_TASK, 368, 68),
    ],
    "edges": [
        _agent_to_task_edge("ucmv-drift", UCMV_DRIFT_AGENT_ID, UCMV_DRIFT_TASK_ID)
    ],
}
