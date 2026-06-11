"""Regression tests for the template diet (LLM-002 / LLM-010).

The generation templates are resent verbatim on EVERY generation call, so
their size is a per-call prompt-token tax. These tests pin the budgets so
the templates cannot silently regrow.
"""

from src.seeds.prompt_templates import (
    DEFAULT_TEMPLATES,
    GENERATE_CREW_PLAN_TEMPLATE,
    GENERATE_TASK_TEMPLATE,
)


def test_generate_task_template_stays_on_a_diet():
    # Was 14,401 chars (~3.6k tokens) with a static 9-tool catalog and 5
    # few-shot examples; trimmed to ~7k. Budget leaves modest headroom.
    assert len(GENERATE_TASK_TEMPLATE) < 8000

    # The static per-tool catalog must NOT come back — the dynamically
    # appended "Available tools" list is the single source of truth.
    assert "TOOL CATALOG" not in GENERATE_TASK_TEMPLATE
    assert "PowerBIAnalysisTool" not in GENERATE_TASK_TEMPLATE

    # The contract pieces survive.
    for marker in ("QUALITY REQUIREMENTS", "DELIVERABLE OUTPUT RULE",
                   "llm_guardrail", "advanced_config", "Available tools"):
        assert marker in GENERATE_TASK_TEMPLATE, marker


def test_generate_crew_plan_template_is_lightweight_and_seeded():
    assert len(GENERATE_CREW_PLAN_TEMPLATE) < 2000
    assert "PLAN OUTLINE" in GENERATE_CREW_PLAN_TEMPLATE
    assert "assigned_agent" in GENERATE_CREW_PLAN_TEMPLATE

    names = {t["name"] for t in DEFAULT_TEMPLATES}
    assert "generate_crew_plan" in names
