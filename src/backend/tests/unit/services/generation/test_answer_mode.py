"""What an answer mode actually changes about a generated crew.

Before this pass, ``research`` and ``deep`` differed by one string
(``reasoning_effort``) and were byte-identical on a model without a native
reasoning budget. These tests pin the differences that now exist.
"""

import copy

from src.services.generation.crew.answer_mode import (
    PERPLEXITY_TOOL_ID,
    apply_answer_mode,
)


def _fixture():
    agents = {
        "agent_a1": {
            "role": "Researcher",
            "goal": "g",
            "backstory": "b",
            "tools": ["26", PERPLEXITY_TOOL_ID],
        }
    }
    tasks = {
        "task_t1": {
            "id": "t1",
            "description": "Find facts",
            "expected_output": "A report",
            "tools": [PERPLEXITY_TOOL_ID],
            "output_file": "output/t1.md",
        }
    }
    generated = {
        "task_t1": {"llm_guardrail": {"description": "Every claim must cite a source"}}
    }
    return agents, tasks, generated


class TestGuardrailCarryThrough:
    def test_generated_guardrail_reaches_the_task(self):
        """The audit's headline finding: the guardrail was generated for every
        task, persisted, and then dropped by this builder, so deep runs
        executed ungated against a criterion already written for them."""
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        assert tasks["task_t1"]["llm_guardrail"]["description"].startswith(
            "Every claim"
        )

    def test_absent_guardrail_is_not_invented(self):
        agents, tasks, _ = _fixture()
        apply_answer_mode("deep", agents, tasks, {})
        assert "llm_guardrail" not in tasks["task_t1"]

    def test_retries_and_degrade_policy_are_set(self):
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        entry = tasks["task_t1"]
        assert entry["max_retries"] == 3
        assert entry["guardrail_on_exhausted"] == "degrade"
        assert entry["on_budget_exceeded"] == "degrade"


class TestOnlyDeepIsTouched:
    def test_research_is_completely_untouched(self):
        """SCOPE: this change is Deep Research only. Research keeps its bare
        engine defaults, its ungated tasks and its tool set — no guardrail, no
        retries, no caps, nothing removed."""
        agents, tasks, generated = _fixture()
        before = (copy.deepcopy(agents), copy.deepcopy(tasks))
        apply_answer_mode("research", agents, tasks, generated)
        assert (agents, tasks) == before

    def test_chat_gets_nothing(self):
        """The light path must stay sub-second and is deliberately separate."""
        agents, tasks, generated = _fixture()
        before = (copy.deepcopy(agents), copy.deepcopy(tasks))
        apply_answer_mode("chat", agents, tasks, generated)
        assert (agents, tasks) == before

    def test_unknown_mode_gets_nothing(self):
        agents, tasks, generated = _fixture()
        before = (copy.deepcopy(agents), copy.deepcopy(tasks))
        apply_answer_mode("nonsense", agents, tasks, generated)
        assert (agents, tasks) == before


class TestDeepOnlyBehaviour:
    def test_deep_gets_the_envelope_and_the_gate(self):
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        entry = tasks["task_t1"]
        assert entry["output_schema"]["required"] == ["summary", "findings"]
        assert len(entry["gate"]["require"]) == 4
        assert "DEEP RESEARCH CONTRACT" in entry["description"]

    def test_deep_output_file_becomes_json(self):
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        assert tasks["task_t1"]["output_file"] == "output/t1.json"

    def test_envelope_is_copied_not_shared(self):
        """Two tasks mutating one shared dict would be a cross-task bug that
        only shows up with more than one task."""
        agents, tasks, generated = _fixture()
        tasks["task_t2"] = dict(tasks["task_t1"], id="t2")
        apply_answer_mode("deep", agents, tasks, generated)
        assert tasks["task_t1"]["gate"] is not tasks["task_t2"]["gate"]


class TestBudgets:
    def test_agent_caps_come_from_the_profile(self):
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["max_iter"] == 30
        assert agents["agent_a1"]["max_execution_time"] == 600

    def test_an_explicit_plan_value_wins(self):
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["max_iter"] = 7
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["max_iter"] == 7


class TestToolPolicy:
    def test_deep_upgrades_perplexity_without_changing_the_tool_list(self):
        """Deep reconfigures what the generator already picked. It does not add
        tools, and — unlike an earlier version of this pass — it does not take
        any away from other modes."""
        agents, tasks, generated = _fixture()
        before = list(agents["agent_a1"]["tools"])
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["tools"] == before
        config = agents["agent_a1"]["tool_configs"]["PerplexityTool"]
        assert config["model"] == "sonar-deep-research"

    def test_perplexity_config_is_not_invented_without_the_tool(self):
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["tools"] = []
        apply_answer_mode("deep", agents, tasks, generated)
        assert "tool_configs" not in agents["agent_a1"]


class TestIdempotence:
    def test_a_second_pass_changes_nothing(self):
        """The pass runs from generation AND from execution config adaptation,
        so a config that took both paths must not get the contract appended to
        its description twice."""
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        after_first = copy.deepcopy(tasks)
        apply_answer_mode("deep", agents, tasks, generated)
        assert tasks == after_first
        assert tasks["task_t1"]["description"].count("DEEP RESEARCH CONTRACT") == 1
