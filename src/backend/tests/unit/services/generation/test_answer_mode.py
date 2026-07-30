"""What an answer mode actually changes about a generated crew.

Before this pass, ``research`` and ``deep`` differed by one string
(``reasoning_effort``) and were byte-identical on a model without a native
reasoning budget. These tests pin the differences that now exist.
"""

import copy

import pytest

from src.services.generation.crew.answer_mode import apply_answer_mode

#: A search tool's catalog id. Just a fixture value now — the module under test
#: no longer knows any tool's id, which is the property TestToolPolicy asserts.
_SEARCH_TOOL_ID = "31"


def _fixture():
    agents = {
        "agent_a1": {
            "role": "Researcher",
            "goal": "g",
            "backstory": "b",
            "tools": ["26", _SEARCH_TOOL_ID],
        }
    }
    tasks = {
        "task_t1": {
            "id": "t1",
            "description": "Find facts",
            "expected_output": "A report",
            "tools": [_SEARCH_TOOL_ID],
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
        assert agents["agent_a1"]["max_execution_time"] == 1200

    def test_a_plan_asking_for_more_wins(self):
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["max_iter"] = 77
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["max_iter"] == 77

    def test_a_plan_asking_for_less_cannot_lower_the_mode_floor(self):
        """The bug this replaced ``setdefault`` for: a crew re-run from the UI
        carries its saved agents' caps (the form ships 300), which silently
        undercut deep mode while deep was ALSO using the slow Perplexity model.
        The agent was configured before anyone picked an answer mode; picking
        deep is the later and more specific instruction."""
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["max_iter"] = 7
        agents["agent_a1"]["max_execution_time"] = 300
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["max_iter"] == 30
        assert agents["agent_a1"]["max_execution_time"] == 1200

    @pytest.mark.parametrize("junk", [None, "", "soon", 0, -5, [], {}])
    def test_an_unusable_plan_value_falls_through_to_the_profile(self, junk):
        """Generation must not die over a field the user cannot see."""
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["max_execution_time"] = junk
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["max_execution_time"] == 1200

    def test_a_numeric_string_is_still_a_number(self):
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["max_iter"] = "99"
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["max_iter"] == 99


class TestToolPolicy:
    """Deep treats tools exactly as research does: it does not touch them.

    It briefly did — the module carried a tool's catalog id and repointed that
    tool at a slow deep-research model. That put vendor knowledge in a mode
    (so exactly one tool benefited and a catalog renumber would have silently
    disabled it) and, because the substituted model answers in minutes rather
    than seconds, it was the direct cause of deep runs blowing their execution
    budget while research runs never did.
    """

    def test_the_tool_list_is_untouched(self):
        agents, tasks, generated = _fixture()
        before = list(agents["agent_a1"]["tools"])
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["tools"] == before

    def test_no_tool_config_is_written(self):
        agents, tasks, generated = _fixture()
        apply_answer_mode("deep", agents, tasks, generated)
        assert "tool_configs" not in agents["agent_a1"]
        assert "tool_configs" not in tasks["task_t1"]

    def test_an_existing_tool_config_is_left_alone(self):
        """A model the user pinned is a decision this mode has no opinion on."""
        agents, tasks, generated = _fixture()
        agents["agent_a1"]["tool_configs"] = {"PerplexityTool": {"model": "sonar-pro"}}
        apply_answer_mode("deep", agents, tasks, generated)
        assert agents["agent_a1"]["tool_configs"] == {
            "PerplexityTool": {"model": "sonar-pro"}
        }

    def test_the_module_names_no_tool(self):
        """The property that makes this untangled: a new research tool needs no
        edit here, and no catalog id can drift out from under it."""
        import inspect

        from src.services.generation.crew import answer_mode

        source = inspect.getsource(answer_mode)
        for vendor in ("Perplexity", "sonar", "PERPLEXITY"):
            assert vendor not in source.replace(
                # The docstring explains the removal; that mention is the point.
                answer_mode.__doc__ or "",
                "",
            )


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
