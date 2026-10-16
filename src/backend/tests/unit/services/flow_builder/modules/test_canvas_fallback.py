"""A flow task built from its canvas node keeps the task's policy.

When the task row is unavailable (deleted, or another workspace's id on an
imported crew) the flow processors build the task from the crew's own canvas
node. The node keeps the code guardrail, LLM guardrail, human input and
retries under ``data.config``; the shared spec builder reads them as
attributes, so each must be mapped or the task runs with that check off.
"""

from src.services.flow_builder.modules.canvas_fallback import canvas_task_data
from src.services.flow_builder.modules.task_adapter import TaskConfig

LLM_GUARDRAIL = {"description": "cites a source", "llm_model": "m"}


def _node(**config):
    return {
        "label": "Research",
        "description": "Find sources",
        "expected_output": "A list",
        "tools": ["SerperDevTool"],
        "tool_configs": {"SerperDevTool": {"requires_approval": True}},
        "config": config,
    }


def test_the_policy_under_config_reaches_the_task_spec():
    task = canvas_task_data(
        "t1",
        _node(
            guardrail='{"type": "minimum_number"}',
            llm_guardrail=LLM_GUARDRAIL,
            human_input=True,
            retry_on_fail=False,
            max_retries=2,
        ),
        "a1",
    )
    spec = TaskConfig._task_data_to_spec(task)

    assert spec["guardrail"] == '{"type": "minimum_number"}'
    assert spec["llm_guardrail"] == LLM_GUARDRAIL
    assert spec["human_input"] is True
    assert spec["retry_on_fail"] is False
    assert spec["max_retries"] == 2
    assert spec["tool_configs"] == {"SerperDevTool": {"requires_approval": True}}


def test_policy_kept_at_the_top_of_the_node_is_read_too():
    data = {**_node(), "guardrail": "g", "llm_guardrail": LLM_GUARDRAIL}
    spec = TaskConfig._task_data_to_spec(canvas_task_data("t1", data, "a1"))
    assert spec["guardrail"] == "g"
    assert spec["llm_guardrail"] == LLM_GUARDRAIL


def test_a_node_with_no_policy_adds_none():
    spec = TaskConfig._task_data_to_spec(canvas_task_data("t1", _node(), None))
    assert "guardrail" not in spec and "llm_guardrail" not in spec
    assert spec["human_input"] is False


def test_the_fields_the_fallback_always_carried_are_unchanged():
    task = canvas_task_data("t1", _node(output_file="out.md"), "a1")
    assert (task.id, task.name, task.agent_id) == ("t1", "Research", "a1")
    assert task.description == "Find sources"
    assert task.expected_output == "A list"
    assert task.tools == ["SerperDevTool"]
    assert task.output_file == "out.md"
    assert task.converter_cls is None


def test_a_node_with_no_config_dict_is_tolerated():
    task = canvas_task_data("t1", {"label": "x", "config": None}, None)
    assert task.config == {} and task.guardrail is None
