"""A flow task built from its crew's canvas node, when the task row is unavailable.

A flow names tasks by id. When the row is not found in the run's workspace
(deleted, or another workspace's id still on an imported crew), the flow
processors build the task from the node on the crew's own canvas instead. That
node carries the task's POLICY as well as its prompt, mostly under
``data.config``: the code guardrail, the LLM guardrail, human input and retries.
The shared spec builder (``TaskConfig._task_data_to_spec``) reads those as
attributes, so a fallback that leaves one out runs the task with that check
silently off. This is the one place that maps them, for starting points,
listeners and routers alike.
"""

from types import SimpleNamespace
from typing import Dict, Optional

#: The node's ``data`` / ``data.config`` dicts: JSON-shaped, keyed by field.
_NodeData = Dict[str, object]


def _setting(data: _NodeData, config: _NodeData, key: str) -> Optional[object]:
    """``data.config[key]``, else the node's top-level ``key``; None when unset."""
    value = config.get(key)
    return data.get(key) if value is None else value


def canvas_task_data(
    task_id: str, data: _NodeData, agent_id: Optional[str]
) -> SimpleNamespace:
    """The task-row stand-in for ``task_id`` from its canvas node ``data``."""
    raw_config = data.get("config")
    config: _NodeData = dict(raw_config) if isinstance(raw_config, dict) else {}
    # The LLM guardrail is read from config only; the node may keep it top-level.
    llm_guardrail = _setting(data, config, "llm_guardrail")
    if llm_guardrail is not None:
        config["llm_guardrail"] = llm_guardrail
    return SimpleNamespace(
        id=task_id,
        name=data.get("label", f"Task {task_id}"),
        description=data.get("description", ""),
        expected_output=data.get("expected_output", ""),
        agent_id=agent_id,
        tools=data.get("tools", []),
        tool_configs=data.get("tool_configs", {}),
        async_execution=data.get("async_execution", False),
        context=data.get("context", []),
        config=config,
        memory=data.get("memory", False),
        markdown=data.get("markdown", False),
        output_file=config.get("output_file"),
        output_json=config.get("output_json"),
        output_pydantic=config.get("output_pydantic"),
        human_input=_setting(data, config, "human_input") or False,
        guardrail=_setting(data, config, "guardrail"),
        retry_on_fail=_setting(data, config, "retry_on_fail"),
        max_retries=_setting(data, config, "max_retries"),
        converter_cls=None,
    )
