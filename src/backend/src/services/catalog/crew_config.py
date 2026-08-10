"""A saved crew -> the ``agents_yaml``/``tasks_yaml`` the engine takes.

The browser builds this same projection client-side before calling
``/executions`` (``JobExecutionService.executeJob``). Having it here as well is
not duplication for its own sake: two things need a crew's CURRENT definition
without a browser in the loop.

- **External invocation** (MCP / A2A) starts a published crew with no canvas.
- **Resume** rebuilds a crashed or finished run from the crew as it is NOW,
  rather than from the ``inputs`` snapshot frozen when the original run
  started. That snapshot is why editing a task after a run was invisible to a
  resume: the definition it replayed was the one that produced the checkpoint,
  so nothing could ever look changed.

Resolved from ``agent_ids``/``task_ids`` rather than from the canvas ``nodes``:
nodes are presentation data (positions, edges, UI state) and a crew can be
perfectly valid with none, while the id lists are what the crew actually IS.
Task ORDER is the order of ``task_ids``, which is also what the canvas writes.

**Fidelity matters more here than it does for a fresh run.** A resume that
rebuilds a thinner crew than the one that ran would quietly drop guardrails,
structured output or tool configuration — the run would still succeed, and
produce something different. So every field the row carries is projected, and
the set is asserted against the model in
``tests/unit/services/catalog/test_crew_config.py`` so a column added later
cannot be silently left behind.
"""

import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Agent columns that are pure pass-through: present in the config under the
# same name whenever the row has a value. Excluded deliberately: id, name,
# group_id, created_by_email, created_at, updated_at (identity/audit, not
# behaviour), and temperature (carried inside the model config, not the agent
# entry the engine reads).
_AGENT_FIELDS = (
    "llm",
    "function_calling_llm",
    "max_iter",
    "max_rpm",
    "max_execution_time",
    "verbose",
    "allow_delegation",
    "cache",
    "memory",
    "embedder_config",
    "system_template",
    "prompt_template",
    "response_template",
    "allow_code_execution",
    "code_execution_mode",
    "max_retry_limit",
    "use_system_prompt",
    "respect_context_window",
    "knowledge_sources",
    "inject_date",
    "date_format",
    "thinking_budget_tokens",
    "reasoning_effort",
    "skills",
)

# Task columns that are pure pass-through. ``context`` is excluded because it
# holds task IDs and has to be translated into the config's task KEYS; agent
# assignment likewise.
_TASK_FIELDS = (
    "async_execution",
    "markdown",
    "output_json",
    "output_pydantic",
    "output_file",
    "callback",
    "human_input",
    "converter_cls",
    "guardrail",
    "llm_guardrail",
    "config",
)


def _copy_fields(row: Any, fields: Tuple[str, ...], into: Dict[str, Any]) -> None:
    """Copy the fields the row actually has a value for.

    ``None`` is skipped rather than written: the engine's builders treat an
    absent key as "use the default", and an explicit null is not always the
    same thing.
    """
    for field in fields:
        value = getattr(row, field, None)
        if value is not None:
            into[field] = value


async def build_crew_execution_config(
    session: Any,
    crew: Any,
    group_context: Any = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Project a saved crew into ``(agents_yaml, tasks_yaml)``.

    Args:
        session: The session to resolve the crew's agents and tasks on.
        crew: The saved ``Crew`` row.
        group_context: When given, agents and tasks are read through the
            group-checked accessors so a crew cannot pull in another tenant's
            rows even if its id lists name them.

    Returns:
        ``(agents_yaml, tasks_yaml)``. Either may be empty when the crew's id
        lists reference rows that no longer exist — callers must treat an empty
        pair as "cannot rebuild" rather than as a valid empty crew.
    """
    from src.services.catalog.agents import AgentService
    from src.services.catalog.tasks import TaskService

    agent_service = AgentService(session)
    task_service = TaskService(session)

    async def get_agent(agent_id: str) -> Any:
        if group_context is not None:
            return await agent_service.get_with_group_check(agent_id, group_context)
        return await agent_service.get(agent_id)

    async def get_task(task_id: str) -> Any:
        if group_context is not None:
            return await task_service.get_with_group_check(task_id, group_context)
        return await task_service.get(task_id)

    agents_yaml: Dict[str, Any] = {}
    agent_key_by_id: Dict[str, str] = {}
    for agent_id in getattr(crew, "agent_ids", None) or []:
        agent = await get_agent(str(agent_id))
        if agent is None:
            logger.warning(
                "Crew %s references agent %s, which no longer exists — it will "
                "not be part of the rebuilt config",
                getattr(crew, "id", None),
                agent_id,
            )
            continue
        key = agent.name or agent.role or str(agent_id)
        agent_key_by_id[str(agent_id)] = key
        entry: Dict[str, Any] = {
            "role": agent.role,
            "goal": agent.goal,
            "backstory": agent.backstory,
            "tools": getattr(agent, "tools", None) or [],
        }
        _copy_fields(agent, _AGENT_FIELDS, entry)
        tool_configs = getattr(agent, "tool_configs", None)
        if tool_configs:
            entry["tool_configs"] = tool_configs
        agents_yaml[key] = entry

    # Two passes over tasks: the first resolves rows and assigns keys, the
    # second translates ``context`` from task IDs to those keys — a task may
    # depend on one declared after it, so the mapping has to be complete first.
    resolved = []
    task_key_by_id: Dict[str, str] = {}
    for task_id in getattr(crew, "task_ids", None) or []:
        task = await get_task(str(task_id))
        if task is None:
            logger.warning(
                "Crew %s references task %s, which no longer exists — it will "
                "not be part of the rebuilt config",
                getattr(crew, "id", None),
                task_id,
            )
            continue
        key = task.name or str(task_id)
        task_key_by_id[str(task_id)] = key
        resolved.append((key, task))

    tasks_yaml: Dict[str, Any] = {}
    for key, task in resolved:
        entry = {
            "id": str(task.id),
            "description": task.description,
            "expected_output": task.expected_output,
            "tools": getattr(task, "tools", None) or [],
        }
        _copy_fields(task, _TASK_FIELDS, entry)

        tool_configs = getattr(task, "tool_configs", None)
        if tool_configs:
            entry["tool_configs"] = tool_configs

        assigned = agent_key_by_id.get(str(getattr(task, "agent_id", "") or ""))
        if assigned:
            entry["agent"] = assigned

        # Context is stored as task IDs; the engine resolves it by task KEY.
        # An id naming a task outside this crew is dropped rather than passed
        # through — CrewPreparation falls back to the FIRST task on an
        # unresolvable reference, which would wire the crew wrongly and
        # silently.
        context_ids = getattr(task, "context", None) or []
        context_keys = [
            task_key_by_id[str(cid)]
            for cid in context_ids
            if str(cid) in task_key_by_id
        ]
        if context_keys:
            entry["context"] = context_keys

        tasks_yaml[key] = entry

    return agents_yaml, tasks_yaml


async def build_crew_execution_config_by_id(
    session: Any,
    crew_id: Any,
    group_context: Any = None,
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """As above, from a crew id. None when the crew is gone or unreadable.

    None is a normal answer, not an error: a run whose crew was deleted since
    is exactly the case that has to fall back to its stored snapshot.
    """
    from src.services.catalog.crews import CrewService

    try:
        crew = await CrewService(session).get(crew_id)
    except Exception as exc:  # noqa: BLE001 — a fallback beats a failed resume
        logger.warning("Could not load crew %s: %s", crew_id, exc)
        return None

    if crew is None:
        return None
    return await build_crew_execution_config(session, crew, group_context)
