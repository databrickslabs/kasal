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
The one thing read from the canvas is AGENT ASSIGNMENT: a task runs on the
agent its agent->task edge comes from, as it does in a UI run, and the row's
``agent_id`` is used only for a task with no edge.

**Fidelity matters more here than it does for a fresh run.** A resume that
rebuilds a thinner crew than the one that ran would quietly drop guardrails,
structured output or tool configuration — the run would still succeed, and
produce something different. So every field the row carries is projected, and
the set is asserted against the model in
``tests/unit/services/catalog/test_crew_config.py`` so a column added later
cannot be silently left behind.

**Only the caller's workspace is read.** The crew id, and the ids it lists, can
come from the caller (a trigger target names any id), so every read goes through
``run_lookup`` with the caller's ``GroupContext``. With no group ids nothing is
read; a crew of another workspace is "not found", the same as a deleted one.
"""

import logging
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

# Agent columns that are pure pass-through: present in the config under the
# same name whenever the row has a value. Excluded deliberately: group_id,
# created_by_email, created_at, updated_at (audit, not behaviour). id and name
# are emitted explicitly. temperature stays the row's 0-100 value: the kernel
# reads spec["temperature"] and scales it (kernel/agent_builder).
_AGENT_FIELDS = (
    "temperature",
    "execution_effort",
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
    # Per-agent max output tokens override (Agent form); the kernel reads it
    # from the agent entry as spec["max_tokens"] (kernel/agent_builder).
    "max_tokens",
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


def _node_row_id(node: Mapping[str, object]) -> Tuple[str, str]:
    """``(kind, row id)`` for a canvas node: kind is "agent", "task" or "".

    ``data.agentId``/``data.taskId`` name the row; the node id is a canvas
    handle that only falls back to ``agent-<id>``/``task-<id>`` — the same
    order CrewService uses when it builds ``agent_ids``/``task_ids``.
    """
    kind = {"agentNode": "agent", "taskNode": "task"}.get(str(node.get("type")), "")
    data = node.get("data")
    row_id = data.get(f"{kind}Id") if kind and isinstance(data, Mapping) else None
    if not row_id:
        node_id = str(node.get("id") or "")
        prefix = f"{kind}-"
        row_id = node_id[len(prefix) :] if node_id.startswith(prefix) else node_id
    return kind, str(row_id)


def _agent_ids_from_edges(crew: object) -> Dict[str, str]:
    """Task row id -> agent row id, read from the crew's agent->task edges.

    The edges are what the canvas shows and what a UI run uses. ``task.agent_id``
    is a second copy that partial writes used to blank, and a blank one puts the
    task on the crew's FIRST agent without an error — so edges win.
    """
    nodes = {
        str(node.get("id")): _node_row_id(node)
        for node in getattr(crew, "nodes", None) or []
        if isinstance(node, Mapping)
    }
    by_task: Dict[str, str] = {}
    for edge in getattr(crew, "edges", None) or []:
        if not isinstance(edge, Mapping):
            continue
        source = nodes.get(str(edge.get("source")))
        target = nodes.get(str(edge.get("target")))
        if source and target and source[0] == "agent" and target[0] == "task":
            # The LAST agent edge wins, as in a canvas run (JobExecutionService).
            by_task[target[1]] = source[1]
    return by_task


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
        group_context: The caller's workspace. Agents and tasks are read only
            from its groups, so a crew cannot pull in another tenant's rows
            even if its id lists name them. With no group ids, nothing is read.

    Returns:
        ``(agents_yaml, tasks_yaml)``. Either may be empty when the crew's id
        lists reference rows that no longer exist or are not the caller's —
        callers must treat an empty pair as "cannot rebuild" rather than as a
        valid empty crew.
    """
    from src.services.catalog.agents import AgentService
    from src.services.catalog.run_lookup import (
        agent_for_run,
        run_group_ids,
        task_for_run,
    )
    from src.services.catalog.tasks import TaskService

    if not run_group_ids(group_context):
        logger.warning(
            "Crew %s not rebuilt: the caller has no workspace",
            getattr(crew, "id", None),
        )
        return {}, {}

    agent_service = AgentService(session)
    task_service = TaskService(session)

    agents_yaml: Dict[str, Any] = {}
    agent_key_by_id: Dict[str, str] = {}
    for agent_id in getattr(crew, "agent_ids", None) or []:
        agent = await agent_for_run(agent_service, str(agent_id), group_context, logger)
        if agent is None:
            continue
        key = agent.name or agent.role or str(agent_id)
        agent_key_by_id[str(agent_id)] = key
        entry: Dict[str, Any] = {
            # The row id lets the run's group-checked fill-in (kasal_service)
            # and settings snapshot find this agent. ``name`` keeps the engine
            # keying it by this entry's key, which is what tasks reference:
            # CrewPreparation names an agent by name, else by id.
            "id": str(agent.id),
            "name": key,
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
        task = await task_for_run(task_service, str(task_id), group_context, logger)
        if task is None:
            continue
        key = task.name or str(task_id)
        task_key_by_id[str(task_id)] = key
        resolved.append((key, task))

    edge_agents = _agent_ids_from_edges(crew)
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

        # The edge the canvas draws wins; the row's agent_id covers a task
        # with no edge (or an edge to an agent no longer in the crew).
        edge_agent = agent_key_by_id.get(edge_agents.get(str(task.id), ""))
        row_agent = agent_key_by_id.get(str(getattr(task, "agent_id", "") or ""))
        assigned = edge_agent or row_agent
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
    """As above, from a crew id. None means "not found" in the caller's workspace.

    That covers a deleted crew, another workspace's crew, an id that is not a
    UUID, and a caller with no workspace. A resume falls back to its stored
    snapshot on None; a caller STARTING a run (the trigger queue) must fail.
    """
    from src.services.catalog.crews import CrewService
    from src.services.catalog.run_lookup import crew_for_run

    try:
        # crew_for_run coerces a string id to the UUID the column binds.
        crew = await crew_for_run(CrewService(session), crew_id, group_context, logger)
    except Exception as exc:  # noqa: BLE001 — a fallback beats a failed resume
        logger.warning("Could not load crew %s: %s", crew_id, exc)
        return None

    if crew is None:
        return None
    return await build_crew_execution_config(session, crew, group_context)
