"""Pure helpers for cloning a crew into a NEW, independent crew.

"Save as new crew": rather than overwriting the crew in place (which mints new
task rows and leaves flows pointing at stale ones — see
``flow_builder.modules.stale_task_config``), the user clones it under a new name,
edits the copy, and leaves the original untouched. A clone is only truly
independent if its agents and tasks are duplicated too; otherwise editing the
copy's tasks would mutate the original's rows.

The fiddly part is rebuilding the canvas graph with the clone's new IDs. That is
kept here as a pure function so it is unit-testable without a session;
``CrewService.clone_with_group`` owns the DB orchestration.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Tuple

from src.schemas.agent import AgentCreate
from src.schemas.task import TaskCreate


def _non_null_fields(obj, field_names) -> Dict[str, Any]:
    """Stored values for ``field_names`` present on ``obj``, dropping ``None``.

    Dropping None lets the Create schema's own defaults apply, which matters for
    non-optional-with-default fields (e.g. a task's ``config``): a stored NULL
    passed explicitly would fail validation, whereas omitting it uses the default.
    """
    out = {}
    for f in field_names:
        if hasattr(obj, f):
            val = getattr(obj, f)
            if val is not None:
                out[f] = val
    return out


def agent_create_from_model(agent) -> AgentCreate:
    """An ``AgentCreate`` carrying the stored agent's fields.

    ``tool_configs`` is expected already decrypted (``AgentService.get``);
    ``create_with_group`` re-encrypts on write. Columns absent from the schema
    are ignored.
    """
    return AgentCreate(**_non_null_fields(agent, AgentCreate.model_fields.keys()))


def task_create_from_model(task, agent_id_map: Dict[str, str]) -> TaskCreate:
    """A ``TaskCreate`` for the clone.

    ``agent_id`` is repointed at the CLONED agent; ``context`` is cleared here
    (the clone's task IDs do not exist yet) and remapped by the caller in a second
    pass once every clone task has an ID.
    """
    data = _non_null_fields(task, TaskCreate.model_fields.keys())
    old_agent = data.get("agent_id")
    if old_agent is not None and str(old_agent) in agent_id_map:
        data["agent_id"] = agent_id_map[str(old_agent)]
    data["context"] = []  # remapped after all clone tasks exist
    return TaskCreate(**data)


def remap_context(context, task_id_map: Dict[str, str]) -> List[str]:
    """Remap a task-context list of OLD task IDs to the clone's new IDs."""
    return [task_id_map.get(str(c), str(c)) for c in (context or [])]


def remap_crew_graph(
    nodes, edges, agent_id_map: Dict[str, str], task_id_map: Dict[str, str]
) -> Tuple[List[dict], List[dict]]:
    """Rebuild ``(nodes, edges)`` with every old agent/task ID replaced by its clone's.

    Node and edge IDs embed the entity UUID as a substring (e.g. ``"task-<uuid>"``,
    ``"reactflow__edge-agent-<uuid>…-task-<uuid>…"``), so those are remapped by
    substring replacement; a node's ``data.agentId`` / ``data.taskId`` /
    ``data.context`` are remapped by exact key. IDs with no mapping are left
    untouched. Operates on deep copies — the source crew is never mutated.
    """
    id_map = {str(k): str(v) for k, v in {**agent_id_map, **task_id_map}.items()}

    def _sub(s: str) -> str:
        for old, new in id_map.items():
            if old and old in s:
                s = s.replace(old, new)
        return s

    new_nodes: List[dict] = []
    for node in nodes or []:
        node = copy.deepcopy(node)
        if isinstance(node.get("id"), str):
            node["id"] = _sub(node["id"])
        data = node.get("data")
        if isinstance(data, dict):
            if str(data.get("agentId")) in agent_id_map:
                data["agentId"] = agent_id_map[str(data["agentId"])]
            if str(data.get("taskId")) in task_id_map:
                data["taskId"] = task_id_map[str(data["taskId"])]
            if isinstance(data.get("context"), list):
                data["context"] = remap_context(data["context"], task_id_map)
        new_nodes.append(node)

    new_edges: List[dict] = []
    for edge in edges or []:
        edge = copy.deepcopy(edge)
        for key in ("id", "source", "target"):
            if isinstance(edge.get(key), str):
                edge[key] = _sub(edge[key])
        new_edges.append(edge)

    return new_nodes, new_edges
