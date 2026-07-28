"""Telling the chat agent that this conversation has files attached.

Attaching a file in ChatMode already binds ``DatabricksKnowledgeSearchTool`` to
the agent and scopes it to that file (``tool_configs.file_paths``). What it did
NOT do was mention the file anywhere in the prompt — so the model saw a generic
"search uploaded documents" tool and no reason to believe this conversation had
any. Asked "what is existing in this report", it answered:

    "The report content isn't provided here, so I can't determine what it
     contains. Please share or upload the report."

— having just been handed the tool that would have read it. The tool's own
description cannot fix that: whether documents exist is a fact about the
SESSION, not about the tool.

Deliberately a hint, not an instruction. It states what is available and how to
reach it, and stops there: no file contents, no "you must search first", no
retrieval recipe. The model decides whether the request concerns the files.
"""

import os
from typing import Any, Dict, Iterable, List

#: Names listed before the hint switches to a count. Long enough for a normal
#: attachment set, short enough that a bulk upload cannot crowd out the prompt.
MAX_LISTED = 10

#: The tool that reads them. Named so the model knows the route, since the hint
#: is the only place the two facts (files exist / this tool reads them) meet.
KNOWLEDGE_TOOL_NAME = "DatabricksKnowledgeSearchTool"


def _file_paths_from_tool_configs(tool_configs: Dict[str, Any]) -> Iterable[str]:
    """Every configured file path, across whichever tools carry them."""
    if not isinstance(tool_configs, dict):
        return []
    paths: List[str] = []
    for config in tool_configs.values():
        if not isinstance(config, dict):
            continue
        configured = config.get("file_paths")
        if isinstance(configured, str):
            configured = [configured]
        if not isinstance(configured, (list, tuple)):
            continue
        paths.extend(str(p) for p in configured if p)
    return paths


def attached_file_names(agent_spec: Dict[str, Any]) -> List[str]:
    """Base names of the files attached to this run, in order, deduplicated.

    Base names because the stored path is an internal location
    (``uploads/<group>/<session>/report.md``) — the workspace layout is not
    something to hand a model, and the name is what the user typed about.
    """
    seen = set()
    names: List[str] = []
    for path in _file_paths_from_tool_configs(agent_spec.get("tool_configs") or {}):
        name = os.path.basename(path.rstrip("/")) or path
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


def build_attachment_hint(agent_spec: Dict[str, Any]) -> str:
    """One line naming what is attached, or "" when nothing is.

    Empty is the common case (most chat turns have no attachments) and must cost
    the prompt nothing.
    """
    names = attached_file_names(agent_spec or {})
    if not names:
        return ""

    listed = names[:MAX_LISTED]
    suffix = "" if len(names) <= MAX_LISTED else f" (and {len(names) - MAX_LISTED} more)"
    return (
        f"Files attached to this conversation: {', '.join(listed)}{suffix}. "
        f"Their contents are not included here — {KNOWLEDGE_TOOL_NAME} reads them."
    )
