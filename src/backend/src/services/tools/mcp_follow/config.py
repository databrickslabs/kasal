"""Config-driven follow specs — the MCP layer stays fully server-agnostic.

MCP has no standard way for a server to say "this tool starts a job and that
tool polls it"; each server invents its own convention. Kasal keeps that
knowledge OUT of code: an MCP server's ``additional_config`` may declare

    "follow": [
        {
            "start_tool": "genie_ask",            # substring of the tool name
            "poll_tool": "genie_poll_response",   # replaces it to name the poll tool
            "id_params": ["conversation_id", "response_id"],
            "cancel_tool": "genie_cancel_response",    # optional server-side cancel
            "terminal_statuses": ["completed", ...],   # optional, has defaults
            "done_fields": ["final_answer", "error"],  # optional, has defaults
        }
    ]

and :func:`follow_spec_from_config` turns a matching declaration into the
:class:`~.runner.FollowSpec` the vendor-neutral loop consumes. The managed
Databricks Genie entries in the Connect-a-tool catalog ship exactly this as
preset data — the engine itself knows no server by name.
"""

import json
from typing import Any, Dict, List, Optional

from src.services.tools.mcp_follow.runner import FollowSpec

#: Statuses that end a follow loop when a pair declares none. Broad on purpose:
#: a failed job must not poll to the timeout and be reported as "did not
#: finish". Compared upper-cased.
DEFAULT_TERMINAL_STATUSES = frozenset(
    {
        "COMPLETED",
        "SUCCEEDED",
        "DONE",
        "FAILED",
        "ERROR",
        "CANCELLED",
        "CANCELED",
        "INCOMPLETE",
        "QUERY_RESULT_EXPIRED",
    }
)
#: Envelope fields that mean "finished" when populated, whatever the status
#: string says, when a pair declares none.
DEFAULT_DONE_FIELDS = ("final_answer", "error")


def status_envelope(result: Any) -> Optional[dict]:
    """A status envelope ({"status": ..., ids...}) from an MCP result, or None
    when the result is not one (an already-final answer, or an unknown shape).
    Servers return it as structuredContent; a JSON text block is accepted in
    case a transport delivers it that way."""
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict) and "status" in structured:
        return structured
    for block in getattr(result, "content", None) or []:
        text = getattr(block, "text", None)
        if isinstance(text, str):
            try:
                data = json.loads(text)
            except Exception:
                continue
            if isinstance(data, dict) and "status" in data:
                return data
    return None


def result_has_content(result: Any) -> bool:
    """True when an MCP result carries a SUBSTANTIVE payload (a finished
    answer) versus an empty / not-ready acknowledgement (servers answer an
    in-flight poll with an empty body / HTTP 202). Conservative on purpose:
    treating empty as "not ready" is what stops the loop from handing the
    agent an unfinished result."""
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict) and structured:
        return True
    for block in getattr(result, "content", None) or []:
        text = getattr(block, "text", None)
        if isinstance(text, str) and text.strip():
            return True
        # A non-text block (embedded resource, image, …) is also real content.
        if text is None and getattr(block, "type", None) not in (None, "text"):
            return True
    return False


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.capitalize() for part in rest)


def _poll_params(envelope: dict, id_params: List[str]) -> Dict[str, Any]:
    """The declared poll ids present in an envelope, looked up under both the
    snake_case name and its camelCase twin (servers are not consistent). ONLY
    declared ids are sent — the adapter keeps unknown keys as-is (it does NOT
    trim to the tool's schema), and an undeclared extra parameter risks a
    validation rejection on strict servers."""
    params: Dict[str, Any] = {}
    for name in id_params:
        value = envelope.get(name)
        if value in (None, ""):
            value = envelope.get(_camel(name))
        if value not in (None, ""):
            params[name] = value
    return params


def _advertises(adapter: Any, tool_name: str) -> bool:
    return any(
        (t.get("name") if isinstance(t, dict) else getattr(t, "name", None))
        == tool_name
        for t in (getattr(adapter, "tools", None) or [])
    )


def follow_spec_from_config(wrapper: Any, result: Any) -> Optional[FollowSpec]:
    """The :class:`FollowSpec` for this tool call, from the server's own
    ``follow`` configuration — or None, in which case the call is any other
    MCP tool and passes through untouched."""
    adapter = getattr(wrapper, "adapter", None)
    declarations = (getattr(adapter, "server_params", None) or {}).get("follow")
    if not declarations:
        return None
    if isinstance(declarations, dict):
        declarations = [declarations]
    tool_name = getattr(wrapper, "name", "") or ""
    for pair in declarations:
        if not isinstance(pair, dict):
            continue
        start = str(pair.get("start_tool") or "")
        poll = str(pair.get("poll_tool") or "")
        id_params = [str(p) for p in (pair.get("id_params") or []) if p]
        # Kasal prefixes tool names with the server title, so the start tool is
        # matched as a substring and replaced to derive the poll tool's name.
        if not start or not poll or len(id_params) < 2 or start not in tool_name:
            continue
        poll_tool = tool_name.replace(start, poll, 1)
        if not _advertises(adapter, poll_tool):
            continue
        # Optional server-side cancel, derived and verified the same way as
        # the poll tool; silently absent when the server does not offer one.
        cancel = str(pair.get("cancel_tool") or "")
        cancel_tool = tool_name.replace(start, cancel, 1) if cancel else None
        if cancel_tool and not _advertises(adapter, cancel_tool):
            cancel_tool = None
        terminal = {
            str(s).upper() for s in (pair.get("terminal_statuses") or [])
        } or DEFAULT_TERMINAL_STATUSES
        done_fields = [str(f) for f in (pair.get("done_fields") or DEFAULT_DONE_FIELDS)]

        def is_final(envelope: dict, _terminal=terminal, _done=done_fields) -> bool:
            if str(envelope.get("status") or "").upper() in _terminal:
                return True
            return any(envelope.get(field) not in (None, "") for field in _done)

        return FollowSpec(
            name=str(pair.get("name") or start),
            poll_tool=poll_tool,
            envelope_of=status_envelope,
            is_final=is_final,
            poll_params_of=lambda envelope, _ids=tuple(id_params): _poll_params(
                envelope, list(_ids)
            ),
            has_content=result_has_content,
            cancel_tool=cancel_tool,
        )
    return None
