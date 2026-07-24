"""Tool-call approval gates — human sign-off before a designated tool runs.

A tool whose config carries ``requires_approval: true`` (or an ``approval``
dict) gets stamped with ``_approval_policy`` by the ToolFactory. The engine
pre-hook installed here (via ``kasal_engine.core.register_tool_hooks``)
intercepts the call in ``wrap_tool`` — the single choke point all three
execution paths share — and:

1. creates an ``HITLApproval`` row (``gate_config.kind == "tool_call"``,
   ``crew_sequence=0`` — the DB row is the source of truth),
2. notifies the UI: in-process → SSE ``hitl_request`` directly; in a
   subprocess → an ``hitl_request`` frame over the execution event pipe,
   which the parent relays to the same SSE channel,
3. blocks the (worker) thread polling the row until it is approved,
   rejected, or the gate deadline passes.

Denials raise ``ToolExecutionBlockedError``, which the LLM receives as the
tool result ("Tool call blocked: …") so the agent adapts instead of crashing.
The existing approve/reject endpoints drive the decision; flow-resume
machinery is skipped for tool gates (the blocked thread resumes itself).
"""

import logging
import os
import time
from typing import Any, Dict, Optional

from kasal_engine.core import ToolExecutionBlockedError, register_tool_hooks, unregister_tool_hooks

from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 1.0
_DEFAULT_TIMEOUT_SECONDS = 300  # tool gates want minutes, not the flow-gate hour
_DEFAULT_TIMEOUT_ACTION = "reject"  # or "approve" per-policy


def _safe_args(kwargs: Dict[str, Any], max_len: int = 500) -> Dict[str, str]:
    safe = {}
    for key, value in (kwargs or {}).items():
        try:
            text = str(value)
        except Exception:
            text = "<unprintable>"
        safe[str(key)] = text if len(text) <= max_len else text[:max_len] + "…"
    return safe


async def _create_approval(
    execution_id: str,
    group_id: str,
    gate_config: Dict[str, Any],
) -> Optional[str]:
    from src.db.session import request_scoped_session
    from src.services.hitl_service import HITLService

    async with request_scoped_session() as session:
        service = HITLService(session)
        approval = await service.create_approval_request(
            execution_id=execution_id,
            flow_id="",
            gate_node_id=f"tool:{gate_config.get('tool_name', 'unknown')}",
            crew_sequence=0,
            gate_config=gate_config,
            group_id=group_id,
        )
        await session.commit()
        return str(approval.id) if approval is not None else None


async def _approval_status(approval_id: str) -> Optional[str]:
    from src.db.session import request_scoped_session
    from src.repositories.hitl_repository import HITLApprovalRepository

    async with request_scoped_session() as session:
        approval = await HITLApprovalRepository(session).get_by_id(approval_id)
        if approval is None:
            return None
        status = approval.status
        return status.value if hasattr(status, "value") else str(status)


async def _notify_sse(execution_id: str, payload: Dict[str, Any]) -> None:
    from src.core.sse_manager import SSEEvent, sse_manager

    await sse_manager.broadcast_to_job(
        execution_id,
        SSEEvent(data=payload, event="hitl_request"),
    )


def _notify(execution_id: str, payload: Dict[str, Any]) -> None:
    """Notify the UI that input is needed — path-appropriate transport."""
    if os.environ.get("CREW_SUBPROCESS_MODE", "").lower() == "true":
        # Subprocess: the parent's relay turns this frame into the same SSE
        # event. The DB row stays the source of truth (pipe drops on full).
        try:
            from src.services import execution_event_pipe

            writer = execution_event_pipe._active_writer
            if writer is not None:
                writer._put({"kind": "hitl_request", **payload})
        except Exception as pipe_err:  # noqa: BLE001
            logger.debug(f"[tool_approval] pipe notify skipped: {pipe_err}")
        return
    try:
        from src.engines.kasal.tools.async_bridge import run_async_with_context

        run_async_with_context(_notify_sse(execution_id, payload), timeout=10)
    except Exception as sse_err:  # noqa: BLE001
        logger.debug(f"[tool_approval] SSE notify skipped: {sse_err}")


def make_tool_approval_hook(execution_id: str, group_context: Optional[GroupContext]):
    """Build the pre-execution hook for one execution's tool calls."""
    from src.engines.kasal.tools.async_bridge import run_async_with_context

    group_id = getattr(group_context, "primary_group_id", None) or "default"

    def approval_hook(tool: Any, kwargs: Dict[str, Any], agent: Any, task: Any) -> None:
        policy = getattr(tool, "_approval_policy", None)
        if policy is None:
            return

        tool_name = getattr(tool, "name", type(tool).__name__)
        timeout_seconds = int(policy.get("timeout_seconds", _DEFAULT_TIMEOUT_SECONDS))
        timeout_action = str(policy.get("timeout_action", _DEFAULT_TIMEOUT_ACTION)).lower()
        gate_config = {
            "kind": "tool_call",
            "tool_name": tool_name,
            "tool_args": _safe_args(kwargs),
            "agent_role": getattr(agent, "role", None),
            "task_name": getattr(task, "name", None),
            "timeout_seconds": timeout_seconds,
            "timeout_action": timeout_action,
            "message": f"Agent wants to run '{tool_name}' — approval required.",
        }

        try:
            approval_id = run_async_with_context(
                _create_approval(execution_id, group_id, gate_config), timeout=30
            )
        except Exception as create_err:  # noqa: BLE001
            # Fail CLOSED: an approval-required tool must not run when the
            # approval record can't be created.
            logger.error(f"[tool_approval] could not create approval: {create_err}")
            raise ToolExecutionBlockedError(
                f"'{tool_name}' requires approval but the approval request "
                f"could not be created ({create_err})."
            ) from create_err

        logger.info(
            f"[tool_approval] execution {execution_id}: '{tool_name}' waiting "
            f"for approval {approval_id} (timeout {timeout_seconds}s)"
        )
        _notify(execution_id, {
            "job_id": execution_id,
            "approval_id": approval_id,
            **gate_config,
        })

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                status = run_async_with_context(_approval_status(approval_id), timeout=15)
            except Exception as poll_err:  # noqa: BLE001
                logger.warning(f"[tool_approval] poll failed (retrying): {poll_err}")
                status = None
            if status == "approved":
                logger.info(f"[tool_approval] {approval_id} approved — running '{tool_name}'")
                return
            if status in ("rejected", "timeout"):
                raise ToolExecutionBlockedError(
                    f"'{tool_name}' was {status} by the human reviewer."
                )
            time.sleep(_POLL_INTERVAL_SECONDS)

        if timeout_action == "approve":
            logger.warning(
                f"[tool_approval] {approval_id} timed out — policy allows proceed"
            )
            return
        raise ToolExecutionBlockedError(
            f"'{tool_name}' approval timed out after {timeout_seconds}s — not executed."
        )

    return approval_hook


def install_tool_approval_hook(execution_id: str, group_context: Optional[GroupContext]):
    """Register the hook; returns a callable that unregisters it."""
    hook = make_tool_approval_hook(execution_id, group_context)
    register_tool_hooks(pre=hook)

    def uninstall() -> None:
        unregister_tool_hooks(pre=hook)

    return uninstall
