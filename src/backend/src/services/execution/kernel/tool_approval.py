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
_DEFAULT_SCOPE = "run"  # a decision sticks for the rest of the run; "call" re-asks

# (execution_id, tool_name) -> "approved" | "rejected". One human decision per
# tool per run: an agent looping on the same tool must not re-prompt the user
# every call. In-memory is enough within a process (one execution per
# subprocess; light path keys by execution id); the DB lookup below covers
# resumed runs in a fresh process.
_run_decisions: Dict[tuple, str] = {}


async def _prior_decision(execution_id: str, tool_name: str, group_id: str) -> Optional[str]:
    """Latest approved/rejected tool_call decision for this tool in this run."""
    from src.db.session import request_scoped_session
    from src.repositories.hitl_repository import HITLApprovalRepository

    async with request_scoped_session() as session:
        approvals = await HITLApprovalRepository(session).get_all_for_execution(
            execution_id, group_id
        )
    decision = None
    for approval in approvals or []:
        config = approval.gate_config or {}
        if config.get("kind") != "tool_call" or config.get("tool_name") != tool_name:
            continue
        status = approval.status
        status = status.value if hasattr(status, "value") else str(status)
        if status in ("approved", "rejected"):
            decision = status  # list is time-ordered; keep the latest
    return decision


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
    from src.services.hitl.service import HITLService

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


async def _restore_running(execution_id: str) -> None:
    """Flip WAITING_FOR_APPROVAL back to RUNNING once a gate is decided.

    Safe unconditionally: the run cannot reach a terminal state while its
    worker thread is blocked on the gate.

    A FAILED restore is not cosmetic and must not be swallowed: the run keeps
    executing while its status stays WAITING_FOR_APPROVAL, so the UI shows the
    gate badge forever and re-opening it reports "nothing is waiting for
    approval" — the decision was made, but nothing reflects it. The usual cause
    is a missing parent row, so say which it is.
    """
    from src.models.execution_status import ExecutionStatus
    from src.services.execution.status import ExecutionStatusService

    updated = await ExecutionStatusService.update_status(
        job_id=execution_id,
        status=ExecutionStatus.RUNNING.value,
        message="Approval decided — run continuing",
    )
    if not updated:
        exists = await ExecutionStatusService.get_status(execution_id) is not None
        logger.error(
            f"[tool_approval] could not restore RUNNING for {execution_id} "
            f"(execution row present={exists}) — the run continues but its status "
            f"stays WAITING_FOR_APPROVAL, so the UI will keep showing the gate"
        )


async def _approval_status(approval_id: str) -> Optional[str]:
    from src.db.session import request_scoped_session
    from src.repositories.hitl_repository import HITLApprovalRepository

    async with request_scoped_session() as session:
        approval = await HITLApprovalRepository(session).get_by_id(approval_id)
        if approval is None:
            return None
        status = approval.status
        return status.value if hasattr(status, "value") else str(status)


def make_tool_approval_hook(execution_id: str, group_context: Optional[GroupContext]):
    """Build the pre-execution hook for one execution's tool calls."""
    from src.services.hitl.notify import notify_input_needed
    from src.services.tools.async_bridge import run_async_with_context

    group_id = getattr(group_context, "primary_group_id", None) or "default"

    def approval_hook(tool: Any, kwargs: Dict[str, Any], agent: Any, task: Any) -> None:
        policy = getattr(tool, "_approval_policy", None)
        if policy is None:
            return

        tool_name = getattr(tool, "name", type(tool).__name__)
        scope = str(policy.get("scope", _DEFAULT_SCOPE)).lower()

        # One decision per tool per run (default): an already-approved tool
        # proceeds silently; an already-denied one is auto-denied with the
        # same message the agent saw the first time. scope="call" re-asks.
        if scope != "call":
            decision = _run_decisions.get((execution_id, tool_name))
            if decision is None:
                try:
                    decision = run_async_with_context(
                        _prior_decision(execution_id, tool_name, group_id), timeout=15
                    )
                except Exception as prior_err:  # noqa: BLE001
                    logger.debug(f"[tool_approval] prior-decision lookup skipped: {prior_err}")
                if decision:
                    _run_decisions[(execution_id, tool_name)] = decision
            if decision == "approved":
                return
            if decision == "rejected":
                raise ToolExecutionBlockedError(
                    f"'{tool_name}' was denied by the human reviewer for this run."
                )

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
        notify_input_needed(execution_id, {
            "job_id": execution_id,
            "approval_id": approval_id,
            **gate_config,
        })

        def _record(decision: str) -> None:
            if scope != "call":
                _run_decisions[(execution_id, tool_name)] = decision
            try:
                run_async_with_context(_restore_running(execution_id), timeout=15)
            except Exception as restore_err:  # noqa: BLE001
                logger.debug(f"[tool_approval] status restore skipped: {restore_err}")

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                status = run_async_with_context(_approval_status(approval_id), timeout=15)
            except Exception as poll_err:  # noqa: BLE001
                logger.warning(f"[tool_approval] poll failed (retrying): {poll_err}")
                status = None
            if status == "approved":
                logger.info(f"[tool_approval] {approval_id} approved — running '{tool_name}'")
                _record("approved")
                return
            if status in ("rejected", "timeout"):
                _record("rejected")
                raise ToolExecutionBlockedError(
                    f"'{tool_name}' was {status} by the human reviewer."
                )
            time.sleep(_POLL_INTERVAL_SECONDS)

        if timeout_action == "approve":
            logger.warning(
                f"[tool_approval] {approval_id} timed out — policy allows proceed"
            )
            _record("approved")
            return
        _record("rejected")
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
        # Drop this run's cached decisions (matters for the long-lived
        # in-process light path; subprocesses die with the process anyway).
        for key in [k for k in _run_decisions if k[0] == execution_id]:
            _run_decisions.pop(key, None)

    return uninstall
