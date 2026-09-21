"""Execution trace-context attach shared by the crew and flow paths.

After a crew is built, its memory storages and tools are tagged with the
execution's job_id + group attribution so custom trace events (e.g. llm_call)
carry the right ownership. Both the crew path (``crew_preparation``) and the
flow path (``flow.modules.flow_methods``) attach this identically through the
single entry point here.
"""

from typing import Any, Dict, Optional

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

# Process-scoped current execution id (job_id). A crew/flow subprocess serves
# exactly one run (see execution/harnesses/selection.py), so a module global is
# a safe, instance-independent source of the execution id. It is the fallback a
# tool uses when its own ``trace_context`` was not attached to the instance that
# actually executes — e.g. when ToolFactory re-creates the tool at task-exec
# time, after attach_tools_trace_context ran on an earlier instance. Without it,
# config-gen persisted powerbi_extraction rows with a NULL execution_id, which
# broke the UC Metric View Generator's DB-fallback handoff (0 views).
_CURRENT_EXECUTION_ID: Optional[str] = None


def set_current_execution_id(job_id: Optional[str]) -> None:
    """Record the running execution's job_id for this process (best-effort)."""
    global _CURRENT_EXECUTION_ID
    if job_id:
        _CURRENT_EXECUTION_ID = job_id


def get_current_execution_id() -> Optional[str]:
    """Return the process-scoped execution job_id, or None if unset."""
    return _CURRENT_EXECUTION_ID


def resolve_tool_execution_id(tool: Any) -> Optional[str]:
    """Job id for a tool: its attached ``trace_context`` first, else the
    process-scoped current execution id. Use this wherever a tool persists or
    looks up a row keyed by execution_id — it survives ToolFactory rebuilding
    the instance after ``attach_tools_trace_context`` ran on an earlier one."""
    jid = (getattr(tool, "trace_context", None) or {}).get("job_id")
    return jid or _CURRENT_EXECUTION_ID


def attach_execution_trace_context(
    crew: Any,
    crew_kwargs: Dict[str, Any],
    *,
    group_id: Optional[str] = None,
    job_id: Optional[str] = None,
    service: Optional[Any] = None,
) -> None:
    """Attach execution trace context to a crew's memory storages and tools.

    - Crew passes its existing ``service`` (a ``CrewMemoryService``) so exec_id/
      group_id come from that service's already-built config — behavior identical
      to the prior inline calls.
    - Flow passes ``group_id``/``job_id`` and a minimal service is built, exactly
      as the flow code did inline before.

    Calls ``attach_memory_trace_context`` then ``attach_tools_trace_context`` in
    that order. Never raises — trace context is best-effort instrumentation.
    """
    try:
        svc = service
        if svc is None:
            from src.services.memory.run.crew_memory import CrewMemoryService

            svc = CrewMemoryService({"group_id": group_id, "execution_id": job_id})
        # Record the run's job_id process-wide so tools whose per-instance
        # trace_context was not attached (e.g. ToolFactory rebuilt the instance
        # after this ran) can still recover it. Prefer the explicit job_id, else
        # the service's configured execution_id (the crew path passes a service).
        _svc_exec_id = None
        try:
            _svc_exec_id = (getattr(svc, "config", None) or {}).get("execution_id")
        except Exception:
            _svc_exec_id = None
        set_current_execution_id(job_id or _svc_exec_id)
        # memory_backend_config is unused by attach_memory_trace_context
        # (it reads only self.config); pass None.
        svc.attach_memory_trace_context(crew, None, crew_kwargs)
        svc.attach_tools_trace_context(crew, crew_kwargs)
    except Exception as exc:  # pragma: no cover - best-effort instrumentation
        logger.debug("[trace-context] attach skipped: %s", exc)
