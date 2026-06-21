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
            from src.engines.crewai.services.crew_memory_service import CrewMemoryService
            svc = CrewMemoryService({"group_id": group_id, "execution_id": job_id})
        # memory_backend_config is unused by attach_memory_trace_context
        # (it reads only self.config); pass None.
        svc.attach_memory_trace_context(crew, None, crew_kwargs)
        svc.attach_tools_trace_context(crew, crew_kwargs)
    except Exception as exc:  # pragma: no cover - best-effort instrumentation
        logger.debug("[trace-context] attach skipped: %s", exc)
