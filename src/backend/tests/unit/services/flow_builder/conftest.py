"""Executor tests run child entry points in-process; keep that contained.

``crewai.events.event_bus`` and ``src.services.otel_tracing`` are imported for
real here, before any test stubs them. The executor tests wrap runs in
``patch.dict("sys.modules", {"src.core.events": MagicMock(), ...})``, and some
also set ``"opentelemetry"`` to None. First imported under that patch, both
fail: ``patch("src.services.otel_tracing.…")`` raises AttributeError and the
event bridge's lazy ``crewai.events.event_bus`` import fails the flow. Those
tests passed only when an earlier file in the xdist worker had imported them.
"""

import crewai.events.event_bus  # noqa: F401

import src.services.otel_tracing  # noqa: F401
from tests.unit.services.run_isolation import (  # noqa: F401
    isolated_run_gate,
    restore_child_process_globals,
)
