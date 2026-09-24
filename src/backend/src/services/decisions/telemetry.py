"""Use the existing event pipeline; never create a second trace writer."""

import logging


def record_decision(policy: str, model: str, status: str, duration_ms: float):
    from src.core.events.bus import event_bus
    from src.core.events.types import DecisionEvaluatedEvent

    try:
        event_bus.emit(
            None,
            DecisionEvaluatedEvent(
                output=f"Jev {policy}: {status}",
                policy=policy,
                model=model,
                status=status,
                duration_ms=duration_ms,
            ),
        )
    except Exception:
        logging.getLogger(__name__).debug(
            "Decision telemetry unavailable", exc_info=True
        )
