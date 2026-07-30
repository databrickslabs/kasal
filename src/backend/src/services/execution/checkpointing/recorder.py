"""The base checkpoint recorder.

Four properties make this worth sharing, and the flow path had none of them
while it derived checkpoints from traces:

- **Event-driven.** The recorder subscribes to the run's event bus. No hooks in
  business logic, so nothing in the crew or flow builders has to remember to
  checkpoint.
- **Idempotent.** Units are keyed, so a retried write or a resumed run
  overwrites rather than appends.
- **Bounded.** Outputs are truncated at a fixed cap and the truncation is
  flagged, never silent.
- **Fail-open.** A checkpoint failure must never fail the run it exists to
  protect. Every handler swallows and logs.

Subclasses decide only what a *unit* is: a task for a crew, a crew for a flow.

Recorders run INSIDE the execution subprocess, so they own their database
session (via ``store``) rather than borrowing a request's.
"""

import logging
from typing import Any, Dict, Iterable, Optional, Tuple

logger = logging.getLogger(__name__)


class CheckpointRecorder:
    """Persists completed-unit checkpoints for one execution.

    Attributes:
        kind: The record kind this recorder writes ("crew" / "flow"); set by
            the subclass, since it selects how ``resume`` interprets the units.
    """

    kind: str = ""

    def __init__(
        self,
        job_id: str,
        unit_count: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
    ):
        if not self.kind:
            raise ValueError(f"{type(self).__name__} must declare a `kind`")
        self._job_id = job_id
        self._unit_count = unit_count
        self._meta = dict(meta or {})

    # ------------------------- subscription -------------------------

    def _subscriptions(self) -> Iterable[Tuple[type, Any]]:
        """(event class, handler) pairs this recorder listens on."""
        raise NotImplementedError

    def register(self, bus=None) -> "CheckpointRecorder":
        """Subscribe on the run's event bus.

        Args:
            bus: The event bus to register on. Defaults to the process-wide
                bus, which is what a subprocess wants.
        """
        if bus is None:
            from src.core.events import event_bus as default_bus

            bus = default_bus

        for event_class, handler in self._subscriptions():
            bus.register_handler(event_class, handler)

        logger.info(
            f"[CHECKPOINT] {type(self).__name__} registered for {self._job_id} "
            f"(kind={self.kind}, units={self._unit_count})"
        )
        return self

    # ------------------------- persistence -------------------------

    def _persist(self, unit: Dict[str, Any]) -> None:
        """Record one completed unit. Never raises."""
        from src.services.execution.checkpointing import store
        from src.services.tools.async_bridge import run_async_with_context

        try:
            run_async_with_context(
                store.record_unit(
                    self._job_id,
                    kind=self.kind,
                    unit=unit,
                    unit_count=self._unit_count,
                    meta=self._meta or None,
                ),
                timeout=60,
            )
        except Exception as e:  # noqa: BLE001 — checkpointing must never break a run
            logger.warning(
                f"[CHECKPOINT] Failed to persist unit {unit.get('key')} for "
                f"{self._job_id} (non-fatal): {e}"
            )

    def _clear(self) -> None:
        """Drop the checkpoint after a successful run. Never raises."""
        from src.services.execution.checkpointing import store
        from src.services.tools.async_bridge import run_async_with_context

        try:
            run_async_with_context(store.clear(self._job_id), timeout=60)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"[CHECKPOINT] Failed to clear checkpoint for "
                f"{self._job_id} (non-fatal): {e}"
            )
