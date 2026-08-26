"""Event-driven triggers: a Lakebase queue that drives crew/flow runs.

The full choreography loop:
- **queue + consumer** (``queue_service`` / ``queue_consumer_service``): a
  non-blocking worker claims due rows and dispatches each target through the same
  path the scheduler uses.
- **subscriptions + emit rules** (``subscription_service``): bind an event name
  to a crew/flow to run (subscription), and a crew/flow's completion to an event
  it emits (emit rule).
- **emit-on-completion** (``emit_service``): when a run COMPLETES, its emit rules
  fan out onto the queue — one row per subscriber — so one crew's output triggers
  the next. See ``src/docs/EVENT_TRIGGERS.md``.
"""

from src.services.triggers.emit_service import EmitService, emit_for_completed_run
from src.services.triggers.queue_consumer_service import TriggerQueueConsumerService
from src.services.triggers.queue_service import TriggerQueueService
from src.services.triggers.subscription_service import SubscriptionService

__all__ = [
    "TriggerQueueConsumerService",
    "TriggerQueueService",
    "SubscriptionService",
    "EmitService",
    "emit_for_completed_run",
]
