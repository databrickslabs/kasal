"""Telling external subscribers that a run changed state.

The execution layer knows WHEN a run's state changes; the protocol adapters know
HOW to tell someone. This is the seam between them, and it exists so the
execution layer never has to import an adapter — it calls one neutral function
and does not learn that A2A, or webhooks, exist at all.

Fire-and-forget by construction. A notification is an optimisation over polling:
a subscriber that cannot be reached must not slow, fail or otherwise affect the
run that triggered it. Every failure here is swallowed and logged.
"""

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def notify_state_change(
    job_id: str, status: str, result: Any = None, session: Any = None
) -> None:
    """Schedule notification of subscribers for ``job_id``.

    Synchronous and returns immediately: it schedules a task and does not await
    it. Awaiting delivery inside the status-update path would put a caller's
    unreachable webhook — three attempts with backoff — directly in the way of a
    crew's progress.

    Its own session, not the caller's: delivery outlives this call, and using a
    session that is about to be committed and closed by the request that
    scheduled us is how "the run finished but the notification raised
    InterfaceError" happens.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop (a sync context, or shutdown). Nothing to schedule onto, and
        # notification is never worth blocking for.
        return

    asyncio.create_task(_notify(job_id, status, result))


async def _notify(job_id: str, status: str, result: Any) -> None:
    try:
        from src.db.session import get_isolated_db_session
        from src.services.a2a import push
        from src.services.external.state import is_terminal, to_external_state

        state = to_external_state(status)
        payload = {
            # The A2A wire shape for a status update. Subscribers registered
            # through the A2A surface, so they get A2A's vocabulary.
            "taskId": job_id,
            "kind": "status-update",
            "status": {"state": f"TASK_STATE_{state.value.upper()}"},
            "final": is_terminal(state),
        }
        if is_terminal(state) and result is not None:
            payload["result"] = str(result)[:4000]

        async with get_isolated_db_session() as session:
            delivered = await push.deliver(job_id, payload, session)
            await session.commit()

        if delivered:
            logger.info(
                "[external] notified %d subscriber(s) of %s -> %s",
                delivered,
                job_id,
                state.value,
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[external] notification for %s skipped: %s", job_id, exc)


def subscribers_exist(job_id: Optional[str]) -> bool:
    """Cheap guard for callers that want to skip the scheduling entirely.

    Deliberately optimistic — it does not query. The real filter is in
    ``push.deliver``, which loads nothing when no config matches; this only
    exists so a caller can avoid creating a task per status change if it wants.
    """
    return bool(job_id)
