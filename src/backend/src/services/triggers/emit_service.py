"""Emit-on-completion: fan a finished run's emit rules out onto the queue.

When a crew/flow run reaches COMPLETED, any EMIT RULE whose ``on_target`` is that
crew/flow fires: for each, every enabled SUBSCRIPTION to the rule's event becomes
a queue row (``target`` = the subscriber, ``payload`` carries the finished run's
output). The queue consumer then dispatches those rows like any other — so a
completed producer automatically triggers its consumers, closing the
choreography loop (emit → subscribe → run).

Reads only triggers-domain data; the finished run's identity and output are
passed IN by the execution layer, so this never reaches into the execution
domain's repositories.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from sqlalchemy.exc import IntegrityError

from src.core.logger import LoggerManager
from src.db.session import routed_scoped_session
from src.repositories.event_subscription_repository import (
    EmitRuleRepository,
    EventSubscriptionRepository,
)
from src.repositories.trigger_queue_repository import TriggerQueueRepository
from src.services.triggers.event_types import EventType, canonical_event_name

logger = LoggerManager.get_instance().system


#: Chain-depth ceiling. Every emitted row carries ``event.hops`` = its parent's
#: depth + 1; a completion at or past the cap emits nothing. This is the guard
#: against choreography cycles (A → B → A, or a handler subscribed to its own
#: ``failed`` event): without it, one self-subscription generates runs — and LLM
#: spend — forever.
def _max_hops() -> int:
    try:
        return max(1, int(os.getenv("KASAL_EVENT_TRIGGERS_MAX_HOPS", "5")))
    except (TypeError, ValueError):
        return 5


def _result_to_inputs(sub: Any, result: Any, event_type: str) -> Dict[str, Any]:
    """Derive the downstream run's inputs from the upstream output.

    An explicit ``input_mapping`` on the subscription wins (static overrides —
    it is NOT a payload projection). A ``failed`` event carries the error under
    ``error`` so a handler crew's template can name it honestly; otherwise the
    raw output is passed through under ``payload`` (stringified when it is not a
    mapping, so a task template can interpolate it).
    """
    mapping = getattr(sub, "input_mapping", None)
    if mapping:
        return dict(mapping)
    if event_type == EventType.FAILED.value:
        return {"error": str(result) if result is not None else ""}
    if isinstance(result, dict):
        return {"payload": result}
    return {"payload": str(result) if result is not None else ""}


class EmitService:
    """Turn a completed run into downstream trigger rows.

    The session is injected and this COMMITS it after enqueueing (the module
    wrapper below hands it a dedicated session for exactly that reason — do not
    inject a request session that is mid-transaction)."""

    def __init__(self, session: Any) -> None:
        self.session = session
        self.rules = EmitRuleRepository(session)
        self.subs = EventSubscriptionRepository(session)
        self.queue = TriggerQueueRepository(session)

    async def emit_for_completed_run(
        self,
        *,
        execution_type: Optional[str],
        flow_id: Any,
        crew_id: Any,
        group_id: Optional[str],
        job_id: str,
        result: Any,
        event_type: str = EventType.COMPLETED.value,
        correlation_id: Optional[str] = None,
        hops: int = 0,
    ) -> int:
        """Enqueue one trigger row per subscriber of this producer's event.

        The producer's emitted event has a standard name
        ``{kind}:{id}:{event_type}`` (see ``event_types.canonical_event_name``).
        An emit rule opts the producer in to a lifecycle type; subscriptions store
        the full canonical name they listen for. Returns how many rows enqueued.

        ``correlation_id`` threads the ORIGIN of a chain (defaults to this run's
        job id when it starts one); ``hops`` is this run's depth in that chain —
        at ``KASAL_EVENT_TRIGGERS_MAX_HOPS`` the chain stops here.
        """
        if hops >= _max_hops():
            logger.warning(
                "[Emit] run %s at hop %d reached the chain-depth cap (%d) — "
                "emitting nothing. A legitimate deeper pipeline can raise "
                "KASAL_EVENT_TRIGGERS_MAX_HOPS; a cycle should be re-wired.",
                job_id,
                hops,
                _max_hops(),
            )
            return 0
        kind = "flow" if execution_type == "flow" else "crew"
        target_id = flow_id if kind == "flow" else crew_id
        if not target_id:
            # An ad-hoc run with no saved definition can't be matched to a rule.
            return 0

        group_ids = [group_id] if group_id else None
        rules = await self.rules.find_enabled_for_target(
            kind, str(target_id), group_ids
        )
        # The producer only emits if it has an enabled rule for THIS lifecycle type.
        if not any((r.event_type or "") == event_type for r in rules):
            return 0

        canonical = canonical_event_name(kind, str(target_id), event_type)
        subs = await self.subs.find_enabled_by_event_type(canonical, group_ids or [])
        if not subs:
            return 0

        # Carry a schema pointer if the emit rule declared one for this type.
        emit_schema = next(
            (
                r.schema_ref
                for r in rules
                if (r.event_type or "") == event_type and r.schema_ref
            ),
            None,
        )

        thread_id = correlation_id or job_id
        enqueued = 0
        for sub in subs:
            # A subscription whose target IS the producer is a self-loop: the
            # run it launches completes and emits this same event again. There
            # is no terminating condition in the choreography layer, so it can
            # never mean anything but "run forever" — refuse it outright
            # instead of letting it burn through the hop cap.
            tgt = sub.target or {}
            if tgt.get("kind") == kind and str(tgt.get("id") or "") == str(target_id):
                logger.warning(
                    "[Emit] subscription %s targets its own producer (%s) — "
                    "skipped. Point it at a different crew/flow to chain runs.",
                    sub.id,
                    canonical,
                )
                continue
            try:
                # Savepoint per row: a duplicate must skip THIS row without
                # poisoning the batch. The deterministic idempotency key makes a
                # double emission (two terminal-status writers racing, or a
                # crash-retry) collapse onto the unique constraint instead of
                # double-firing the subscriber.
                async with self.session.begin_nested():
                    await self.queue.enqueue(
                        group_id=group_id,
                        event_type=canonical,
                        target=sub.target,
                        payload={
                            "inputs": _result_to_inputs(sub, result, event_type),
                            # A pointer, not the state: the schema names the payload
                            # contract; the source run lets a consumer trace the chain.
                            "event": {
                                "type": canonical,
                                "schema": sub.schema_ref or emit_schema,
                                "source_run": job_id,
                                "hops": hops + 1,
                            },
                        },
                        correlation_id=thread_id,
                        causation_run_id=job_id,
                        idempotency_key=f"emit:{job_id}:{event_type}:{sub.id}",
                    )
                enqueued += 1
            except IntegrityError:
                logger.info(
                    "[Emit] duplicate emission suppressed (run %s, sub %s)",
                    job_id,
                    sub.id,
                )

        if enqueued:
            await self.session.commit()
        return enqueued


async def emit_for_completed_run(
    *,
    execution_type: Optional[str],
    flow_id: Any,
    crew_id: Any,
    group_id: Optional[str],
    job_id: str,
    result: Any,
    event_type: str = EventType.COMPLETED.value,
    correlation_id: Optional[str] = None,
    hops: int = 0,
) -> int:
    """Completion-hook entry point for the execution layer.

    Non-fatal by contract: acquires its OWN routed session (so a failure cannot
    poison the caller's status transaction) and swallows every error — a run's
    status update must never fail because a downstream event could not be queued.
    """
    try:
        async with routed_scoped_session() as session:
            # Gated on the same admin toggle as the consumer: feature OFF means a
            # completed run emits nothing (no orphan pending rows pile up).
            from src.services.settings.engine import EngineConfigService

            if not await EngineConfigService(session).get_event_triggers_enabled():
                return 0
            n = await EmitService(session).emit_for_completed_run(
                execution_type=execution_type,
                flow_id=flow_id,
                crew_id=crew_id,
                group_id=group_id,
                job_id=job_id,
                result=result,
                event_type=event_type,
                correlation_id=correlation_id,
                hops=hops,
            )
        if n:
            logger.info("[Emit] run %s emitted %d downstream trigger(s)", job_id, n)
        return n
    except Exception as exc:  # noqa: BLE001 — never fail a status update over this
        logger.warning("[Emit] emit-on-completion skipped for run %s: %s", job_id, exc)
        return 0
