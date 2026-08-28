"""HTTP surface for the event-trigger queue (``/triggers``).

Thin boundary: enqueue an event, list/inspect/delete queued events. All work is
group-scoped in ``TriggerQueueService``. The background consumer
(``services/triggers/queue_consumer_service``) is what actually drains the queue
and launches runs — this router only manages the rows.
"""

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.exc import IntegrityError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import ConflictError, ForbiddenError
from src.core.permissions import check_role_in_context
from src.schemas.triggers import (
    DispatchResult,
    EmitRuleCreate,
    EmitRuleResponse,
    EnqueueTrigger,
    SubscriptionCreate,
    SubscriptionListResponse,
    SubscriptionResponse,
    TriggerEventResponse,
    TriggerListResponse,
)
from src.services.triggers import (
    SubscriptionService,
    TriggerQueueConsumerService,
    TriggerQueueService,
)

router = APIRouter(
    prefix="/triggers",
    tags=["triggers"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


async def get_trigger_queue_service(session: SessionDep) -> TriggerQueueService:
    """Router → Service → Repository → DB."""
    return TriggerQueueService(session)


TriggerQueueServiceDep = Annotated[
    TriggerQueueService, Depends(get_trigger_queue_service)
]


async def get_subscription_service(session: SessionDep) -> SubscriptionService:
    return SubscriptionService(session)


SubscriptionServiceDep = Annotated[
    SubscriptionService, Depends(get_subscription_service)
]

# Shared, module-level consumer for the on-demand "Dispatch now" endpoint. It
# keeps strong refs to in-flight dispatch tasks (its ``_tasks`` set), so holding
# the instance here — rather than constructing a throwaway per request — is what
# stops those background runs from being garbage-collected when the request ends.
_dispatch_consumer = TriggerQueueConsumerService()


@router.post(
    "", response_model=TriggerEventResponse, status_code=status.HTTP_201_CREATED
)
async def enqueue_trigger(
    payload: EnqueueTrigger,
    service: TriggerQueueServiceDep,
    group_context: GroupContextDep,
) -> TriggerEventResponse:
    """Enqueue an event that will trigger a crew/flow run.

    Roles: Admin / Editor / Operator — enqueueing is starting a run.
    """
    if not check_role_in_context(group_context, ["admin", "editor", "operator"]):
        raise ForbiddenError("Enqueueing trigger events requires operator access")
    try:
        row = await service.enqueue(payload, group_context)
    except IntegrityError as exc:
        raise ConflictError(
            f"A trigger event with idempotency_key "
            f"{payload.idempotency_key!r} already exists"
        ) from exc
    return TriggerEventResponse.model_validate(row)


@router.get("", response_model=TriggerListResponse)
async def list_triggers(
    service: TriggerQueueServiceDep,
    group_context: GroupContextDep,
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = Query(50, ge=1, le=200),
) -> TriggerListResponse:
    """List this tenant's queued events, most recent first."""
    rows = await service.list_events(group_context, status=status_filter, limit=limit)
    events = [TriggerEventResponse.model_validate(row) for row in rows]
    return TriggerListResponse(events=events, total=len(events))


# --- Subscriptions (event → crew/flow) + emit rules (crew/flow → event) ---
# Declared BEFORE the /{event_id} routes so these literal paths are not captured
# by the int path param.


@router.get("/subscriptions", response_model=SubscriptionListResponse)
async def list_subscriptions(
    service: SubscriptionServiceDep,
    group_context: GroupContextDep,
) -> SubscriptionListResponse:
    subs = await service.list_subscriptions(group_context)
    rules = await service.list_emit_rules(group_context)
    return SubscriptionListResponse(
        subscriptions=[SubscriptionResponse.model_validate(s) for s in subs],
        emit_rules=[EmitRuleResponse.model_validate(r) for r in rules],
    )


@router.post(
    "/subscriptions",
    response_model=SubscriptionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_subscription(
    payload: SubscriptionCreate,
    service: SubscriptionServiceDep,
    group_context: GroupContextDep,
) -> SubscriptionResponse:
    # Roles: Admin / Editor — choreography config changes what runs automatically.
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Managing subscriptions requires editor access")
    row = await service.create_subscription(payload, group_context)
    return SubscriptionResponse.model_validate(row)


@router.delete("/subscriptions/{sub_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    sub_id: Annotated[int, Path(...)],
    service: SubscriptionServiceDep,
    group_context: GroupContextDep,
) -> None:
    # Roles: Admin / Editor.
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Managing subscriptions requires editor access")
    await service.delete_subscription(sub_id, group_context)


@router.post(
    "/emit-rules",
    response_model=EmitRuleResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_emit_rule(
    payload: EmitRuleCreate,
    service: SubscriptionServiceDep,
    group_context: GroupContextDep,
) -> EmitRuleResponse:
    # Roles: Admin / Editor — choreography config changes what runs automatically.
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Managing emit rules requires editor access")
    row = await service.create_emit_rule(payload, group_context)
    return EmitRuleResponse.model_validate(row)


@router.delete("/emit-rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_emit_rule(
    rule_id: Annotated[int, Path(...)],
    service: SubscriptionServiceDep,
    group_context: GroupContextDep,
) -> None:
    # Roles: Admin / Editor.
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Managing emit rules requires editor access")
    await service.delete_emit_rule(rule_id, group_context)


@router.post("/dispatch", response_model=DispatchResult)
async def dispatch_pending(
    group_context: GroupContextDep,
    session: SessionDep,
    batch: int = Query(10, ge=1, le=50),
) -> DispatchResult:
    """Drain the caller's queue on demand: claim up to ``batch`` of THIS
    tenant's due events and launch a run for each in the background.

    The manual equivalent of the background consumer loop (which is opt-in via
    the ``event_triggers_enabled`` setting in Configuration → Engines) — it lets
    a fired event be dispatched immediately without waiting for the poller. Each
    claimed row moves ``pending`` → ``claimed`` → ``dispatched`` as its run is
    launched; refresh the event list to watch the transitions.

    Roles: Admin / Editor / Operator. Scoped to the caller's groups — this can
    never drain (or fast-forward the backoff of) another tenant's rows, and it
    honours the same admin gate as the background consumer.
    """
    if not check_role_in_context(group_context, ["admin", "editor", "operator"]):
        raise ForbiddenError("Dispatching trigger events requires operator access")
    from src.services.settings.engine import EngineConfigService

    if not await EngineConfigService(session).get_event_triggers_enabled():
        raise ForbiddenError(
            "Event triggers are disabled — enable them in Configuration -> Engines"
        )
    group_ids = group_context.group_ids or []
    if not group_ids:
        return DispatchResult(claimed=0)
    tasks = await _dispatch_consumer.claim_and_dispatch(batch, group_ids=group_ids)
    return DispatchResult(claimed=len(tasks))


@router.get("/{event_id}", response_model=TriggerEventResponse)
async def get_trigger(
    event_id: Annotated[int, Path(...)],
    service: TriggerQueueServiceDep,
    group_context: GroupContextDep,
) -> TriggerEventResponse:
    row = await service.get_event(event_id, group_context)
    return TriggerEventResponse.model_validate(row)


@router.delete("/{event_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_trigger(
    event_id: Annotated[int, Path(...)],
    service: TriggerQueueServiceDep,
    group_context: GroupContextDep,
) -> None:
    # Roles: Admin / Editor / Operator.
    if not check_role_in_context(group_context, ["admin", "editor", "operator"]):
        raise ForbiddenError("Deleting trigger events requires operator access")
    await service.delete_event(event_id, group_context)
