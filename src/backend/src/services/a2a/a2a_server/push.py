"""Push notifications — the one genuinely new subsystem in either proposal.

Everything else on the external surfaces is an adapter over machinery Kasal
already had. This is not: nothing here existed, and it is the only place Kasal
makes an outbound request to an address a caller chose.

That last point is what shapes the module. A URL supplied by an external agent
and fetched server-side is the textbook SSRF setup, so delivery goes through
``assert_safe_outbound_url`` — the same guard the HITL webhooks use, which
requires https, rejects loopback, RFC1918, link-local and cloud-metadata
targets, re-resolves after DNS to defeat rebinding, and refuses to follow
redirects (a 30x is otherwise a way to reach an internal host that passed the
pre-flight check).

Deliveries are signed when the caller registered a secret, so the receiver can
tell a real notification from anyone who learned the URL.
"""

import hashlib
import hmac
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.services.external.identity import ExternalCaller

logger = logging.getLogger(__name__)

#: A receiver that is slow is a receiver that is down, as far as a notification
#: is concerned. Kept short so one bad endpoint cannot stall a run's updates.
DELIVERY_TIMEOUT_SECONDS = 10.0

#: Attempts per notification, with a widening gap. Beyond this the receiver is
#: not coming back within the life of one status change, and the caller can
#: still poll GetTask — push is an optimisation over polling, never the only
#: way to learn an outcome.
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (1.0, 4.0)

#: After this many consecutive failures the config stops being tried. A webhook
#: pointing at something permanently gone would otherwise cost every run three
#: timed-out requests forever.
FAILURE_LIMIT = 10


class PushConfigNotFound(Exception):
    """No such config, or not this caller's."""


async def register(
    caller: ExternalCaller,
    task_id: str,
    url: str,
    token: Optional[str] = None,
    secret: Optional[str] = None,
    session: Any = None,
) -> Dict[str, Any]:
    """Register a webhook for a task, or update it if the URL is already set.

    The URL is validated NOW as well as at delivery. Refusing an unusable
    endpoint at registration is the difference between a caller that fixes it
    immediately and one that waits for a notification that will never arrive.
    """
    from src.utils.url_security import UnsafeUrlError, assert_safe_outbound_url

    try:
        await assert_safe_outbound_url(url)
    except UnsafeUrlError as exc:
        raise ValueError(f"Webhook URL is not permitted: {exc}")

    # The caller must be able to see the task, or this becomes a way to attach a
    # webhook to another workspace's run and watch it.
    from src.services.external.invocation import run_status

    if await run_status(caller, task_id, session=session) is None:
        raise PushConfigNotFound(f"No task {task_id!r}")

    from src.models.a2a_push_config import A2APushConfig
    from src.repositories.a2a_push_config_repository import A2APushConfigRepository

    repository = A2APushConfigRepository(session)
    group_id = caller.group_context.primary_group_id
    existing = await repository.find_for_task_and_url(task_id, url, group_id)

    if existing is not None:
        # Re-registering the same URL is an update. A second row would silently
        # double every future notification for a caller that simply retried.
        existing.token = token
        existing.secret = secret
        existing.consecutive_failures = 0
        existing.last_error = None
        await session.flush()
        row = existing
    else:
        row = A2APushConfig(
            task_id=task_id,
            url=url,
            token=token,
            secret=secret,
            group_id=group_id,
            created_by_email=caller.identifier,
        )
        session.add(row)
        await session.flush()

    logger.info("[a2a-push] %s registered %s for task %s", caller.origin, url, task_id)
    return _to_dict(row)


async def list_for_task(
    caller: ExternalCaller, task_id: str, session: Any = None
) -> List[Dict[str, Any]]:
    """Configs registered on a task, group-scoped."""
    from src.repositories.a2a_push_config_repository import A2APushConfigRepository

    rows = await A2APushConfigRepository(session).list_for_task(
        task_id, caller.group_ids
    )
    return [_to_dict(r) for r in rows]


async def delete(caller: ExternalCaller, config_id: int, session: Any = None) -> bool:
    """Remove a config. False when it does not exist or is not this caller's."""
    from src.repositories.a2a_push_config_repository import A2APushConfigRepository

    removed = await A2APushConfigRepository(session).delete_for_group(
        config_id, caller.group_ids
    )
    return bool(removed)


async def deliver(task_id: str, payload: Dict[str, Any], session: Any) -> int:
    """POST ``payload`` to every webhook registered on the task.

    Returns how many succeeded. Never raises: this runs alongside a task's
    progress, and a webhook nobody is listening to must not affect the run that
    triggered it.
    """
    from src.repositories.a2a_push_config_repository import A2APushConfigRepository

    try:
        rows = await A2APushConfigRepository(session).list_deliverable(
            task_id, FAILURE_LIMIT
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[a2a-push] could not load configs for %s: %s", task_id, exc)
        return 0

    delivered = 0
    for row in rows:
        if await _deliver_one(row, payload):
            delivered += 1
    try:
        await session.flush()
    except Exception:  # noqa: BLE001
        logger.debug("[a2a-push] could not record delivery state", exc_info=True)
    return delivered


async def _deliver_one(row: Any, payload: Dict[str, Any]) -> bool:
    """One webhook, with retries. Records the outcome on the row."""
    import asyncio

    import httpx

    from src.utils.url_security import UnsafeUrlError, assert_safe_outbound_url

    body = json.dumps(payload, default=str)
    headers = {"Content-Type": "application/json"}
    if row.token:
        headers["Authorization"] = f"Bearer {row.token}"
    if row.secret:
        headers["X-Kasal-Signature"] = hmac.new(
            row.secret.encode(), body.encode(), hashlib.sha256
        ).hexdigest()

    last_error: Optional[str] = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            # Re-checked on EVERY attempt, not just at registration: DNS can
            # change between them, which is the whole point of rebinding.
            await assert_safe_outbound_url(row.url)

            async with httpx.AsyncClient(
                timeout=DELIVERY_TIMEOUT_SECONDS, follow_redirects=False
            ) as client:
                response = await client.post(row.url, content=body, headers=headers)

            if response.is_success:
                row.last_status = str(response.status_code)
                row.last_error = None
                row.consecutive_failures = 0
                row.last_attempt_at = datetime.utcnow()
                return True

            # Do not echo the receiver's body back anywhere: it is an untrusted
            # response to a server-side request.
            last_error = f"HTTP {response.status_code}"
        except UnsafeUrlError as exc:
            # Not retryable, and not the receiver being slow — stop immediately.
            last_error = f"unsafe URL: {exc}"
            break
        except Exception as exc:  # noqa: BLE001
            last_error = type(exc).__name__

        if attempt < MAX_ATTEMPTS - 1:
            await asyncio.sleep(BACKOFF_SECONDS[attempt])

    row.last_status = "failed"
    row.last_error = last_error
    row.consecutive_failures = (row.consecutive_failures or 0) + 1
    row.last_attempt_at = datetime.utcnow()
    logger.warning(
        "[a2a-push] delivery to %s failed (%s), %d consecutive",
        row.url,
        last_error,
        row.consecutive_failures,
    )
    return False


def _to_dict(row: Any) -> Dict[str, Any]:
    """A config as the caller sees it.

    The token and secret are NEVER returned. A caller that registered them has
    them; anything else reading this response should not.
    """
    return {
        "id": row.id,
        "taskId": row.task_id,
        "url": row.url,
        "authenticated": bool(row.token or row.secret),
        "lastStatus": row.last_status,
        "consecutiveFailures": row.consecutive_failures or 0,
    }
