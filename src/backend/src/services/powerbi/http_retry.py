"""Throttling (HTTP 429) retry shared by every Power BI and Fabric REST call.

One policy for all call sites (executeQueries, getDefinition and its operation polls, report and refresh
listings, admin scanner calls): wait as the service asks, else back off exponentially, and give up after a
bounded number of attempts or total wait. The last response is returned unchanged so the caller reports it.

The helpers take any response with ``status_code``, ``headers`` and ``text`` (``requests`` and ``httpx``
responses both qualify), and never log the URL, headers or bodies: only the label, the attempt and the wait.

Stdlib only. ``pipeline_config.py`` also loads this file by path (the standalone CLI), so it must not import
its siblings.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Protocol, TypeVar

logger = logging.getLogger(__name__)

THROTTLED = 429
#: Calls made per logical request: the first call plus up to four retries.
MAX_ATTEMPTS = 5
#: Longest single wait, whatever the service asks for.
MAX_WAIT_PER_RETRY_S = 120.0
#: Waiting budget per logical request; a retry that would exceed it is not made.
MAX_TOTAL_WAIT_S = 240.0
_BACKOFF_BASE_S = 2.0
#: Added to a service-given wait so the retry does not land on the boundary.
_HINT_MARGIN_S = 1.0
_RETRY_IN = re.compile(r"Retry in (\d{1,4}) seconds", re.IGNORECASE)
_BODY_SCAN_CHARS = 500


class ThrottledResponse(Protocol):
    """The three attributes the retry policy reads; requests and httpx responses both satisfy it."""

    @property
    def status_code(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def text(self) -> str: ...


R = TypeVar("R", bound=ThrottledResponse)


def retry_after_seconds(response: ThrottledResponse) -> float | None:
    """Seconds the service asked us to wait: the ``Retry-After`` header (seconds or HTTP date), else the
    ``Retry in N seconds`` text of the message. None when it said nothing usable."""
    header = str(response.headers.get("Retry-After", "")).strip()
    if header.isdigit():
        return float(header)
    if header:
        try:
            when = parsedate_to_datetime(header)
        except (TypeError, ValueError):
            when = None
        if when is not None:
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    match = _RETRY_IN.search(response.text[:_BODY_SCAN_CHARS])
    return float(match.group(1)) if match else None


class ThrottleBudget:
    """The attempts and the waiting left for one logical request.

    ``wait_before_retry`` takes the response just received and returns the seconds to sleep before the next
    attempt, or None when the response is not throttled or the budget is spent (the caller then returns it).
    """

    def __init__(self, label: str) -> None:
        self._label = label
        self._attempt = 1
        self._waited = 0.0

    def wait_before_retry(self, response: ThrottledResponse) -> float | None:
        if response.status_code != THROTTLED or self._attempt >= MAX_ATTEMPTS:
            return None
        hint = retry_after_seconds(response)
        if hint is None:
            seconds = min(
                MAX_WAIT_PER_RETRY_S, _BACKOFF_BASE_S * float(1 << (self._attempt - 1))
            )
        else:
            seconds = min(MAX_WAIT_PER_RETRY_S, hint + _HINT_MARGIN_S)
        if self._waited + seconds > MAX_TOTAL_WAIT_S:
            logger.info(
                "%s: throttled (HTTP 429) and the waiting budget is spent; not retrying",
                self._label,
            )
            return None
        self._attempt += 1
        self._waited += seconds
        logger.info(
            "%s: throttled (HTTP 429); retry %d of %d in %.0fs",
            self._label,
            self._attempt - 1,
            MAX_ATTEMPTS - 1,
            seconds,
        )
        return seconds


def send_with_throttle_retry(send: Callable[[], R], label: str) -> R:
    """Call ``send`` until it is not throttled, within the budget. Synchronous (``time.sleep``)."""
    budget = ThrottleBudget(label)
    response = send()
    while (seconds := budget.wait_before_retry(response)) is not None:
        time.sleep(seconds)
        response = send()
    return response


async def send_with_throttle_retry_async(
    send: Callable[[], Awaitable[R]], label: str
) -> R:
    """Asynchronous twin of :func:`send_with_throttle_retry` (``asyncio.sleep``, never blocks the loop)."""
    budget = ThrottleBudget(label)
    response = await send()
    while (seconds := budget.wait_before_retry(response)) is not None:
        await asyncio.sleep(seconds)
        response = await send()
    return response
