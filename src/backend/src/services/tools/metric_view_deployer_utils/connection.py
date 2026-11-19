"""Retrying a SQL Statement API request whose connection broke.

A proxy between the deployer and the workspace (a corporate TLS inspector) sometimes resets
the handshake: ``ConnectionResetError: [Errno 104] Connection reset by peer``. One such blip
used to fail the view being deployed. Repeating the request is safe, because every statement
the deployer sends is idempotent (CREATE OR REPLACE, IF NOT EXISTS, DESCRIBE, SHOW) and a
reset handshake means the request was never received. Timeouts are NOT retried: there the
statement may already be running.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

import requests

logger = logging.getLogger(__name__)

CONNECT_ATTEMPTS = 3
_R = TypeVar("_R")


def retry_connection_reset(send: Callable[[], _R]) -> _R:
    """``send()``, repeated up to ``CONNECT_ATTEMPTS`` times while the connection breaks."""
    for attempt in range(CONNECT_ATTEMPTS - 1):
        try:
            return send()
        except requests.ConnectionError as exc:
            logger.warning(
                "[MVDeployer] connection broke (%s), retrying", type(exc).__name__
            )
            time.sleep(2**attempt)
    return send()
