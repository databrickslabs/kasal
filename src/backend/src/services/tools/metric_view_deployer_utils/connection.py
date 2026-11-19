"""Retrying a SQL Statement API request whose connection broke.

A proxy between the deployer and the workspace (a corporate TLS inspector) sometimes resets
the handshake: ``ConnectionResetError: [Errno 104] Connection reset by peer``. One such blip
used to fail the view being deployed. Repeating a request is safe when its statement can run
twice without harm (CREATE OR REPLACE, IF NOT EXISTS, DESCRIBE, SHOW, a status poll). A
statement that writes rows (the snapshot loader's INSERT) is NOT repeated: a connection can
also break after the server accepted it, and a second run would load the rows twice. Timeouts
are not retried either: there the statement may already be running.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from typing import TypeVar

import requests

logger = logging.getLogger(__name__)

CONNECT_ATTEMPTS = 3
_R = TypeVar("_R")


_WRITES_ROWS = re.compile(r"\s*(?:INSERT|MERGE|UPDATE|DELETE|COPY)\b", re.IGNORECASE)


def retry_connection_reset(send: Callable[[], _R], statement: str = "") -> _R:
    """``send()``, repeated up to ``CONNECT_ATTEMPTS`` times while the connection breaks;
    sent once when ``statement`` writes rows."""
    if _WRITES_ROWS.match(statement):
        return send()
    for attempt in range(CONNECT_ATTEMPTS - 1):
        try:
            return send()
        except requests.ConnectionError as exc:
            logger.warning(
                "[MVDeployer] connection broke (%s), retrying", type(exc).__name__
            )
            time.sleep(2**attempt)
    return send()
