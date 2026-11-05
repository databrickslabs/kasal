"""Backoff for Genie rate limits: retry HTTP 429 and 503 with growing, capped waits.

The wrapper keeps a count of the retries it made, so each answer can report how often
it had to wait. ``sleep`` is injected so tests do not wait.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from src.services.tools.genie_space_sync_utils.genie_api import GenieApiError, GenieHttp

RETRY_STATUSES = frozenset({429, 503})


class RetryingHttp:
    """A ``GenieHttp`` that waits and retries on rate limits and transient 503s."""

    def __init__(
        self,
        http: GenieHttp,
        *,
        attempts: int = 5,
        base_s: float = 2.0,
        cap_s: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._http = http
        self._attempts = max(1, attempts)
        self._base_s = base_s
        self._cap_s = cap_s
        self._sleep = sleep
        self.retries = 0

    def __call__(
        self,
        method: str,
        path: str,
        body: dict[str, object] | None,
        params: dict[str, str] | None,
    ) -> dict[str, object]:
        attempt = 0
        while True:
            try:
                return self._http(method, path, body, params)
            except GenieApiError as exc:
                attempt += 1
                if exc.status not in RETRY_STATUSES or attempt >= self._attempts:
                    raise
                self.retries += 1
                self._sleep(min(self._cap_s, self._base_s * 2 ** (attempt - 1)))
