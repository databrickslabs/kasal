"""
API rate limiting — pure-ASGI middleware (SSE-safe).

Curbs cost / DoS abuse of expensive endpoints (e.g. LLM generation) with a
generous per-identity request limit.

Design decisions:
  - **Pure ASGI** (not BaseHTTPMiddleware) so it never buffers StreamingResponse
    bodies — critical for SSE streaming through the Databricks Apps HTTP/2 proxy
    (same reason UserContextMiddleware / SecurityHeadersMiddleware are pure ASGI).
    This is also why we use the ``limits`` engine directly rather than slowapi —
    slowapi's middleware is BaseHTTPMiddleware and would buffer SSE.
  - **Only the API surface** (``/api/``) is limited; SSE streams, health checks,
    and static/frontend asset loads are exempt (a single page load fetches many
    assets and must not exhaust the budget).
  - **Per identity**: keyed by the proxy-supplied ``X-Forwarded-Email`` (falling
    back to client IP), so one tenant's abuse can't rate-limit another.
  - **In-memory by default** (correct for a single app instance). For a
    horizontally-scaled deployment set ``RATE_LIMIT_STORAGE_URI`` (e.g.
    ``redis://host:6379``) so the budget is shared across replicas.
  - **Guarded**: if the ``limits`` package is unavailable, the middleware is a
    transparent pass-through (never raises) — so the dependency can roll out via
    the next ``uv sync`` without breaking a running ``--reload`` dev server.

Configuration (env vars):
  - ``RATE_LIMIT_ENABLED``      — "false"/"0"/"no"/"off" disables it (default on)
  - ``RATE_LIMIT_DEFAULT``      — limit string, default ``"600/minute"``
  - ``RATE_LIMIT_STORAGE_URI``  — limits storage URI, default in-memory
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

_DISABLED_VALUES = {"0", "false", "no", "off"}


def _rate_limit_enabled() -> bool:
    val = os.getenv("RATE_LIMIT_ENABLED", "true").strip().lower()
    return val not in _DISABLED_VALUES


class RateLimitMiddleware:
    """Pure-ASGI per-identity rate limiter for the ``/api/`` surface."""

    def __init__(self, app):
        self.app = app
        self._active = False

        if not _rate_limit_enabled():
            logger.info("[RATE_LIMIT] Disabled via RATE_LIMIT_ENABLED")
            return

        try:
            from limits import parse
            from limits.storage import storage_from_string
            from limits.strategies import FixedWindowRateLimiter
        except ImportError:
            logger.warning(
                "[RATE_LIMIT] 'limits' package not installed — rate limiting disabled "
                "(will activate after the next dependency sync)."
            )
            return

        limit_str = os.getenv("RATE_LIMIT_DEFAULT", "600/minute")
        storage_uri = os.getenv("RATE_LIMIT_STORAGE_URI") or "memory://"
        try:
            self._item = parse(limit_str)
            self._limiter = FixedWindowRateLimiter(storage_from_string(storage_uri))
        except Exception as exc:  # pragma: no cover — defensive (bad config)
            logger.error(
                "[RATE_LIMIT] Invalid config (%r, %r): %s", limit_str, storage_uri, exc
            )
            return

        self._active = True
        if storage_uri == "memory://":
            backend = "in-memory (per-process)"
        else:
            backend = f"shared ({storage_uri.split('://')[0]})"
        logger.info(
            "[RATE_LIMIT] Enabled %s/identity on /api/ (storage=%s)",
            limit_str,
            backend,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _should_limit(path: str) -> bool:
        """Only the API surface is limited; SSE / health are always exempt."""
        if not path.startswith("/api/"):
            return False  # static/frontend/docs — never rate-limited
        if "/sse/" in path or path.endswith("/stream"):
            return False  # long-lived SSE streams
        if path.rsplit("/", 1)[-1] in ("health", "healthcheck", "healthz"):
            return False
        return True

    @staticmethod
    def _identity(scope) -> str:
        headers = dict(scope.get("headers") or [])
        for h in (b"x-forwarded-email", b"x-auth-request-email"):
            val = headers.get(h)
            if val:
                return "user:" + val.decode("latin-1", "replace")
        client = scope.get("client")
        return f"ip:{client[0]}" if client else "ip:unknown"

    async def _send_429(self, send) -> None:
        body = json.dumps(
            {"detail": "Rate limit exceeded. Please slow down and try again shortly."}
        ).encode()
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", b"60"),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})

    async def __call__(self, scope, receive, send):
        if (
            not self._active
            or scope.get("type") != "http"
            or not self._should_limit(scope.get("path", ""))
        ):
            await self.app(scope, receive, send)
            return

        try:
            allowed = self._limiter.hit(self._item, self._identity(scope))
        except Exception as exc:  # never fail a request because the limiter errored
            logger.debug(f"[RATE_LIMIT] hit() error (allowing request): {exc}")
            allowed = True

        if not allowed:
            await self._send_429(send)
            return

        await self.app(scope, receive, send)
