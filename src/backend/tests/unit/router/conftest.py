"""Shared fixtures and helpers for router tests.

Since commit 6472582, routers rely on global exception handlers registered
in main.py instead of inline try/except blocks. Router tests create
standalone FastAPI apps with just the router under test, so they need
these handlers registered explicitly.

Note on the Exception handler:
    FastAPI's build_middleware_stack routes ``Exception`` and ``500`` handlers
    to Starlette's ``ServerErrorMiddleware`` instead of ``ExceptionMiddleware``.
    With TestClient's default ``raise_server_exceptions=True``, the
    ServerErrorMiddleware re-raises exceptions rather than invoking the handler.
    To work around this, the generic Exception handler is installed as ASGI
    middleware that wraps the app, catching any unhandled exceptions and
    returning a proper 500 JSON response.
"""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse as StarletteJSONResponse

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from src.core.exceptions import KasalError


class _GenericExceptionMiddleware(BaseHTTPMiddleware):
    """Middleware that catches unhandled exceptions and returns 500 JSON.

    This replaces ``@app.exception_handler(Exception)`` which does not work
    reliably with TestClient because FastAPI routes it to ServerErrorMiddleware.
    """

    async def dispatch(self, request: StarletteRequest, call_next):
        try:
            response = await call_next(request)
            return response
        except Exception:
            return StarletteJSONResponse(
                status_code=500,
                content={"detail": "Internal server error"},
            )


def register_exception_handlers(app: FastAPI) -> None:
    """Register the global exception handlers that main.py normally provides.

    Router tests create standalone FastAPI apps with just the router under
    test. This helper ensures the test app behaves like the real app by
    registering the same exception handlers that main.py installs.

    KasalError and ValueError handlers are registered via FastAPI's
    exception_handler decorator (they work correctly with ExceptionMiddleware).
    The generic Exception catch-all is registered as ASGI middleware to
    bypass the ServerErrorMiddleware routing issue.
    """

    @app.exception_handler(KasalError)
    async def kasal_error_handler(
        request: Request, exc: KasalError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(
        request: Request, exc: ValueError
    ) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    app.add_middleware(_GenericExceptionMiddleware)
