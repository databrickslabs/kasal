"""
Unit tests for global exception handlers registered in src.main.

Uses a minimal FastAPI app with the same exception handlers to verify
that each handler maps exceptions to the correct HTTP status code and
response body without leaking internal details.
"""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.requests import Request

from src.core.exceptions import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    KasalError,
    NotFoundError,
)


# ---------------------------------------------------------------------------
# Build a minimal test app that mirrors the production exception handlers
# ---------------------------------------------------------------------------

def _build_test_app() -> FastAPI:
    """Create a small FastAPI app with the same global exception handlers as main.py."""
    app = FastAPI()

    # -- KasalError handler --------------------------------------------------
    @app.exception_handler(KasalError)
    async def kasal_error_handler(request: Request, exc: KasalError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    # -- ValueError handler --------------------------------------------------
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    # -- pydantic ValidationError handler ------------------------------------
    try:
        from pydantic import ValidationError as PydanticValidationError

        @app.exception_handler(PydanticValidationError)
        async def pydantic_validation_handler(
            request: Request, exc: PydanticValidationError
        ) -> JSONResponse:
            return JSONResponse(status_code=422, content={"detail": str(exc)})
    except ImportError:
        pass

    # -- SQLAlchemy IntegrityError handler -----------------------------------
    try:
        from sqlalchemy.exc import IntegrityError as SAIntegrityError

        @app.exception_handler(SAIntegrityError)
        async def integrity_error_handler(
            request: Request, exc: SAIntegrityError
        ) -> JSONResponse:
            return JSONResponse(
                status_code=409, content={"detail": "Database integrity conflict"}
            )
    except ImportError:
        pass

    # -- Generic catch-all handler -------------------------------------------
    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500, content={"detail": "Internal server error"}
        )

    # -- Test endpoints that raise various exceptions ------------------------

    @app.get("/raise-kasal-error")
    async def raise_kasal_error():
        raise KasalError(detail="base kasal error")

    @app.get("/raise-not-found")
    async def raise_not_found():
        raise NotFoundError(detail="Agent not found")

    @app.get("/raise-conflict")
    async def raise_conflict():
        raise ConflictError(detail="Duplicate name")

    @app.get("/raise-forbidden")
    async def raise_forbidden():
        raise ForbiddenError(detail="Admin only")

    @app.get("/raise-bad-request")
    async def raise_bad_request():
        raise BadRequestError(detail="Missing field")

    @app.get("/raise-value-error")
    async def raise_value_error():
        raise ValueError("invalid input")

    @app.get("/raise-integrity-error")
    async def raise_integrity_error():
        from sqlalchemy.exc import IntegrityError
        raise IntegrityError("INSERT ...", {}, Exception("UNIQUE constraint"))

    @app.get("/raise-generic-exception")
    async def raise_generic_exception():
        raise RuntimeError("internal secret details should not leak")

    @app.get("/raise-http-exception")
    async def raise_http_exception():
        raise HTTPException(status_code=418, detail="I'm a teapot")

    @app.get("/healthy")
    async def healthy():
        return {"ok": True}

    return app


@pytest.fixture(scope="module")
def client():
    """TestClient for the test app."""
    app = _build_test_app()
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Tests: KasalError hierarchy
# ---------------------------------------------------------------------------

class TestKasalErrorHandler:
    def test_base_kasal_error(self, client):
        r = client.get("/raise-kasal-error")
        assert r.status_code == 500
        assert r.json() == {"detail": "base kasal error"}

    def test_not_found_error(self, client):
        r = client.get("/raise-not-found")
        assert r.status_code == 404
        assert r.json() == {"detail": "Agent not found"}

    def test_conflict_error(self, client):
        r = client.get("/raise-conflict")
        assert r.status_code == 409
        assert r.json() == {"detail": "Duplicate name"}

    def test_forbidden_error(self, client):
        r = client.get("/raise-forbidden")
        assert r.status_code == 403
        assert r.json() == {"detail": "Admin only"}

    def test_bad_request_error(self, client):
        r = client.get("/raise-bad-request")
        assert r.status_code == 400
        assert r.json() == {"detail": "Missing field"}


# ---------------------------------------------------------------------------
# Tests: ValueError -> 400
# ---------------------------------------------------------------------------

class TestValueErrorHandler:
    def test_value_error_returns_400(self, client):
        r = client.get("/raise-value-error")
        assert r.status_code == 400
        assert r.json() == {"detail": "invalid input"}


# ---------------------------------------------------------------------------
# Tests: IntegrityError -> 409
# ---------------------------------------------------------------------------

class TestIntegrityErrorHandler:
    def test_integrity_error_returns_409(self, client):
        r = client.get("/raise-integrity-error")
        assert r.status_code == 409
        assert r.json() == {"detail": "Database integrity conflict"}

    def test_integrity_error_hides_internals(self, client):
        r = client.get("/raise-integrity-error")
        body = r.json()
        assert "UNIQUE constraint" not in body["detail"]
        assert "INSERT" not in body["detail"]


# ---------------------------------------------------------------------------
# Tests: Generic Exception -> 500 (no leak)
# ---------------------------------------------------------------------------

class TestGenericExceptionHandler:
    def test_generic_exception_returns_500(self, client):
        r = client.get("/raise-generic-exception")
        assert r.status_code == 500
        assert r.json() == {"detail": "Internal server error"}

    def test_generic_exception_does_not_leak_message(self, client):
        r = client.get("/raise-generic-exception")
        body = r.json()
        assert "internal secret details" not in body["detail"]
        assert "RuntimeError" not in body["detail"]


# ---------------------------------------------------------------------------
# Tests: HTTPException passthrough (FastAPI native)
# ---------------------------------------------------------------------------

class TestHTTPExceptionPassthrough:
    def test_http_exception_passthrough(self, client):
        r = client.get("/raise-http-exception")
        assert r.status_code == 418
        assert r.json() == {"detail": "I'm a teapot"}


# ---------------------------------------------------------------------------
# Tests: Happy path still works
# ---------------------------------------------------------------------------

class TestHappyPath:
    def test_normal_endpoint(self, client):
        r = client.get("/healthy")
        assert r.status_code == 200
        assert r.json() == {"ok": True}
