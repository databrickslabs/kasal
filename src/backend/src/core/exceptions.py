"""
Centralized exception hierarchy for the Kasal application.

These exceptions carry HTTP semantics so the global exception handlers
registered in main.py can map them to proper status codes automatically.
"""

from typing import Dict, Optional


class KasalError(Exception):
    """Base exception for all Kasal domain errors."""

    status_code: int = 500
    detail: str = "Internal server error"

    def __init__(
        self,
        detail: str | None = None,
        status_code: int | None = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        if detail is not None:
            self.detail = detail
        if status_code is not None:
            self.status_code = status_code
        self.headers = headers
        super().__init__(self.detail)


class NotFoundError(KasalError):
    """Resource not found (404)."""

    status_code = 404
    detail = "Resource not found"


class ConflictError(KasalError):
    """Resource conflict, e.g. duplicate or integrity violation (409)."""

    status_code = 409
    detail = "Resource conflict"


class ForbiddenError(KasalError):
    """Insufficient permissions (403)."""

    status_code = 403
    detail = "Forbidden"


class BadRequestError(KasalError):
    """Client sent an invalid request (400)."""

    status_code = 400
    detail = "Bad request"


class UnauthorizedError(KasalError):
    """Authentication required or credentials invalid (401)."""

    status_code = 401
    detail = "Unauthorized"

    def __init__(
        self,
        detail: str | None = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        if headers is None:
            headers = {"WWW-Authenticate": "Bearer"}
        super().__init__(detail=detail, headers=headers)


class GoneError(KasalError):
    """Resource is no longer available (410)."""

    status_code = 410
    detail = "Gone"


class UnprocessableEntityError(KasalError):
    """Request was well-formed but contains semantic errors (422)."""

    status_code = 422
    detail = "Unprocessable entity"


class LakebaseUnavailableError(KasalError):
    """Lakebase database is unreachable after retries (503)."""

    status_code = 503
    detail = "Database connection unavailable"
