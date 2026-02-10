"""
Unit tests for src.core.exceptions module.

Tests the custom exception hierarchy: KasalError and all subclasses.
Covers default values, custom overrides, inheritance, and str() behavior.
"""

import pytest

from src.core.exceptions import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    KasalError,
    NotFoundError,
)


# ---------------------------------------------------------------------------
# KasalError (base)
# ---------------------------------------------------------------------------
class TestKasalError:
    def test_default_status_code(self):
        exc = KasalError()
        assert exc.status_code == 500

    def test_default_detail(self):
        exc = KasalError()
        assert exc.detail == "Internal server error"

    def test_custom_detail(self):
        exc = KasalError(detail="something broke")
        assert exc.detail == "something broke"
        assert exc.status_code == 500

    def test_custom_status_code(self):
        exc = KasalError(status_code=503)
        assert exc.status_code == 503
        assert exc.detail == "Internal server error"

    def test_custom_both(self):
        exc = KasalError(detail="overloaded", status_code=503)
        assert exc.detail == "overloaded"
        assert exc.status_code == 503

    def test_str_returns_detail(self):
        exc = KasalError(detail="boom")
        assert str(exc) == "boom"

    def test_is_exception(self):
        assert issubclass(KasalError, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(KasalError) as exc_info:
            raise KasalError(detail="test raise")
        assert exc_info.value.detail == "test raise"
        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# NotFoundError
# ---------------------------------------------------------------------------
class TestNotFoundError:
    def test_default_status_code(self):
        exc = NotFoundError()
        assert exc.status_code == 404

    def test_default_detail(self):
        exc = NotFoundError()
        assert exc.detail == "Resource not found"

    def test_custom_detail(self):
        exc = NotFoundError(detail="Agent not found")
        assert exc.detail == "Agent not found"
        assert exc.status_code == 404

    def test_inherits_kasal_error(self):
        assert issubclass(NotFoundError, KasalError)

    def test_caught_as_kasal_error(self):
        with pytest.raises(KasalError):
            raise NotFoundError()

    def test_str(self):
        exc = NotFoundError(detail="missing item")
        assert str(exc) == "missing item"


# ---------------------------------------------------------------------------
# ConflictError
# ---------------------------------------------------------------------------
class TestConflictError:
    def test_default_status_code(self):
        exc = ConflictError()
        assert exc.status_code == 409

    def test_default_detail(self):
        exc = ConflictError()
        assert exc.detail == "Resource conflict"

    def test_custom_detail(self):
        exc = ConflictError(detail="Duplicate entry")
        assert exc.detail == "Duplicate entry"
        assert exc.status_code == 409

    def test_inherits_kasal_error(self):
        assert issubclass(ConflictError, KasalError)

    def test_caught_as_kasal_error(self):
        with pytest.raises(KasalError):
            raise ConflictError()


# ---------------------------------------------------------------------------
# ForbiddenError
# ---------------------------------------------------------------------------
class TestForbiddenError:
    def test_default_status_code(self):
        exc = ForbiddenError()
        assert exc.status_code == 403

    def test_default_detail(self):
        exc = ForbiddenError()
        assert exc.detail == "Forbidden"

    def test_custom_detail(self):
        exc = ForbiddenError(detail="Admin only")
        assert exc.detail == "Admin only"
        assert exc.status_code == 403

    def test_inherits_kasal_error(self):
        assert issubclass(ForbiddenError, KasalError)

    def test_caught_as_kasal_error(self):
        with pytest.raises(KasalError):
            raise ForbiddenError()


# ---------------------------------------------------------------------------
# BadRequestError
# ---------------------------------------------------------------------------
class TestBadRequestError:
    def test_default_status_code(self):
        exc = BadRequestError()
        assert exc.status_code == 400

    def test_default_detail(self):
        exc = BadRequestError()
        assert exc.detail == "Bad request"

    def test_custom_detail(self):
        exc = BadRequestError(detail="Missing field 'name'")
        assert exc.detail == "Missing field 'name'"
        assert exc.status_code == 400

    def test_inherits_kasal_error(self):
        assert issubclass(BadRequestError, KasalError)

    def test_caught_as_kasal_error(self):
        with pytest.raises(KasalError):
            raise BadRequestError()


# ---------------------------------------------------------------------------
# Cross-cutting: hierarchy / polymorphism
# ---------------------------------------------------------------------------
class TestExceptionHierarchy:
    def test_all_subclasses_are_kasal_error(self):
        for cls in (NotFoundError, ConflictError, ForbiddenError, BadRequestError):
            assert issubclass(cls, KasalError)
            assert issubclass(cls, Exception)

    def test_subclass_status_codes_are_distinct(self):
        codes = {
            NotFoundError: 404,
            ConflictError: 409,
            ForbiddenError: 403,
            BadRequestError: 400,
            KasalError: 500,
        }
        for cls, expected_code in codes.items():
            assert cls().status_code == expected_code

    def test_custom_status_code_override_on_subclass(self):
        exc = NotFoundError(status_code=410)
        assert exc.status_code == 410
        assert exc.detail == "Resource not found"

    def test_none_detail_keeps_default(self):
        exc = NotFoundError(detail=None)
        # None passed explicitly should keep default
        assert exc.detail == "Resource not found"

    def test_none_status_code_keeps_default(self):
        exc = BadRequestError(status_code=None)
        assert exc.status_code == 400
