"""
Unit tests for main application module.

Tests app creation, middleware setup, route registration, and
environment configuration.
"""
import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import FastAPI, APIRouter
from fastapi.testclient import TestClient


class TestAppCreation:
    """Test cases for FastAPI app object and configuration."""

    def test_app_is_fastapi_instance(self):
        """Test that app is a FastAPI instance."""
        from src.main import app
        assert isinstance(app, FastAPI)

    def test_app_title(self):
        """Test that app title matches settings."""
        from src.main import app
        assert app.title is not None
        assert len(app.title) > 0

    def test_app_version(self):
        """Test that app version is set."""
        from src.main import app
        assert app.version is not None

    def test_app_has_lifespan(self):
        """Test that app has a lifespan handler configured."""
        from src.main import app
        assert app.router.lifespan_context is not None


class TestEnvironmentSetup:
    """Test cases for environment variable configuration."""

    def test_seed_debug_is_set(self):
        """Test that SEED_DEBUG environment variable is set."""
        import src.main  # noqa: F401 - trigger module-level code
        assert os.environ.get("SEED_DEBUG") == "True"

    def test_crewai_telemetry_disabled(self):
        """Test that CrewAI telemetry is disabled."""
        import src.main  # noqa: F401
        assert os.environ.get("CREWAI_DISABLE_TELEMETRY") == "true"

    def test_mlflow_tracking_uri_set(self):
        """Test that MLFLOW_TRACKING_URI is set to databricks."""
        import src.main  # noqa: F401
        assert os.environ.get("MLFLOW_TRACKING_URI") == "databricks"

    def test_log_dir_is_set(self):
        """Test that LOG_DIR environment variable is set."""
        import src.main  # noqa: F401
        log_dir = os.environ.get("LOG_DIR")
        assert log_dir is not None
        assert log_dir.endswith("logs")


class TestModuleLevelAttributes:
    """Test module-level variables and imports."""

    def test_logger_exists(self):
        """Test that the module-level logger is defined."""
        from src.main import logger
        assert logger is not None
        assert logger.name == "src.main"

    def test_log_path_exists(self):
        """Test that log_path is defined and ends with 'logs'."""
        from src.main import log_path
        assert log_path is not None
        assert isinstance(log_path, str)
        assert log_path.endswith("logs")

    def test_api_router_is_imported(self):
        """Test that api_router is imported."""
        from src.main import api_router
        assert isinstance(api_router, APIRouter)

    def test_settings_imported(self):
        """Test that settings are imported."""
        from src.main import settings
        assert hasattr(settings, "DATABASE_URI")
        assert hasattr(settings, "API_V1_STR")


class TestRouteRegistration:
    """Test that routes are registered on the app."""

    def test_health_endpoint_registered(self):
        """Test that /health endpoint is registered."""
        from src.main import app
        routes = [r.path for r in app.routes]
        assert "/health" in routes

    def test_api_v1_routes_registered(self):
        """Test that API v1 routes are registered."""
        from src.main import app
        routes = [r.path for r in app.routes]
        # At least some /api/v1 prefixed routes should exist
        api_routes = [r for r in routes if r.startswith("/api/v1")]
        assert len(api_routes) > 0

    def test_health_endpoint_returns_healthy(self):
        """Test the /health endpoint response."""
        from src.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestMiddlewareSetup:
    """Test that middleware is configured."""

    def test_cors_middleware_added(self):
        """Test that CORS middleware is configured on the app."""
        from src.main import app
        middleware_classes = [
            m.cls.__name__ if hasattr(m, "cls") else str(m)
            for m in app.user_middleware
        ]
        # CORS middleware should be present
        assert any("CORS" in name or "BaseHTTP" in name for name in middleware_classes)


class TestExceptionHandlers:
    """Test that global exception handlers are registered."""

    def test_kasal_error_handler_registered(self):
        """Test that KasalError handler is registered."""
        from src.main import app
        from src.core.exceptions import KasalError
        assert KasalError in app.exception_handlers

    def test_value_error_handler_registered(self):
        """Test that ValueError handler is registered."""
        from src.main import app
        assert ValueError in app.exception_handlers

    def test_generic_exception_handler_registered(self):
        """Test that the generic Exception handler is registered."""
        from src.main import app
        assert Exception in app.exception_handlers


class TestLifespanContextManager:
    """Test lifespan context manager protocol."""

    def test_lifespan_is_async_context_manager(self):
        """Test that lifespan follows async context manager protocol."""
        from src.main import lifespan

        test_app = FastAPI()
        ctx = lifespan(test_app)
        assert hasattr(ctx, "__aenter__")
        assert hasattr(ctx, "__aexit__")
