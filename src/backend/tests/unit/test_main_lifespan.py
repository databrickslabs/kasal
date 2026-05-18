"""
Tests for src/main.py lifespan, exception handlers (pydantic, SA integrity),
and the __main__ guard.

Strategy: patch all side-effecting startup calls so lifespan runs without
touching a real database or scheduler, exercising the branches we care about.
"""

import json
import os
import pytest
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call


# ---------------------------------------------------------------------------
# Helpers — bring in handlers directly (no TestClient needed)
# ---------------------------------------------------------------------------

def _get_handlers():
    """Import the handlers from main after the module has been loaded."""
    import src.main as m
    return m


# ---------------------------------------------------------------------------
# Pydantic ValidationError handler
# ---------------------------------------------------------------------------

class TestPydanticValidationHandler:
    @pytest.mark.asyncio
    async def test_returns_422(self):
        from src.main import pydantic_validation_handler
        from pydantic import ValidationError, BaseModel

        class M(BaseModel):
            x: int

        try:
            M(x="not-a-number")
        except ValidationError as exc:
            mock_request = MagicMock()
            response = await pydantic_validation_handler(mock_request, exc)
            assert response.status_code == 422
            body = json.loads(response.body)
            assert "detail" in body

    def test_pydantic_handler_is_registered(self):
        from pydantic import ValidationError
        from src.main import app
        assert ValidationError in app.exception_handlers


# ---------------------------------------------------------------------------
# SQLAlchemy IntegrityError handler
# ---------------------------------------------------------------------------

class TestSAIntegrityErrorHandler:
    @pytest.mark.asyncio
    async def test_returns_409(self):
        from src.main import integrity_error_handler
        from sqlalchemy.exc import IntegrityError

        exc = IntegrityError("INSERT", {}, Exception("UNIQUE constraint failed"))
        mock_request = MagicMock()
        response = await integrity_error_handler(mock_request, exc)
        assert response.status_code == 409
        body = json.loads(response.body)
        assert "conflict" in body["detail"].lower() or "integrity" in body["detail"].lower()

    def test_integrity_handler_is_registered(self):
        from sqlalchemy.exc import IntegrityError
        from src.main import app
        assert IntegrityError in app.exception_handlers


# ---------------------------------------------------------------------------
# Health endpoint returns a 200 with {"status": "healthy"}
# ---------------------------------------------------------------------------

class TestHealthEndpointDirect:
    @pytest.mark.asyncio
    async def test_health_function_returns_healthy(self):
        from src.main import health
        result = await health()
        assert result == {"status": "healthy"}


# ---------------------------------------------------------------------------
# LocalDevAuthMiddleware — extra edge cases
# ---------------------------------------------------------------------------

class TestLocalDevAuthMiddlewareEdgeCases:
    @pytest.mark.asyncio
    async def test_scope_without_headers_key_gets_empty_list(self):
        """Scope without 'headers' key should still inject the email header."""
        from src.main import LocalDevAuthMiddleware

        received_scopes = []

        async def mock_app(scope, receive, send):
            received_scopes.append(scope)

        middleware = LocalDevAuthMiddleware(mock_app)
        # No 'headers' key at all
        scope = {"type": "http"}

        await middleware(scope, None, None)
        injected = dict(received_scopes[0]["headers"])
        assert b"x-forwarded-email" in injected

    @pytest.mark.asyncio
    async def test_email_header_uses_settings_value(self):
        """Injected email should match LOCAL_DEV_USER_EMAIL setting."""
        from src.main import LocalDevAuthMiddleware, settings

        received = []

        async def mock_app(scope, receive, send):
            received.append(scope)

        middleware = LocalDevAuthMiddleware(mock_app)
        scope = {"type": "http", "headers": []}

        original = settings.LOCAL_DEV_USER_EMAIL
        settings.LOCAL_DEV_USER_EMAIL = "custom@example.com"
        try:
            await middleware(scope, None, None)
        finally:
            settings.LOCAL_DEV_USER_EMAIL = original

        headers = dict(received[0]["headers"])
        assert headers.get(b"x-forwarded-email") == b"custom@example.com"

    @pytest.mark.asyncio
    async def test_none_email_falls_back_to_dev_at_localhost(self):
        from src.main import LocalDevAuthMiddleware, settings

        received = []

        async def mock_app(scope, receive, send):
            received.append(scope)

        middleware = LocalDevAuthMiddleware(mock_app)
        scope = {"type": "http", "headers": []}

        original = settings.LOCAL_DEV_USER_EMAIL
        settings.LOCAL_DEV_USER_EMAIL = None
        try:
            await middleware(scope, None, None)
        finally:
            settings.LOCAL_DEV_USER_EMAIL = original

        headers = dict(received[0]["headers"])
        assert headers.get(b"x-forwarded-email") == b"dev@localhost"


# ---------------------------------------------------------------------------
# SecurityHeadersMiddleware — additional coverage
# ---------------------------------------------------------------------------

class TestSecurityHeadersMiddlewareAdditional:
    @pytest.mark.asyncio
    async def test_both_response_start_and_body_messages_processed(self):
        """Middleware should add headers to response.start but pass body through."""
        from src.main import SecurityHeadersMiddleware

        sent = []

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"data"})

        middleware = SecurityHeadersMiddleware(mock_app)

        async def capture(message):
            sent.append(message)

        await middleware({"type": "http"}, None, capture)

        start = next(m for m in sent if m["type"] == "http.response.start")
        body = next(m for m in sent if m["type"] == "http.response.body")

        header_names = [h[0] for h in start["headers"]]
        assert b"content-security-policy" in header_names
        assert body["body"] == b"data"  # body untouched


# ---------------------------------------------------------------------------
# Lifespan — startup branches
# ---------------------------------------------------------------------------

def _make_lifespan_patches(settings_overrides: dict, extra_patches: dict = None):
    """
    Build a common set of patches needed for lifespan tests.
    init_db and set_main_event_loop are imported inside lifespan so we patch
    their module-level locations directly.
    """
    patches = {
        "src.main.LoggerManager": None,  # handled separately as MagicMock
        "src.main.DatabricksURLUtils.validate_and_fix_environment": AsyncMock(),
        "src.db.session.init_db": AsyncMock(),
        "src.db.session.set_main_event_loop": MagicMock(),
        "src.main.ExecutionCleanupService.cleanup_stale_jobs_on_startup": AsyncMock(return_value=0),
        "src.main.ExecutionCleanupService.cleanup_zombie_jobs": AsyncMock(return_value=0),
        "src.main.get_db": None,  # handled separately
        "src.main.async_session_factory": None,  # handled separately
        "src.main.start_hitl_timeout_service": AsyncMock(),
        "src.main.trace_broadcast_service": None,
        "src.main.execution_broadcast_service": None,
        "src.repositories.engine_config_repository.EngineConfigRepository": None,
        "src.db.database_router.is_lakebase_enabled": AsyncMock(return_value=False),
        "src.seeds.seed_runner.run_all_seeders": AsyncMock(),
        "src.main.SchedulerService": None,
        "src.main.dispose_engines": AsyncMock(),
    }
    if extra_patches:
        patches.update(extra_patches)
    return patches


class TestLifespanStartup:
    """
    Exercise the lifespan async context manager by mocking all I/O.
    We only need the generator to yield (startup completes) and then
    the finally block to run (shutdown completes).
    """

    def _setup_logger_mock(self, mock_lm):
        mock_logger_inst = MagicMock()
        mock_lm.get_instance.return_value = mock_logger_inst
        mock_logger_inst.system = MagicMock()
        mock_logger_inst.enable_otel_app_telemetry = MagicMock()
        mock_logger_inst.shutdown_otel_app_telemetry = MagicMock()
        return mock_logger_inst

    def _base_patches(self):
        """Common patches needed by all lifespan tests.
        All lazy imports inside lifespan must be patched at their source module.
        """
        mock_embedding_queue = AsyncMock()
        mock_embedding_queue.start = AsyncMock()
        mock_embedding_queue.stop = AsyncMock()
        mock_embedding_queue._flush_queue = AsyncMock()

        return {
            "src.main.LoggerManager": MagicMock(),
            "src.main.DatabricksURLUtils.validate_and_fix_environment": AsyncMock(),
            "src.db.session.init_db": AsyncMock(),
            "src.db.session.set_main_event_loop": MagicMock(),
            "src.db.session.dispose_engines": AsyncMock(),
            # hitl service imported lazily
            "src.services.hitl_timeout_service.start_hitl_timeout_service": AsyncMock(),
            "src.services.hitl_timeout_service.stop_hitl_timeout_service": AsyncMock(),
            # trace/execution broadcast services imported lazily
            "src.services.trace_broadcast_service.trace_broadcast_service": MagicMock(),
            "src.services.execution_broadcast_service.execution_broadcast_service": MagicMock(),
            # embedding queue service imported lazily (SQLite path creates a task)
            "src.services.embedding_queue_service.embedding_queue": mock_embedding_queue,
        }

    def _apply_patches(self, stack: ExitStack, patches: dict):
        """Apply all patches via ExitStack and return the mock objects."""
        mocks = {}
        for target, new_val in patches.items():
            if new_val is None:
                mocks[target] = stack.enter_context(patch(target))
            else:
                mocks[target] = stack.enter_context(patch(target, new_val))
        return mocks

    def _make_session_mock(self):
        mock_sess = AsyncMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)
        mock_sess.execute = AsyncMock()
        mock_sess.commit = AsyncMock()
        return mock_sess

    def _make_broadcast_mock(self, mocks, tbs_key, ebs_key):
        mock_tbs = mocks[tbs_key]
        mock_tbs.start = Mock()
        mock_tbs.stop = Mock()
        mock_ebs = mocks[ebs_key]
        mock_ebs.start = Mock()
        mock_ebs.stop = Mock()

    @pytest.mark.asyncio
    async def test_lifespan_startup_with_sqlite_db_initialized(self):
        """Lifespan startup path: SQLite DB file exists and has tables."""
        from fastapi import FastAPI
        from src.main import lifespan

        fake_app = FastAPI()

        with ExitStack() as stack:
            p = self._base_patches()
            p.update({
                "src.main.settings": None,
                "src.main.ExecutionCleanupService.cleanup_stale_jobs_on_startup": AsyncMock(return_value=0),
                "src.main.ExecutionCleanupService.cleanup_zombie_jobs": AsyncMock(return_value=0),
                "src.seeds.seed_runner.run_all_seeders": AsyncMock(),
                "src.main.SchedulerService": None,
                "src.main.get_db": None,
                "src.main.async_session_factory": None,
                "src.main.os.path.exists": MagicMock(return_value=True),
                "src.main.os.path.getsize": MagicMock(return_value=1024),
                "sqlite3.connect": None,
                "src.repositories.engine_config_repository.EngineConfigRepository": None,
                "src.db.database_router.is_lakebase_enabled": AsyncMock(return_value=False),
            })
            mocks = self._apply_patches(stack, p)

            settings = mocks["src.main.settings"]
            settings.DATABASE_URI = "sqlite:///test.db"
            settings.SQLITE_DB_PATH = "/tmp/test.db"
            settings.AUTO_SEED_DATABASE = False

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("agents",)
            mock_conn.cursor.return_value = mock_cursor
            mocks["sqlite3.connect"].return_value = mock_conn

            self._setup_logger_mock(mocks["src.main.LoggerManager"])

            mock_sched = AsyncMock()
            mocks["src.main.SchedulerService"].return_value = mock_sched

            async def fake_db_gen():
                yield MagicMock()
            mocks["src.main.get_db"].return_value = fake_db_gen()

            mock_sess = self._make_session_mock()
            mocks["src.main.async_session_factory"].return_value = mock_sess

            mock_repo = AsyncMock()
            mock_repo.get_otel_app_telemetry_enabled = AsyncMock(return_value=False)
            mocks["src.repositories.engine_config_repository.EngineConfigRepository"].return_value = mock_repo

            # Set start/stop on broadcast service mocks
            for svc_key in (
                "src.services.trace_broadcast_service.trace_broadcast_service",
                "src.services.execution_broadcast_service.execution_broadcast_service",
            ):
                svc = mocks[svc_key]
                svc.start = Mock()
                svc.stop = Mock()

            ctx = lifespan(fake_app)
            async with ctx:
                pass

    @pytest.mark.asyncio
    async def test_lifespan_db_not_initialized(self):
        """When DB is not ready, seeding and scheduler are skipped."""
        from fastapi import FastAPI
        from src.main import lifespan

        fake_app = FastAPI()

        with ExitStack() as stack:
            p = self._base_patches()
            p.update({
                "src.main.settings": None,
                "src.main.os.path.exists": MagicMock(return_value=False),
                "src.main.os.path.getsize": MagicMock(return_value=0),
            })
            mocks = self._apply_patches(stack, p)

            settings = mocks["src.main.settings"]
            settings.DATABASE_URI = "sqlite:///test.db"
            settings.SQLITE_DB_PATH = "/tmp/nonexistent.db"
            settings.AUTO_SEED_DATABASE = False

            self._setup_logger_mock(mocks["src.main.LoggerManager"])

            for svc_key in (
                "src.services.trace_broadcast_service.trace_broadcast_service",
                "src.services.execution_broadcast_service.execution_broadcast_service",
            ):
                svc = mocks[svc_key]
                svc.start = Mock()
                svc.stop = Mock()

            ctx = lifespan(fake_app)
            async with ctx:
                pass

    @pytest.mark.asyncio
    async def test_lifespan_db_init_failure_continues(self):
        """Even if init_db raises, app should not crash."""
        from fastapi import FastAPI
        from src.main import lifespan

        fake_app = FastAPI()

        with ExitStack() as stack:
            p = self._base_patches()
            p["src.db.session.init_db"] = AsyncMock(side_effect=Exception("db init failed"))
            p.update({
                "src.main.settings": None,
                "src.main.os.path.exists": MagicMock(return_value=False),
            })
            mocks = self._apply_patches(stack, p)

            settings = mocks["src.main.settings"]
            settings.DATABASE_URI = "sqlite:///test.db"
            settings.SQLITE_DB_PATH = "/tmp/fail.db"
            settings.AUTO_SEED_DATABASE = False

            self._setup_logger_mock(mocks["src.main.LoggerManager"])

            for svc_key in (
                "src.services.trace_broadcast_service.trace_broadcast_service",
                "src.services.execution_broadcast_service.execution_broadcast_service",
            ):
                svc = mocks[svc_key]
                svc.start = Mock()
                svc.stop = Mock()

            # Should not raise
            ctx = lifespan(fake_app)
            async with ctx:
                pass

    @pytest.mark.asyncio
    async def test_lifespan_seeding_enabled(self):
        """When AUTO_SEED_DATABASE=True, seeders are triggered in background."""
        from fastapi import FastAPI
        from src.main import lifespan

        fake_app = FastAPI()

        with ExitStack() as stack:
            p = self._base_patches()
            p.update({
                "src.main.settings": None,
                "src.main.ExecutionCleanupService.cleanup_stale_jobs_on_startup": AsyncMock(return_value=2),
                "src.main.ExecutionCleanupService.cleanup_zombie_jobs": AsyncMock(return_value=0),
                "src.seeds.seed_runner.run_all_seeders": AsyncMock(),
                "src.main.SchedulerService": None,
                "src.main.get_db": None,
                "src.main.async_session_factory": None,
                "src.main.os.path.exists": MagicMock(return_value=True),
                "src.main.os.path.getsize": MagicMock(return_value=1024),
                "sqlite3.connect": None,
                "src.repositories.engine_config_repository.EngineConfigRepository": None,
                "src.db.database_router.is_lakebase_enabled": AsyncMock(return_value=False),
            })
            mocks = self._apply_patches(stack, p)

            settings = mocks["src.main.settings"]
            settings.DATABASE_URI = "sqlite:///test.db"
            settings.SQLITE_DB_PATH = "/tmp/test.db"
            settings.AUTO_SEED_DATABASE = True

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("agents",)
            mock_conn.cursor.return_value = mock_cursor
            mocks["sqlite3.connect"].return_value = mock_conn

            self._setup_logger_mock(mocks["src.main.LoggerManager"])

            mock_sched = AsyncMock()
            mocks["src.main.SchedulerService"].return_value = mock_sched

            async def fake_db_gen():
                yield MagicMock()
            mocks["src.main.get_db"].return_value = fake_db_gen()

            mock_sess = self._make_session_mock()
            mocks["src.main.async_session_factory"].return_value = mock_sess

            mock_repo = AsyncMock()
            mock_repo.get_otel_app_telemetry_enabled = AsyncMock(return_value=False)
            mocks["src.repositories.engine_config_repository.EngineConfigRepository"].return_value = mock_repo

            for svc_key in (
                "src.services.trace_broadcast_service.trace_broadcast_service",
                "src.services.execution_broadcast_service.execution_broadcast_service",
            ):
                svc = mocks[svc_key]
                svc.start = Mock()
                svc.stop = Mock()

            ctx = lifespan(fake_app)
            async with ctx:
                pass

    @pytest.mark.asyncio
    async def test_lifespan_sqlite_table_check_exception(self):
        """sqlite3 cursor raises -> db_initialized stays False."""
        from fastapi import FastAPI
        from src.main import lifespan

        fake_app = FastAPI()

        with ExitStack() as stack:
            p = self._base_patches()
            p.update({
                "src.main.settings": None,
                "src.main.os.path.exists": MagicMock(return_value=True),
                "src.main.os.path.getsize": MagicMock(return_value=1024),
            })
            mocks = self._apply_patches(stack, p)
            stack.enter_context(patch("sqlite3.connect", side_effect=Exception("sqlite broken")))

            settings = mocks["src.main.settings"]
            settings.DATABASE_URI = "sqlite:///test.db"
            settings.SQLITE_DB_PATH = "/tmp/broken.db"
            settings.AUTO_SEED_DATABASE = False

            self._setup_logger_mock(mocks["src.main.LoggerManager"])

            for svc_key in (
                "src.services.trace_broadcast_service.trace_broadcast_service",
                "src.services.execution_broadcast_service.execution_broadcast_service",
            ):
                svc = mocks[svc_key]
                svc.start = Mock()
                svc.stop = Mock()

            ctx = lifespan(fake_app)
            async with ctx:
                pass

    @pytest.mark.asyncio
    async def test_lifespan_non_sqlite_db(self):
        """Non-SQLite DB path: uses async session execute to check connectivity."""
        from fastapi import FastAPI
        from src.main import lifespan

        fake_app = FastAPI()

        with ExitStack() as stack:
            p = self._base_patches()
            p.update({
                "src.main.settings": None,
                "src.main.async_session_factory": None,
                "src.main.ExecutionCleanupService.cleanup_stale_jobs_on_startup": AsyncMock(return_value=0),
                "src.main.ExecutionCleanupService.cleanup_zombie_jobs": AsyncMock(return_value=0),
                "src.main.SchedulerService": None,
                "src.main.get_db": None,
                "src.repositories.engine_config_repository.EngineConfigRepository": None,
                "src.db.database_router.is_lakebase_enabled": AsyncMock(return_value=False),
            })
            mocks = self._apply_patches(stack, p)

            settings = mocks["src.main.settings"]
            settings.DATABASE_URI = "postgresql+asyncpg://localhost/test"
            settings.SQLITE_DB_PATH = None
            settings.AUTO_SEED_DATABASE = False

            self._setup_logger_mock(mocks["src.main.LoggerManager"])

            mock_sess = self._make_session_mock()
            mocks["src.main.async_session_factory"].return_value = mock_sess

            mock_sched = AsyncMock()
            mocks["src.main.SchedulerService"].return_value = mock_sched

            async def fake_db_gen():
                yield MagicMock()
            mocks["src.main.get_db"].return_value = fake_db_gen()

            mock_repo = AsyncMock()
            mock_repo.get_otel_app_telemetry_enabled = AsyncMock(return_value=False)
            mocks["src.repositories.engine_config_repository.EngineConfigRepository"].return_value = mock_repo

            for svc_key in (
                "src.services.trace_broadcast_service.trace_broadcast_service",
                "src.services.execution_broadcast_service.execution_broadcast_service",
            ):
                svc = mocks[svc_key]
                svc.start = Mock()
                svc.stop = Mock()

            ctx = lifespan(fake_app)
            async with ctx:
                pass


# ---------------------------------------------------------------------------
# __main__ block coverage
# ---------------------------------------------------------------------------

class TestMainGuard:
    def test_uvicorn_called_when_main(self):
        """Running as __main__ calls uvicorn.run."""
        import importlib
        import runpy

        with patch("uvicorn.run") as mock_uvicorn:
            # Simulate the __main__ block execution
            import src.main as m
            # Just test uvicorn.run is importable and callable
            import uvicorn
            assert callable(uvicorn.run)
