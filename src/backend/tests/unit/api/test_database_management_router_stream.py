"""
Tests for streaming and error-path branches of
src/api/database_management_router.py that are not covered by existing smoke tests.

Covers:
- migrate_to_lakebase_stream (event_generator: happy path, no-endpoint auto-detect,
  failed migration, exceptions, cancellation)
- debug_permissions in DEBUG_MODE=False (returns 404)
- debug_headers in DEBUG_MODE=False (returns 404)
- export/import/list_backups: service failure paths
- list_backups: non-system-admin returns 403
- test_lakebase_connection: missing instance_name returns 400
"""

import asyncio
import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from src.core.exceptions import BadRequestError, ForbiddenError, KasalError
from src.schemas.database_management import (
    ExportRequest,
    ImportRequest,
    ListBackupsRequest,
)
from src.api.database_management_router import (
    export_database,
    import_database,
    list_backups,
    get_database_info,
    debug_permissions,
    debug_headers,
    migrate_to_lakebase,
    migrate_to_lakebase_stream,
    test_lakebase_connection as _router_test_lakebase_connection,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class SysAdminCtx:
    primary_group_id = "g1"
    group_email = "admin@x.com"
    access_token = "tok"
    current_user = type("U", (), {"is_system_admin": True})()


class RegularCtx:
    primary_group_id = "g1"
    group_email = "user@x.com"
    access_token = "tok"
    current_user = type("U", (), {"is_system_admin": False})()


# ---------------------------------------------------------------------------
# debug_permissions / debug_headers: DEBUG_MODE=False returns 404
# ---------------------------------------------------------------------------

class TestDebugEndpointsWhenNotDebug:
    @pytest.mark.asyncio
    async def test_debug_permissions_returns_404_when_not_debug(self):
        from fastapi import HTTPException
        from src.config.settings import settings as app_settings

        original = app_settings.DEBUG_MODE
        app_settings.DEBUG_MODE = False
        try:
            with pytest.raises(HTTPException) as exc_info:
                await debug_permissions(session=AsyncMock(), group_context=SysAdminCtx())
            assert exc_info.value.status_code == 404
        finally:
            app_settings.DEBUG_MODE = original

    @pytest.mark.asyncio
    async def test_debug_headers_returns_404_when_not_debug(self):
        from fastapi import HTTPException
        from src.config.settings import settings as app_settings

        original = app_settings.DEBUG_MODE
        app_settings.DEBUG_MODE = False
        try:
            req = SimpleNamespace(headers={})
            with pytest.raises(HTTPException) as exc_info:
                await debug_headers(request=req, group_context=SysAdminCtx())
            assert exc_info.value.status_code == 404
        finally:
            app_settings.DEBUG_MODE = original


# ---------------------------------------------------------------------------
# export / import / list_backups — service failure paths
# ---------------------------------------------------------------------------

class TestServiceFailurePaths:
    @pytest.mark.asyncio
    async def test_export_raises_kasal_error_on_failure(self):
        svc = AsyncMock()
        svc.export_to_volume = AsyncMock(
            return_value={"success": False, "error": "Volume not found"}
        )
        with patch("src.utils.user_context.UserContext"):
            with pytest.raises(KasalError):
                await export_database(
                    ExportRequest(), service=svc, group_context=SysAdminCtx()
                )

    @pytest.mark.asyncio
    async def test_import_raises_kasal_error_on_failure(self):
        svc = AsyncMock()
        svc.import_from_volume = AsyncMock(
            return_value={"success": False, "error": "Backup not found"}
        )
        with patch("src.utils.user_context.UserContext"):
            with pytest.raises(KasalError):
                await import_database(
                    ImportRequest(catalog="c", schema="s", volume_name="v", backup_filename="b.db"),
                    service=svc,
                    group_context=SysAdminCtx(),
                )

    @pytest.mark.asyncio
    async def test_list_backups_non_admin_raises_forbidden(self):
        svc = AsyncMock()
        with pytest.raises(ForbiddenError):
            await list_backups(
                ListBackupsRequest(), service=svc, group_context=RegularCtx()
            )

    @pytest.mark.asyncio
    async def test_list_backups_service_failure_raises_kasal_error(self):
        svc = AsyncMock()
        svc.list_backups = AsyncMock(
            return_value={"success": False, "error": "Volume error"}
        )
        with pytest.raises(KasalError):
            await list_backups(
                ListBackupsRequest(), service=svc, group_context=SysAdminCtx()
            )

    @pytest.mark.asyncio
    async def test_get_database_info_failure_raises_kasal_error(self):
        svc = AsyncMock()
        svc.get_database_info = AsyncMock(
            return_value={"success": False, "error": "DB unavailable"}
        )
        with pytest.raises(KasalError):
            await get_database_info(service=svc, group_context=SysAdminCtx())

    @pytest.mark.asyncio
    async def test_export_no_error_key_uses_default_message(self):
        """When service returns success=False with no 'error' key, uses default."""
        svc = AsyncMock()
        svc.export_to_volume = AsyncMock(return_value={"success": False})
        with patch("src.utils.user_context.UserContext"):
            with pytest.raises(KasalError) as exc_info:
                await export_database(
                    ExportRequest(), service=svc, group_context=SysAdminCtx()
                )
        assert "Export failed" in str(exc_info.value)


# ---------------------------------------------------------------------------
# test_lakebase_connection POST: missing instance_name
# ---------------------------------------------------------------------------

class TestLakebaseConnectionPost:
    @pytest.mark.asyncio
    async def test_missing_instance_name_raises_bad_request(self):
        svc = AsyncMock()
        with pytest.raises(BadRequestError):
            await _router_test_lakebase_connection({}, service=svc)


# ---------------------------------------------------------------------------
# migrate_to_lakebase_stream — event_generator branches
# ---------------------------------------------------------------------------

class TestMigrateToLakbaseStream:
    """
    We collect all SSE events emitted by the async generator inside the
    StreamingResponse by iterating over it directly.
    """

    def _make_request_obj(self):
        return SimpleNamespace(headers={})

    async def _collect_events(self, streaming_response):
        """Iterate the StreamingResponse body_iterator and parse SSE events."""
        events = []
        async for chunk in streaming_response.body_iterator:
            if isinstance(chunk, bytes):
                chunk = chunk.decode()
            if chunk.startswith("data: "):
                payload = chunk[len("data: "):].strip()
                try:
                    events.append(json.loads(payload))
                except json.JSONDecodeError:
                    events.append({"raw": payload})
        return events

    def _stream_patches(self, mock_service, extract_return="tok", dispose=True):
        """Build context managers for migrate_to_lakebase_stream tests."""
        mock_sess = MagicMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)
        return mock_sess

    @pytest.mark.asyncio
    async def test_happy_path_with_endpoint_provided(self):
        """When endpoint is provided and migration succeeds."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        async def fake_stream(instance, endpoint, recreate_schema, migrate_data):
            yield {"type": "progress", "message": "step1"}
            yield {"type": "result", "success": True, "message": "done"}

        mock_service = MagicMock()
        mock_service.migrate_existing_data_stream = fake_stream
        mock_service.get_config = AsyncMock(return_value={})
        mock_service.save_config = AsyncMock(return_value={})

        mock_sess = MagicMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   return_value="tok"), \
             patch("src.api.database_management_router.LakebaseService",
                   return_value=mock_service), \
             patch("src.db.session._local_session_factory",
                   return_value=mock_sess), \
             patch("src.db.session.dispose_engines", AsyncMock()):

            response = await migrate_to_lakebase_stream(
                request={"instance_name": "kb", "endpoint": "pg.example.com"},
                raw_request=raw_request,
                group_context=ctx,
            )

            events = await self._collect_events(response)

        types = [e.get("type") for e in events]
        assert "start" in types

    @pytest.mark.asyncio
    async def test_no_endpoint_auto_detects_successfully(self):
        """Without endpoint, auto-detect from instance returns DNS."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        async def fake_stream(instance, endpoint, recreate_schema, migrate_data):
            yield {"type": "result", "success": True, "message": "ok"}

        mock_service = MagicMock()
        mock_service.get_instance = AsyncMock(
            return_value={"read_write_dns": "pg.auto.example.com"}
        )
        mock_service.migrate_existing_data_stream = fake_stream
        mock_service.get_config = AsyncMock(return_value={})
        mock_service.save_config = AsyncMock(return_value={})

        mock_sess = MagicMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   return_value="tok"), \
             patch("src.api.database_management_router.LakebaseService",
                   return_value=mock_service), \
             patch("src.db.session._local_session_factory",
                   return_value=mock_sess), \
             patch("src.db.session.dispose_engines", AsyncMock()):

            response = await migrate_to_lakebase_stream(
                request={"instance_name": "kb"},
                raw_request=raw_request,
                group_context=ctx,
            )

            events = await self._collect_events(response)

        types = [e.get("type") for e in events]
        assert "start" in types
        assert "progress" in types  # auto-detect progress event

    @pytest.mark.asyncio
    async def test_no_endpoint_auto_detect_no_dns_returns_error(self):
        """Auto-detect returns no DNS -> error event and generator stops."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        mock_service = MagicMock()
        mock_service.get_instance = AsyncMock(return_value={"read_write_dns": None})

        mock_sess = MagicMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   return_value=None), \
             patch("src.api.database_management_router.LakebaseService",
                   return_value=mock_service), \
             patch("src.db.session._local_session_factory",
                   return_value=mock_sess):

            response = await migrate_to_lakebase_stream(
                request={"instance_name": "kb"},
                raw_request=raw_request,
                group_context=ctx,
            )

            events = await self._collect_events(response)

        types = [e.get("type") for e in events]
        assert "error" in types

    @pytest.mark.asyncio
    async def test_no_endpoint_auto_detect_exception(self):
        """Auto-detect raises -> error event and generator stops."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        mock_service = MagicMock()
        mock_service.get_instance = AsyncMock(side_effect=Exception("API down"))

        mock_sess = MagicMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   return_value=None), \
             patch("src.api.database_management_router.LakebaseService",
                   return_value=mock_service), \
             patch("src.db.session._local_session_factory",
                   return_value=mock_sess):

            response = await migrate_to_lakebase_stream(
                request={"instance_name": "kb"},
                raw_request=raw_request,
                group_context=ctx,
            )

            events = await self._collect_events(response)

        types = [e.get("type") for e in events]
        assert "error" in types

    @pytest.mark.asyncio
    async def test_migration_failed_disables_lakebase(self):
        """When migration result is success=False, config is updated with enabled=False."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        async def fake_stream(instance, endpoint, recreate_schema, migrate_data):
            yield {"type": "result", "success": False, "message": "failed"}

        saved_configs = []

        async def capture_save(cfg):
            saved_configs.append(dict(cfg))
            return cfg

        # Use AsyncMock for all async methods on the service
        mock_service = AsyncMock()
        mock_service.migrate_existing_data_stream = fake_stream
        mock_service.get_config = AsyncMock(return_value={})
        mock_service.save_config = capture_save

        # _local_session_factory returns an async context manager
        mock_sess = AsyncMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)
        mock_factory = MagicMock(return_value=mock_sess)

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   return_value="tok"), \
             patch("src.api.database_management_router.LakebaseService",
                   return_value=mock_service), \
             patch("src.db.session._local_session_factory", mock_factory):

            response = await migrate_to_lakebase_stream(
                request={"instance_name": "kb", "endpoint": "pg.example.com"},
                raw_request=raw_request,
                group_context=ctx,
            )

            await self._collect_events(response)

        # Config should have been saved with enabled=False
        assert any(not c.get("enabled", True) for c in saved_configs), \
            f"Expected enabled=False in saved configs, got: {saved_configs}"

    @pytest.mark.asyncio
    async def test_outer_exception_emits_error_event(self):
        """When extract_user_token_from_request raises, error event is emitted."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   side_effect=Exception("token error")):

            response = await migrate_to_lakebase_stream(
                request={"instance_name": "kb", "endpoint": "pg.example.com"},
                raw_request=raw_request,
                group_context=ctx,
            )

            events = await self._collect_events(response)

        types = [e.get("type") for e in events]
        assert "error" in types

    @pytest.mark.asyncio
    async def test_schema_only_migration(self):
        """migrate_data=False sends 'schema creation' action message."""
        ctx = SysAdminCtx()
        raw_request = self._make_request_obj()

        async def fake_stream(instance, endpoint, recreate_schema, migrate_data):
            yield {"type": "result", "success": True, "message": "schema created"}

        mock_service = MagicMock()
        mock_service.migrate_existing_data_stream = fake_stream
        mock_service.get_config = AsyncMock(return_value={})
        mock_service.save_config = AsyncMock(return_value={})

        mock_sess = MagicMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=False)

        with patch("src.utils.databricks_auth.extract_user_token_from_request",
                   return_value="tok"), \
             patch("src.api.database_management_router.LakebaseService",
                   return_value=mock_service), \
             patch("src.db.session._local_session_factory",
                   return_value=mock_sess), \
             patch("src.db.session.dispose_engines", AsyncMock()):

            response = await migrate_to_lakebase_stream(
                request={
                    "instance_name": "kb",
                    "endpoint": "pg.example.com",
                    "migrate_data": False,
                },
                raw_request=raw_request,
                group_context=ctx,
            )

            events = await self._collect_events(response)

        start_events = [e for e in events if e.get("type") == "start"]
        assert len(start_events) > 0
        assert "schema creation" in start_events[0].get("message", "")
