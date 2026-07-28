"""
Comprehensive unit tests for the src/api/memory_backend package.

Tests cover all endpoints not yet covered by existing smoke tests,
focusing on happy-path, permission checks, error branches, and edge cases.
"""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.memory_backend import configs_router, records_router
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    MemoryBackendConfig,
    MemoryBackendCreate,
    MemoryBackendType,
    MemoryBackendUpdate,
)

# ---------------------------------------------------------------------------
# Minimal group context helpers
# ---------------------------------------------------------------------------


class AdminCtx:
    user_role = "admin"
    primary_group_id = "g1"
    group_email = "admin@example.com"


class RegularCtx:
    user_role = "regular"
    primary_group_id = "g1"
    group_email = "user@example.com"


# ---------------------------------------------------------------------------
# is_workspace_admin helper used by the router
# ---------------------------------------------------------------------------


def _admin_ctx():
    return AdminCtx()


def _regular_ctx():
    return RegularCtx()


# Patch is_workspace_admin to control admin status in tests.
# Each router module binds the name at import time, so patching only the
# source module (src.core.permissions) would leave those bindings untouched —
# the patch has to land on every module that imports it.
_ADMIN_CHECK_SITES = (
    "src.api.memory_backend.configs_router.is_workspace_admin",
    "src.api.memory_backend.vectorsearch_router.is_workspace_admin",
    "src.api.memory_backend.lakebase_router.is_workspace_admin",
)


@contextlib.contextmanager
def _patch_admin(is_admin: bool):
    with contextlib.ExitStack() as stack:
        for target in _ADMIN_CHECK_SITES:
            stack.enter_context(patch(target, return_value=is_admin))
        yield


# ---------------------------------------------------------------------------
# test_lakebase_connection
# ---------------------------------------------------------------------------


class TestTestLakbaseConnection:
    @pytest.mark.asyncio
    async def test_success_with_instance_name(self):
        from src.api.memory_backend.lakebase_router import test_lakebase_connection

        svc = AsyncMock()
        svc.test_lakebase_connection = AsyncMock(return_value={"success": True})

        result = await test_lakebase_connection(
            group_context=_admin_ctx(),
            service=svc,
            request={"instance_name": "kasal-lakebase"},
        )
        assert result["success"] is True
        svc.test_lakebase_connection.assert_awaited_once_with(
            instance_name="kasal-lakebase"
        )

    @pytest.mark.asyncio
    async def test_success_without_request(self):
        from src.api.memory_backend.lakebase_router import test_lakebase_connection

        svc = AsyncMock()
        svc.test_lakebase_connection = AsyncMock(return_value={"success": True})

        result = await test_lakebase_connection(
            group_context=_admin_ctx(),
            service=svc,
            request=None,
        )
        assert result["success"] is True
        svc.test_lakebase_connection.assert_awaited_once_with(instance_name=None)

    @pytest.mark.asyncio
    async def test_exception_returns_failure(self):
        from src.api.memory_backend.lakebase_router import test_lakebase_connection

        svc = AsyncMock()
        svc.test_lakebase_connection = AsyncMock(
            side_effect=Exception("connection refused")
        )

        result = await test_lakebase_connection(
            group_context=_admin_ctx(),
            service=svc,
            request=None,
        )
        assert result["success"] is False
        assert "connection refused" in result["message"]


# ---------------------------------------------------------------------------
# initialize_lakebase_tables
# ---------------------------------------------------------------------------


class TestInitializeLakebaseTables:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        from src.api.memory_backend.lakebase_router import initialize_lakebase_tables

        svc = AsyncMock()

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await initialize_lakebase_tables(
                    request={},
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_calls_service_with_defaults(self):
        # Updated for app-modes: initialize_lakebase_tables now uses memory_table
        # instead of short_term_table/long_term_table/entity_table
        from src.api.memory_backend.lakebase_router import initialize_lakebase_tables

        svc = AsyncMock()
        svc.initialize_lakebase_tables = AsyncMock(return_value={"success": True})

        with _patch_admin(True):
            result = await initialize_lakebase_tables(
                request={},
                group_context=_admin_ctx(),
                service=svc,
            )
        assert result["success"] is True
        svc.initialize_lakebase_tables.assert_awaited_once_with(
            embedding_dimension=1024,
            memory_table="crew_memory",
            instance_name=None,
        )

    @pytest.mark.asyncio
    async def test_calls_service_with_custom_values(self):
        # Updated for app-modes: uses memory_table instead of per-type tables
        from src.api.memory_backend.lakebase_router import initialize_lakebase_tables

        svc = AsyncMock()
        svc.initialize_lakebase_tables = AsyncMock(return_value={"success": True})

        with _patch_admin(True):
            await initialize_lakebase_tables(
                request={
                    "instance_name": "my-lb",
                    "embedding_dimension": 768,
                    "memory_table": "custom_memory",
                },
                group_context=_admin_ctx(),
                service=svc,
            )
        svc.initialize_lakebase_tables.assert_awaited_once_with(
            embedding_dimension=768,
            memory_table="custom_memory",
            instance_name="my-lb",
        )


# ---------------------------------------------------------------------------
# get_lakebase_table_stats
# ---------------------------------------------------------------------------


class TestGetLakbaseTableStats:
    @pytest.mark.asyncio
    async def test_returns_stats(self):
        from src.api.memory_backend.lakebase_router import get_lakebase_table_stats

        svc = AsyncMock()
        svc.get_lakebase_table_stats = AsyncMock(return_value={"tables": {"st": 10}})

        result = await get_lakebase_table_stats(
            group_context=_admin_ctx(),
            service=svc,
            instance_name=None,
        )
        assert result["tables"]["st"] == 10
        svc.get_lakebase_table_stats.assert_awaited_once_with(
            instance_name=None, group_id="g1"
        )

    @pytest.mark.asyncio
    async def test_passes_instance_name(self):
        from src.api.memory_backend.lakebase_router import get_lakebase_table_stats

        svc = AsyncMock()
        svc.get_lakebase_table_stats = AsyncMock(return_value={})

        await get_lakebase_table_stats(
            group_context=_admin_ctx(),
            service=svc,
            instance_name="kasal-lb",
        )
        svc.get_lakebase_table_stats.assert_awaited_once_with(
            instance_name="kasal-lb", group_id="g1"
        )


# ---------------------------------------------------------------------------
# create_memory_config
# ---------------------------------------------------------------------------


class TestCreateMemoryConfig:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        from src.api.memory_backend.configs_router import create_memory_config

        svc = AsyncMock()
        config = MagicMock(spec=MemoryBackendCreate)

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await create_memory_config(
                    config=config,
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_creates_and_validates_backend(self):
        from src.api.memory_backend.configs_router import create_memory_config
        from src.models.memory_backend import MemoryBackend

        mock_backend = MagicMock(spec=MemoryBackend)
        svc = AsyncMock()
        svc.create_memory_backend = AsyncMock(return_value=mock_backend)

        config = MagicMock(spec=MemoryBackendCreate)

        with _patch_admin(True):
            with patch(
                "src.api.memory_backend.configs_router.MemoryBackendResponse"
            ) as mock_resp:
                mock_resp.model_validate.return_value = {"id": "1"}
                result = await create_memory_config(
                    config=config,
                    group_context=_admin_ctx(),
                    service=svc,
                )
        svc.create_memory_backend.assert_awaited_once_with("g1", config)


# ---------------------------------------------------------------------------
# get_memory_configs
# ---------------------------------------------------------------------------


class TestGetMemoryConfigs:
    @pytest.mark.asyncio
    async def test_returns_list(self):
        from src.api.memory_backend.configs_router import get_memory_configs

        mock_backends = [MagicMock(), MagicMock()]
        svc = AsyncMock()
        svc.get_memory_backends = AsyncMock(return_value=mock_backends)

        with patch(
            "src.api.memory_backend.configs_router.MemoryBackendResponse"
        ) as mock_resp:
            mock_resp.model_validate.side_effect = lambda b: b
            result = await get_memory_configs(
                request=MagicMock(),
                group_context=_admin_ctx(),
                service=svc,
            )
        assert len(result) == 2
        svc.get_memory_backends.assert_awaited_once_with("g1")


# ---------------------------------------------------------------------------
# get_default_memory_config
# ---------------------------------------------------------------------------


class TestGetDefaultMemoryConfig:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_default(self):
        from src.api.memory_backend.configs_router import get_default_memory_config

        svc = AsyncMock()
        svc.get_default_memory_backend = AsyncMock(return_value=None)

        result = await get_default_memory_config(
            group_context=_admin_ctx(),
            service=svc,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_config_when_found(self):
        from src.api.memory_backend.configs_router import get_default_memory_config

        mock_backend = MagicMock()
        svc = AsyncMock()
        svc.get_default_memory_backend = AsyncMock(return_value=mock_backend)

        with patch(
            "src.api.memory_backend.configs_router.MemoryBackendResponse"
        ) as mock_resp:
            mock_resp.model_validate.return_value = {"id": "default"}
            result = await get_default_memory_config(
                group_context=_admin_ctx(),
                service=svc,
            )
        assert result == {"id": "default"}


# ---------------------------------------------------------------------------
# get_memory_config_by_id
# ---------------------------------------------------------------------------


class TestGetMemoryConfigById:
    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self):
        from src.api.memory_backend.configs_router import get_memory_config_by_id

        svc = AsyncMock()
        svc.get_memory_backend = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await get_memory_config_by_id(
                backend_id="nonexistent",
                group_context=_admin_ctx(),
                service=svc,
            )

    @pytest.mark.asyncio
    async def test_returns_backend_when_found(self):
        from src.api.memory_backend.configs_router import get_memory_config_by_id

        mock_backend = MagicMock()
        svc = AsyncMock()
        svc.get_memory_backend = AsyncMock(return_value=mock_backend)

        with patch(
            "src.api.memory_backend.configs_router.MemoryBackendResponse"
        ) as mock_resp:
            mock_resp.model_validate.return_value = {"id": "b1"}
            result = await get_memory_config_by_id(
                backend_id="b1",
                group_context=_admin_ctx(),
                service=svc,
            )
        assert result == {"id": "b1"}


# ---------------------------------------------------------------------------
# update_memory_config
# ---------------------------------------------------------------------------


class TestUpdateMemoryConfig:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        from src.api.memory_backend.configs_router import update_memory_config

        svc = AsyncMock()

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await update_memory_config(
                    backend_id="b1",
                    update_data=MagicMock(),
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self):
        from src.api.memory_backend.configs_router import update_memory_config

        svc = AsyncMock()
        svc.update_memory_backend = AsyncMock(return_value=None)

        with _patch_admin(True):
            with pytest.raises(NotFoundError):
                await update_memory_config(
                    backend_id="missing",
                    update_data=MagicMock(),
                    group_context=_admin_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_returns_updated_backend(self):
        from src.api.memory_backend.configs_router import update_memory_config

        mock_backend = MagicMock()
        svc = AsyncMock()
        svc.update_memory_backend = AsyncMock(return_value=mock_backend)

        with _patch_admin(True):
            with patch(
                "src.api.memory_backend.configs_router.MemoryBackendResponse"
            ) as mock_resp:
                mock_resp.model_validate.return_value = {"id": "b1"}
                result = await update_memory_config(
                    backend_id="b1",
                    update_data=MagicMock(),
                    group_context=_admin_ctx(),
                    service=svc,
                )
        assert result == {"id": "b1"}


# ---------------------------------------------------------------------------
# delete_memory_config
# ---------------------------------------------------------------------------


class TestDeleteMemoryConfig:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        from src.api.memory_backend.configs_router import delete_memory_config

        svc = AsyncMock()

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await delete_memory_config(
                    backend_id="b1",
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self):
        from src.api.memory_backend.configs_router import delete_memory_config

        svc = AsyncMock()
        svc.delete_memory_backend = AsyncMock(return_value=False)

        with _patch_admin(True):
            with pytest.raises(NotFoundError):
                await delete_memory_config(
                    backend_id="missing",
                    group_context=_admin_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_returns_success_when_deleted(self):
        from src.api.memory_backend.configs_router import delete_memory_config

        svc = AsyncMock()
        svc.delete_memory_backend = AsyncMock(return_value=True)

        with _patch_admin(True):
            result = await delete_memory_config(
                backend_id="b1",
                group_context=_admin_ctx(),
                service=svc,
            )
        assert result["success"] is True


# ---------------------------------------------------------------------------
# get_memory_stats
# ---------------------------------------------------------------------------


class TestGetMemoryStats:
    @pytest.mark.asyncio
    async def test_returns_stats(self):
        from src.api.memory_backend.records_router import get_memory_stats

        svc = AsyncMock()
        svc.get_memory_stats = AsyncMock(return_value={"total": 42})

        result = await get_memory_stats(
            crew_id="crew123",
            group_context=_admin_ctx(),
            service=svc,
        )
        assert result["total"] == 42
        svc.get_memory_stats.assert_awaited_once_with("g1", "crew123")


# ---------------------------------------------------------------------------
# validate_memory_config — additional branch coverage
# ---------------------------------------------------------------------------


class TestValidateMemoryConfigAdditional:
    @pytest.mark.asyncio
    async def test_non_databricks_type_is_valid(self):
        """Non-Databricks backend types pass validation without databricks checks."""
        from src.api.memory_backend.configs_router import validate_memory_config
        from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

        cfg = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        svc = AsyncMock()
        ctx = AdminCtx()

        result = await validate_memory_config(
            config=cfg, service=svc, group_context=ctx
        )
        assert result["valid"] is True
        assert result["errors"] == []

    @pytest.mark.asyncio
    async def test_databricks_config_missing_raises_error(self):
        from src.api.memory_backend.configs_router import validate_memory_config

        cfg = MemoryBackendConfig(backend_type=MemoryBackendType.DATABRICKS)
        svc = AsyncMock()

        result = await validate_memory_config(
            config=cfg, service=svc, group_context=AdminCtx()
        )
        assert result["valid"] is False
        assert any(
            "Databricks configuration" in e or "required" in e for e in result["errors"]
        )

    @pytest.mark.asyncio
    async def test_databricks_missing_endpoint_errors(self):
        # Updated for app-modes: use memory_index instead of short_term_index
        from src.api.memory_backend.configs_router import validate_memory_config

        cfg = MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="",  # empty endpoint_name triggers validation error
                memory_index="catalog.schema.unified",
                embedding_dimension=1024,
            ),
        )
        svc = AsyncMock()
        result = await validate_memory_config(
            config=cfg, service=svc, group_context=AdminCtx()
        )
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# clear_crew_memory
# ---------------------------------------------------------------------------


class TestClearCrewMemory:
    @pytest.mark.asyncio
    async def test_raises_bad_request_when_no_memory_types(self):
        """clear_crew_memory raises BadRequestError when no memory_types provided."""
        with pytest.raises(BadRequestError):
            await records_router.clear_crew_memory(
                crew_id="crew1",
                request={},
                group_context=_admin_ctx(),
            )

    @pytest.mark.asyncio
    async def test_success_with_memory_types(self):
        result = await records_router.clear_crew_memory(
            crew_id="crew1",
            request={"memory_types": ["short_term", "long_term"]},
            group_context=_admin_ctx(),
        )
        assert result["success"] is True
        assert "crew1" in result["message"]


# ---------------------------------------------------------------------------
# save_lakebase_config
# ---------------------------------------------------------------------------


class TestSaveLakbaseConfig:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        from src.api.memory_backend.lakebase_router import save_lakebase_config

        svc = AsyncMock()

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await save_lakebase_config(
                    request={},
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_creates_new_config(self):
        from uuid import uuid4

        from src.api.memory_backend.lakebase_router import save_lakebase_config
        from src.models.memory_backend import MemoryBackend

        mock_backend = MagicMock(spec=MemoryBackend)
        mock_backend.id = uuid4()

        svc = AsyncMock()
        svc.get_memory_backends = AsyncMock(return_value=[])
        svc.create_memory_backend = AsyncMock(return_value=mock_backend)
        svc.set_default_backend = AsyncMock()

        with _patch_admin(True):
            result = await save_lakebase_config(
                request={"lakebase_config": {"instance_name": "lb1"}},
                group_context=_admin_ctx(),
                service=svc,
            )

        assert result["success"] is True


# ---------------------------------------------------------------------------
# save_default_config
# ---------------------------------------------------------------------------


class TestSaveDefaultConfig:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        from src.api.memory_backend.configs_router import save_default_config

        svc = AsyncMock()

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await save_default_config(
                    request={},
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_creates_active_default_config_with_cognitive_tuning(self):
        """Local save must persist an ACTIVE DEFAULT config carrying the
        cognitive tuning, so crew execution loads it (the whole fix)."""
        from uuid import uuid4

        from src.api.memory_backend.configs_router import save_default_config
        from src.models.memory_backend import MemoryBackend
        from src.schemas.memory_backend import MemoryBackendType

        mock_backend = MagicMock(spec=MemoryBackend)
        mock_backend.id = uuid4()

        svc = AsyncMock()
        svc.get_memory_backends = AsyncMock(return_value=[])
        svc.create_memory_backend = AsyncMock(return_value=mock_backend)
        svc.set_default_backend = AsyncMock()

        cognitive = {
            "memory_llm_model": "databricks-claude-haiku-4-5",
            "query_analysis_threshold": 99977,
            "exploration_budget": 0,
        }
        with _patch_admin(True):
            result = await save_default_config(
                request={"cognitive_config": cognitive},
                group_context=_admin_ctx(),
                service=svc,
            )

        assert result["success"] is True
        # The config handed to create_memory_backend is a DEFAULT backend that
        # carries the cognitive tuning (so get_active_config later loads it).
        created = svc.create_memory_backend.await_args.args[1]
        assert created.backend_type == MemoryBackendType.DEFAULT
        assert created.cognitive_config is not None
        assert (
            created.cognitive_config.memory_llm_model == "databricks-claude-haiku-4-5"
        )
        assert created.cognitive_config.query_analysis_threshold == 99977
        assert created.cognitive_config.exploration_budget == 0
        svc.set_default_backend.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_creates_config_without_cognitive_tuning(self):
        from uuid import uuid4

        from src.api.memory_backend.configs_router import save_default_config
        from src.models.memory_backend import MemoryBackend

        mock_backend = MagicMock(spec=MemoryBackend)
        mock_backend.id = uuid4()
        svc = AsyncMock()
        svc.get_memory_backends = AsyncMock(return_value=[])
        svc.create_memory_backend = AsyncMock(return_value=mock_backend)
        svc.set_default_backend = AsyncMock()

        with _patch_admin(True):
            result = await save_default_config(
                request={},
                group_context=_admin_ctx(),
                service=svc,
            )

        assert result["success"] is True
        created = svc.create_memory_backend.await_args.args[1]
        assert created.cognitive_config is None


# ---------------------------------------------------------------------------
# _browse_default_records / _delete_default_records (local LanceDB store)
# ---------------------------------------------------------------------------


class TestLocalDefaultStoreReadDelete:
    def test_browse_returns_empty_when_store_missing(self, tmp_path):
        from src.api.memory_backend.record_browsers import _browse_default_records

        with patch(
            "src.api.memory_backend.record_browsers.local_memory_store_dir",
            return_value=tmp_path / "nope",
        ):
            result = _browse_default_records(
                group_id="g", scope=None, limit=10, offset=0
            )
        # Returns (records, total) so the client can paginate.
        assert result == ([], 0)

    def test_browse_reads_the_group_store_and_passes_scope(self, tmp_path):
        """Reads the ONE deterministic group store and forwards the scope filter
        (workspace=/group, session=/group/session) to the storage layer.

        Also returns the full store count as ``total`` (via ``storage.count``)
        so the browser knows how many pages exist beyond the one returned.

        This test used to mock ``src.services.memory.engine.Memory`` — the very thing
        that had broken. The real call built a bare ``Memory()`` under
        ``CREWAI_STORAGE_DIR``, which since crewAI's removal defaults to an
        in-process dict, so the browser reported an EMPTY store (0 shown against
        13 on disk) while this stayed green. It now mocks the storage class the
        runtime actually writes through, and requires the memory.db to exist.
        """
        from src.api.memory_backend.record_browsers import _browse_default_records

        store = tmp_path / "kasal_default_g"
        store.mkdir()
        (store / "memory.db").touch()  # the real store file must exist
        storage = MagicMock()
        storage.list_records.return_value = ["rec"]
        storage.count.return_value = 412

        with (
            patch(
                "src.api.memory_backend.record_browsers.local_memory_store_dir",
                return_value=store,
            ),
            patch(
                "src.services.memory.local_storage_backend.LocalMemoryStorage",
                return_value=storage,
            ),
            patch(
                "src.api.memory_backend.record_browsers._memory_record_to_dict",
                return_value={"created_at": "2026-01-01", "metadata": {}},
            ),
        ):
            records, total = _browse_default_records(
                group_id="g", scope="/g/sess1", limit=10, offset=0
            )

        assert len(records) == 1
        # `total` reflects the full store count, not just the returned page.
        assert total == 412
        storage.list_records.assert_called_once()
        assert storage.list_records.call_args.kwargs.get("scope_prefix") == "/g/sess1"
        assert storage.count.call_args.kwargs.get("scope_prefix") == "/g/sess1"

    def test_browse_returns_nothing_when_the_db_file_is_absent(self, tmp_path):
        """A store directory left over from the LanceDB era has no memory.db."""
        from src.api.memory_backend.record_browsers import _browse_default_records

        store = tmp_path / "kasal_default_g"
        (store / "memory" / "memories.lance").mkdir(parents=True)  # legacy leftover
        with patch(
            "src.api.memory_backend.record_browsers.local_memory_store_dir",
            return_value=store,
        ):
            assert _browse_default_records(
                group_id="g", scope=None, limit=10, offset=0
            ) == ([], 0)

    def test_scoped_delete_uses_the_real_store(self, tmp_path):
        """The scoped delete had the same defect as browse: it reported 0 while
        removing nothing."""
        from src.api.memory_backend.record_browsers import _delete_default_records

        store = tmp_path / "kasal_default_g"
        store.mkdir()
        (store / "memory.db").touch()
        storage = MagicMock()
        storage.delete.return_value = 7

        with (
            patch(
                "src.api.memory_backend.record_browsers.local_memory_store_dir",
                return_value=store,
            ),
            patch(
                "src.services.memory.local_storage_backend.LocalMemoryStorage",
                return_value=storage,
            ),
        ):
            deleted = _delete_default_records(group_id="g", scope="/g/sess1")

        assert deleted == 7
        assert storage.delete.call_args.kwargs.get("scope_prefix") == "/g/sess1"

    def test_delete_returns_zero_when_store_missing(self, tmp_path):
        from src.api.memory_backend.record_browsers import _delete_default_records

        with patch(
            "src.api.memory_backend.record_browsers.local_memory_store_dir",
            return_value=tmp_path / "nope",
        ):
            assert _delete_default_records(group_id="g", scope=None) == 0


# ---------------------------------------------------------------------------
# switch_to_disabled_mode
# ---------------------------------------------------------------------------


class TestSwitchToDisabledMode:
    @pytest.mark.asyncio
    async def test_raises_403_for_non_admin(self):
        svc = AsyncMock()

        with _patch_admin(False):
            with pytest.raises(ForbiddenError):
                await configs_router.switch_to_disabled_mode(
                    group_context=_regular_ctx(),
                    service=svc,
                )

    @pytest.mark.asyncio
    async def test_success(self):
        svc = AsyncMock()
        svc.delete_all_and_create_disabled = AsyncMock(
            return_value={"success": True, "message": "Done"}
        )

        with _patch_admin(True):
            result = await configs_router.switch_to_disabled_mode(
                group_context=_admin_ctx(),
                service=svc,
            )
        assert result["success"] is True
