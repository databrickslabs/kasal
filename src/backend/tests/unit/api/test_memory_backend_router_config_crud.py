"""
Coverage tests for the src/api/memory_backend package.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.memory_backend import (
    configs_router,
    dependencies,
    lakebase_router,
    records_router,
)
from src.core.exceptions import ForbiddenError, NotFoundError


class AdminCtx:
    def __init__(self, is_admin=True, is_system_admin=False):
        self.user_role = "admin" if is_admin else "viewer"
        self.current_user = SimpleNamespace(
            is_system_admin=is_system_admin,
            is_personal_workspace_manager=is_admin,
        )
        self.primary_group_id = "user_alice_example_com"
        self.group_ids = ["user_alice_example_com"]
        self.group_email = "alice@example.com"
        self.access_token = "tok"


# ─── get_memory_backend_service ───────────────────────────────────────────────


def test_get_memory_backend_service():
    fake_session = MagicMock()
    with patch("src.api.memory_backend.dependencies.MemoryBackendService") as MockSvc:
        MockSvc.return_value = MagicMock()
        dependencies.get_memory_backend_service(session=fake_session)
        MockSvc.assert_called_once_with(fake_session)


# ─── test_lakebase_connection ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lakebase_connection_success():
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(return_value={"success": True})
    ctx = AdminCtx()
    result = await lakebase_router.test_lakebase_connection(
        group_context=ctx, service=svc, request=None
    )
    assert result["success"] is True


@pytest.mark.asyncio
async def test_lakebase_connection_exception():
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(side_effect=Exception("conn failed"))
    ctx = AdminCtx()
    result = await lakebase_router.test_lakebase_connection(
        group_context=ctx, service=svc, request=None
    )
    assert result["success"] is False


# ─── get_memory_configs ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_memory_configs():
    svc = AsyncMock()
    svc.get_all = AsyncMock(return_value=[])
    ctx = AdminCtx()
    result = await configs_router.get_memory_configs(
        service=svc, group_context=ctx, request=None
    )
    assert isinstance(result, list)


# ─── get_memory_config_by_id ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_memory_config_by_id_not_found():
    svc = AsyncMock()
    svc.get_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx()
    with pytest.raises(NotFoundError):
        await configs_router.get_memory_config_by_id(
            backend_id="999", service=svc, group_context=ctx
        )


# ─── create_memory_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_memory_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.create_memory_config(
            config=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_create_memory_config_success():
    svc = AsyncMock()
    created = MagicMock()
    svc.create_memory_backend = AsyncMock(return_value=created)
    ctx = AdminCtx(is_admin=True)
    mock_response = MagicMock()
    with patch(
        "src.api.memory_backend.configs_router.MemoryBackendResponse"
    ) as mock_resp_cls:
        mock_resp_cls.model_validate.return_value = mock_response
        result = await configs_router.create_memory_config(
            config=MagicMock(), service=svc, group_context=ctx
        )
    assert result is mock_response


# ─── update_memory_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_memory_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.update_memory_config(
            backend_id=1, update_data=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_update_memory_config_not_found():
    svc = AsyncMock()
    svc.update_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await configs_router.update_memory_config(
            backend_id="999", update_data=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_update_memory_config_success():
    svc = AsyncMock()
    updated = MagicMock()
    svc.update_memory_backend = AsyncMock(return_value=updated)
    ctx = AdminCtx(is_admin=True)
    mock_response = MagicMock()
    with patch(
        "src.api.memory_backend.configs_router.MemoryBackendResponse"
    ) as mock_resp_cls:
        mock_resp_cls.model_validate.return_value = mock_response
        result = await configs_router.update_memory_config(
            backend_id="1", update_data=MagicMock(), service=svc, group_context=ctx
        )
    assert result is mock_response


# ─── delete_memory_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_memory_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.delete_memory_config(
            backend_id="1", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_delete_memory_config_not_found():
    svc = AsyncMock()
    svc.delete_memory_backend = AsyncMock(return_value=False)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await configs_router.delete_memory_config(
            backend_id="999", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_delete_memory_config_success():
    svc = AsyncMock()
    svc.delete_memory_backend = AsyncMock(return_value=True)
    ctx = AdminCtx(is_admin=True)
    result = await configs_router.delete_memory_config(
        backend_id="1", service=svc, group_context=ctx
    )
    assert result["success"] is True


# ─── set_default_memory_config ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_default_memory_config_not_found():
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=False)
    ctx = AdminCtx(is_admin=True)
    with pytest.raises(NotFoundError):
        await configs_router.set_default_memory_config(
            backend_id="999", service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_set_default_memory_config_success():
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=True)
    ctx = AdminCtx(is_admin=True)
    result = await configs_router.set_default_memory_config(
        backend_id="1", service=svc, group_context=ctx
    )
    assert result["success"] is True


# ─── get_default_memory_config ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_default_memory_config_none():
    svc = AsyncMock()
    svc.get_default_memory_backend = AsyncMock(return_value=None)
    ctx = AdminCtx()
    result = await configs_router.get_default_memory_config(
        service=svc, group_context=ctx
    )
    assert result is None


# ─── get_memory_stats ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_memory_stats():
    svc = AsyncMock()
    svc.get_memory_stats = AsyncMock(return_value={"total": 100})
    ctx = AdminCtx()
    result = await records_router.get_memory_stats(
        crew_id=None, service=svc, group_context=ctx
    )
    assert result["total"] == 100


# ─── validate_memory_config ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_validate_memory_config_databricks_valid():
    svc = AsyncMock()
    ctx = AdminCtx()
    from src.schemas.memory_backend import (
        DatabricksMemoryConfig,
        MemoryBackendConfig,
        MemoryBackendType,
    )

    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config=DatabricksMemoryConfig(
            memory_index="catalog.schema.memory_index",
            workspace_url="https://example.com",
            endpoint_name="my-endpoint",
            short_term_index="st_idx",
            long_term_index="lt_idx",
            entity_index="ent_idx",
        ),
    )
    result = await configs_router.validate_memory_config(
        config=config, service=svc, group_context=ctx
    )
    assert result["valid"] is True


@pytest.mark.asyncio
async def test_validate_memory_config_databricks_no_config():
    svc = AsyncMock()
    ctx = AdminCtx()
    from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

    config = MemoryBackendConfig(
        backend_type=MemoryBackendType.DATABRICKS, databricks_config=None
    )
    result = await configs_router.validate_memory_config(
        config=config, service=svc, group_context=ctx
    )
    assert result["valid"] is False


# ─── initialize_lakebase_tables ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_initialize_lakebase_tables_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await lakebase_router.initialize_lakebase_tables(
            request={}, service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_initialize_lakebase_tables_success():
    svc = AsyncMock()
    svc.initialize_lakebase_tables = AsyncMock(return_value={"success": True})
    ctx = AdminCtx(is_admin=True)
    result = await lakebase_router.initialize_lakebase_tables(
        request={"instance_name": "my-lakebase", "embedding_dimension": 768},
        service=svc,
        group_context=ctx,
    )
    assert result["success"] is True


# ─── get_lakebase_table_stats ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_lakebase_table_stats():
    svc = AsyncMock()
    svc.get_lakebase_table_stats = AsyncMock(return_value={"tables": []})
    ctx = AdminCtx()
    result = await lakebase_router.get_lakebase_table_stats(
        service=svc, group_context=ctx
    )
    assert "tables" in result


# ─── get_lakebase_table_data ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_lakebase_table_data():
    svc = AsyncMock()
    svc.get_lakebase_table_data = AsyncMock(return_value={"documents": []})
    ctx = AdminCtx()
    result = await lakebase_router.get_lakebase_table_data(
        service=svc, group_context=ctx, table_name="crew_short_term_memory", limit=50
    )
    assert "documents" in result


# ─── get_lakebase_entity_data ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_lakebase_entity_data():
    svc = AsyncMock()
    svc.get_lakebase_entity_data = AsyncMock(return_value={"entities": []})
    ctx = AdminCtx()
    result = await lakebase_router.get_lakebase_entity_data(
        service=svc, group_context=ctx
    )
    assert "entities" in result


# ─── save_lakebase_config ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_lakebase_config_forbidden():
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await lakebase_router.save_lakebase_config(
            request=MagicMock(), service=svc, group_context=ctx
        )


@pytest.mark.asyncio
async def test_save_lakebase_config_success():
    svc = AsyncMock()
    svc.save_lakebase_config = AsyncMock(return_value={"saved": True})
    ctx = AdminCtx(is_admin=True)
    result = await lakebase_router.save_lakebase_config(
        request=MagicMock(), service=svc, group_context=ctx
    )
    assert result is not None


# ─── R2-05: every configuration mutation is a workspace-admin action ──────────
@pytest.mark.asyncio
async def test_set_default_bulk_delete_and_cleanup_refuse_an_operator():
    from src.core.exceptions import ForbiddenError

    svc = AsyncMock()
    operator = AdminCtx(is_admin=False)
    with pytest.raises(ForbiddenError):
        await configs_router.set_default_memory_config(
            backend_id="1", service=svc, group_context=operator
        )
    with pytest.raises(ForbiddenError):
        await configs_router.delete_all_databricks_configs(
            service=svc, group_context=operator
        )
    with pytest.raises(ForbiddenError):
        await configs_router.cleanup_disabled_configs(
            service=svc, group_context=operator
        )
    svc.set_default_backend.assert_not_awaited()
    svc.delete_memory_backend.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend_type", ["default", "lakebase"])
@pytest.mark.parametrize(
    "failure", [None, "cleanup", "default", "commit", "closed_commit"]
)
async def test_memory_replacement_response_matches_committed_state(
    backend_type, failure
):
    """Real router, service and request transaction: success implies a saved default."""
    from contextlib import nullcontext

    import httpx
    from fastapi import FastAPI
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from src.dependencies.providers import get_group_context
    from src.db import database_router as db
    from src.main import app as production_app
    from src.models.memory_backend import MemoryBackend
    from src.repositories.memory_backend_repository import MemoryBackendRepository
    from src.utils.user_context import GroupContext

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(MemoryBackend.__table__.create)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as seed:
            seed.add(
                MemoryBackend(id="old", name="Old", group_id="group", is_default=True)
            )
            await seed.commit()

        app = FastAPI(exception_handlers=dict(production_app.exception_handlers))
        app.include_router(configs_router.router)
        app.include_router(lakebase_router.router)
        app.dependency_overrides[get_group_context] = lambda: GroupContext(
            group_ids=["group"], group_email="audit@example.com", user_role="admin"
        )
        session = factory()
        fault = nullcontext()
        if failure == "cleanup":
            fault = patch.object(
                MemoryBackendRepository,
                "delete",
                AsyncMock(side_effect=RuntimeError("SENTINEL")),
            )
        elif failure == "default":
            fault = patch.object(
                MemoryBackendRepository, "set_default", AsyncMock(return_value=False)
            )
        elif failure in {"commit", "closed_commit"}:
            error = (
                IntegrityError("INSERT SENTINEL", {}, Exception("SENTINEL"))
                if failure == "commit"
                else RuntimeError("connection is closed")
            )
            fault = patch.object(session, "commit", AsyncMock(side_effect=error))
        with (
            patch.object(db, "is_lakebase_enabled", AsyncMock(return_value=False)),
            patch.object(db, "async_session_factory", return_value=session),
            fault,
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://example.com",
            ) as client:
                body = (
                    {"lakebase_config": {"instance_name": "audit"}}
                    if backend_type == "lakebase"
                    else {}
                )
                response = await client.post(f"/{backend_type}/save-config", json=body)
        assert (
            response.status_code
            == {
                None: 200,
                "cleanup": 500,
                "default": 409,
                "commit": 409,
                "closed_commit": 500,
            }[failure]
        )
        assert "SENTINEL" not in response.text
        async with factory() as verify:
            rows = (await verify.execute(select(MemoryBackend))).scalars().all()
            assert len(rows) == 1 and rows[0].is_default
            assert rows[0].id == ("old" if failure else response.json()["backend_id"])
    finally:
        await engine.dispose()
