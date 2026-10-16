"""Connect-and-migrate is refused (400) when the App installation binds Lakebase.

With the native ``lakebase`` resource bound, Kasal already runs on that database
in schema ``kasal``. ``migrate_existing_data_stream`` with "recreate" ran
``DROP SCHEMA IF EXISTS kasal CASCADE`` on it: it lacked the
``LakebaseAppResource.from_env()`` guard ``save_config`` has. Every migrate
entry point now refuses before connecting, and the DROP itself refuses too.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from src.api.database_management_router import router
from src.core.databricks_app import LakebaseAppResource
from src.core.exceptions import BadRequestError, KasalError
from src.dependencies.admin_auth import get_system_admin_user
from src.dependencies.providers import get_group_context, get_local_db
from src.services.databricks.lakebase.schema import LakebaseSchemaService
from src.services.databricks.lakebase.service import LakebaseService

_RESOURCE = LakebaseAppResource(
    "instance.example.com", "databricks_postgres", "app-sp", 5432, "lakebase"
)


@pytest.fixture
def installation_managed():
    with patch.object(LakebaseAppResource, "from_env", return_value=_RESOURCE):
        yield


@pytest.fixture
def engines():
    """Every engine the migration could build; none may see a statement."""
    engine = MagicMock()
    with (
        patch(
            "src.services.databricks.lakebase.service.create_engine",
            return_value=engine,
        ),
        patch(
            "src.services.databricks.lakebase.connection.LakebaseConnectionService."
            "create_lakebase_engine_sync",
            return_value=engine,
        ),
        patch(
            "src.services.databricks.lakebase.connection.LakebaseConnectionService."
            "generate_credentials",
            AsyncMock(),
        ) as credentials,
    ):
        yield SimpleNamespace(engine=engine, credentials=credentials)


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.add_exception_handler(
        KasalError,
        lambda request, exc: JSONResponse(
            status_code=exc.status_code, content={"detail": exc.detail}
        ),
    )
    app.dependency_overrides[get_system_admin_user] = lambda: SimpleNamespace()
    app.dependency_overrides[get_group_context] = lambda: SimpleNamespace(
        group_email="admin@example.com", access_token=None
    )

    async def _local_db():
        yield MagicMock()

    app.dependency_overrides[get_local_db] = _local_db
    return TestClient(app)


def _sent(mock: MagicMock) -> str:
    """Every call made on ``mock`` or its children, with SQL rendered as text."""
    return " ".join(
        f"{name} " + " ".join(str(arg) for arg in args)
        for name, args, _ in mock.mock_calls
    )


def _no_drop_was_sent(engines) -> None:
    engines.credentials.assert_not_called()
    sent = _sent(engines.engine)
    assert "DROP SCHEMA" not in sent and "begin" not in sent and "connect" not in sent


class TestTheEndpointsReturn400:
    @pytest.mark.parametrize(
        "path",
        [
            "/database-management/lakebase/migrate/stream",
            "/database-management/lakebase/migrate",
        ],
    )
    def test_with_the_app_resource_bound(self, path, installation_managed, engines):
        body = {"instance_name": "i", "endpoint": "e", "recreate_schema": True}

        response = _client().post(path, json=body)

        assert response.status_code == 400, response.text
        detail = response.json()["detail"]
        assert "lakebase" in detail and "drop" in detail
        _no_drop_was_sent(engines)


@pytest.mark.asyncio
class TestTheServiceRefusesOnItsOwn:
    """Defence in depth: any other caller of the service is refused too."""

    async def test_the_stream_never_reaches_drop_schema(
        self, installation_managed, engines
    ):
        stream = LakebaseService(session=None).migrate_existing_data_stream(
            "i", "e", recreate_schema=True
        )
        with pytest.raises(BadRequestError):
            await stream.__anext__()
        _no_drop_was_sent(engines)

    async def test_the_one_shot_migration_is_refused(
        self, installation_managed, engines
    ):
        with pytest.raises(BadRequestError):
            await LakebaseService(session=MagicMock()).migrate_existing_data(
                "i", "e", recreate_schema=True
            )
        _no_drop_was_sent(engines)


class TestTheSchemaDropRefuses:
    def test_sync_drop(self, installation_managed):
        engine = MagicMock()
        with pytest.raises(BadRequestError):
            LakebaseSchemaService().drop_schema_sync(engine, "app@example.com")
        engine.begin.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_recreate(self, installation_managed):
        engine = MagicMock()
        with pytest.raises(BadRequestError):
            await LakebaseSchemaService().create_schema_async(
                engine, "app@example.com", recreate=True
            )
        engine.begin.assert_not_called()

    def test_without_the_resource_the_drop_still_runs(self):
        engine = MagicMock()
        LakebaseSchemaService().drop_schema_sync(engine, "app@example.com")
        assert "DROP SCHEMA IF EXISTS kasal CASCADE" in _sent(engine)
