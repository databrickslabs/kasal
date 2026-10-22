"""A Databricks App with no 'lakebase' resource starts on SQLite unless told not to."""

import logging
from unittest.mock import patch

import pytest

from src.db.app_resource import check_sqlite_fallback


class _ReachedSqlitePath(Exception):
    """Raised by the patched reload to stop init_db once it passes the guard."""


def _in_app_without_resource():
    return (
        patch("src.core.databricks_app.is_databricks_app", return_value=True),
        patch(
            "src.core.databricks_app.LakebaseAppResource.from_env", return_value=None
        ),
        patch("importlib.reload", side_effect=_ReachedSqlitePath),
    )


@pytest.mark.asyncio
async def test_init_db_falls_back_to_sqlite_when_not_required(monkeypatch, caplog):
    monkeypatch.delenv("KASAL_REQUIRE_LAKEBASE_RESOURCE", raising=False)
    in_app, no_resource, reload = _in_app_without_resource()
    from src.db.session import init_db

    with in_app, no_resource, reload, caplog.at_level(logging.WARNING):
        with pytest.raises(_ReachedSqlitePath):
            await init_db()
    assert "No 'lakebase' database resource is attached" in caplog.text


@pytest.mark.asyncio
async def test_init_db_refuses_when_required(monkeypatch):
    monkeypatch.setenv("KASAL_REQUIRE_LAKEBASE_RESOURCE", "true")
    in_app, no_resource, reload = _in_app_without_resource()
    from src.db.session import init_db

    with in_app, no_resource, reload:
        with pytest.raises(RuntimeError, match="Attach the 'lakebase'"):
            await init_db()


def test_check_sqlite_fallback_raises_when_required():
    with pytest.raises(RuntimeError, match="Attach the 'lakebase'"):
        check_sqlite_fallback(True)


def test_check_sqlite_fallback_warns_when_optional(caplog):
    with caplog.at_level(logging.WARNING, logger="src.db.app_resource"):
        check_sqlite_fallback(False)
    assert "KASAL_REQUIRE_LAKEBASE_RESOURCE=true" in caplog.text
