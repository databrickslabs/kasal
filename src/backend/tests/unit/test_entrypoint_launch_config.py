"""The launch configuration ``src/entrypoint.py`` derives from its flags.

Exercised from the entrypoint's stdlib section only (as
``test_app_dependency_startup`` does), so nothing here imports the backend.
"""

import types
from pathlib import Path

import pytest

APP_ROOT = Path(__file__).resolve().parents[3]
SOURCE = (APP_ROOT / "entrypoint.py").read_text()
BOOTSTRAP = SOURCE.split("# Add backend directory to path FIRST", 1)[0]

launcher = types.ModuleType("entrypoint_launch_config")
launcher.__file__ = str(APP_ROOT / "entrypoint.py")
exec(compile(BOOTSTRAP, launcher.__file__, "exec"), launcher.__dict__)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("postgresql://u:p@db:5432/k", "postgresql+asyncpg://u:p@db:5432/k"),
        ("postgres://u:p@db:5432/k", "postgresql+asyncpg://u:p@db:5432/k"),
        ("postgresql+asyncpg://u:p@db/k", "postgresql+asyncpg://u:p@db/k"),
        ("postgresql+psycopg://u:p@db/k", "postgresql+psycopg://u:p@db/k"),
    ],
)
def test_postgres_urls_get_the_async_driver(url, expected):
    assert launcher._async_postgres_url(url) == expected


def test_postgres_sets_database_type_and_an_async_url_everywhere(tmp_path):
    env = launcher._database_environment(
        "postgres", "postgresql://u:p@db:5432/k", tmp_path, environ={}
    )
    url = "postgresql+asyncpg://u:p@db:5432/k"
    assert env == {
        "DATABASE_TYPE": "postgres",
        "DATABASE_URL": url,
        "DATABASE_URI": url,
        "SYNC_DATABASE_URI": url,
    }


def test_postgres_without_url_uses_the_local_default_with_asyncpg(tmp_path):
    env = launcher._database_environment("postgres", None, tmp_path, environ={})
    assert env["DATABASE_TYPE"] == "postgres"
    assert env["DATABASE_URI"].startswith("postgresql+asyncpg://")
    assert env["DATABASE_URI"].endswith("@localhost:5432/kasal")


def test_sqlite_sets_database_type_and_honours_sqlite_db_path(tmp_path):
    db = str(tmp_path / "x.db")
    env = launcher._database_environment(
        "sqlite", None, tmp_path, environ={"SQLITE_DB_PATH": db}
    )
    assert env["DATABASE_TYPE"] == "sqlite"
    assert env["DATABASE_URI"] == f"sqlite+aiosqlite:///{db}"
    assert env["SYNC_DATABASE_URI"] == f"sqlite:///{db}"
    assert env["SQLITE_DB_PATH"] == db


def test_sqlite_defaults_beside_the_entrypoint(tmp_path):
    env = launcher._database_environment("sqlite", None, tmp_path, environ={})
    assert env["SQLITE_DB_PATH"] == str(tmp_path / "kasal.db")


@pytest.mark.parametrize(
    "environ, host",
    [
        ({}, "127.0.0.1"),
        ({"DATABRICKS_APP_NAME": "kasal"}, "0.0.0.0"),
        ({"KASAL_BIND_HOST": "0.0.0.0"}, "0.0.0.0"),
        ({"KASAL_BIND_HOST": "10.0.0.5", "DATABRICKS_APP_NAME": "kasal"}, "10.0.0.5"),
    ],
)
def test_bind_host(environ, host):
    assert launcher._bind_host(environ) == host


def test_environment_dev_is_a_local_run_with_the_dev_identity():
    overrides = launcher._environment_overrides("dev", environ={})
    assert overrides == {"KASAL_DEPLOYMENT_MODE": "local", "LOCAL_DEV_AUTH": "true"}
    # It must never claim to be inside Databricks Apps: the backend refuses
    # LOCAL_DEV_AUTH whenever DATABRICKS_APP_NAME is set.
    assert "DATABRICKS_APP_NAME" not in overrides


def test_environment_dev_keeps_an_explicit_local_dev_auth():
    overrides = launcher._environment_overrides(
        "dev", environ={"LOCAL_DEV_AUTH": "false"}
    )
    assert overrides["LOCAL_DEV_AUTH"] == "false"


@pytest.mark.parametrize("environment", [None, "prod"])
def test_other_environments_change_nothing(environment):
    assert launcher._environment_overrides(environment, environ={}) == {}


def test_reload_passes_an_import_string_factory():
    """Uvicorn exits 1 when asked to reload an app OBJECT."""
    run_app = SOURCE.split("def run_app():", 1)[1]
    assert '"entrypoint:build_app"' in run_app
    assert "factory=True" in run_app
