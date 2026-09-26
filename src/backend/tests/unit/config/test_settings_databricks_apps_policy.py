"""Development-only flags are refused inside Databricks Apps (Settings)."""

import logging

import pytest

from src.config.settings import Settings

_APP = {
    "DATABRICKS_APP_NAME": "kasal",
    "DATABRICKS_APP_PORT": "8000",
    "DATABRICKS_WORKSPACE_ID": "123",
    "DATABRICKS_HOST": "https://example.com",
}
_FLAGS = ("DEBUG_MODE", "KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS", "DOCS_ENABLED")


@pytest.fixture
def local(monkeypatch):
    for key in (*_APP, "ENVIRONMENT", "CORS_ORIGINS", *_FLAGS):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("KASAL_DEPLOYMENT_MODE", "local")
    return monkeypatch


@pytest.fixture
def apps(local):
    local.delenv("KASAL_DEPLOYMENT_MODE", raising=False)
    for key, value in _APP.items():
        local.setenv(key, value)
    return local


def test_outside_apps_flags_and_cors_default_apply(local):
    for flag in _FLAGS:
        local.setenv(flag, "true")
    s = Settings()
    assert s.DEBUG_MODE and s.DOCS_ENABLED
    assert s.KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS
    assert "http://localhost:3000" in s.CORS_ORIGINS


def test_inside_apps_unsafe_flags_are_ignored_and_logged(apps, caplog):
    for flag in _FLAGS:
        apps.setenv(flag, "true")
    with caplog.at_level(logging.ERROR, logger="src.config.settings"):
        s = Settings()
    assert not s.DEBUG_MODE
    assert not s.DOCS_ENABLED
    assert not s.KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS
    logged = caplog.text
    for flag in _FLAGS:
        assert f"{flag} is set but ignored inside Databricks Apps" in logged


def test_inside_apps_docs_are_off_even_by_default(apps):
    assert Settings().DOCS_ENABLED is False


def test_inside_apps_localhost_cors_default_is_dropped(apps):
    assert Settings().CORS_ORIGINS == []


def test_inside_apps_explicit_cors_origins_are_honoured(apps):
    apps.setenv("CORS_ORIGINS", '["https://portal.example.com"]')
    assert Settings().CORS_ORIGINS == ["https://portal.example.com"]


def test_app_name_alone_is_enough_to_refuse(local):
    """A partial platform environment fails closed."""
    local.setenv("DATABRICKS_APP_NAME", "kasal")
    local.setenv("DEBUG_MODE", "true")
    assert Settings().DEBUG_MODE is False


def test_environment_development_inside_apps_is_ignored(apps, caplog):
    apps.setenv("ENVIRONMENT", "development")
    with caplog.at_level(logging.WARNING, logger="src.config.settings"):
        Settings()
    assert "ENVIRONMENT=development is ignored" in caplog.text
