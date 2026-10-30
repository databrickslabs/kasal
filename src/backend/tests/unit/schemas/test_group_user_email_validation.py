"""Unit tests for GroupUserCreateRequest's environment-aware email validation.

Production / Databricks Apps enforce strict RFC email validation, while local dev
also accepts synthetic no-TLD emails (e.g. dev@localhost) that the app itself issues.
"""

import pytest
from pydantic import ValidationError

from src.schemas.group import GroupUserCreateRequest


def test_local_dev_accepts_localhost_email(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    m = GroupUserCreateRequest(user_email="dev@localhost")
    assert m.user_email == "dev@localhost"


def test_local_dev_trims_whitespace(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    m = GroupUserCreateRequest(user_email="  dev@localhost  ")
    assert m.user_email == "dev@localhost"


def test_local_dev_rejects_malformed(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    with pytest.raises(ValidationError):
        GroupUserCreateRequest(user_email="garbage")
    with pytest.raises(ValidationError):
        GroupUserCreateRequest(user_email="@nolocal")
    with pytest.raises(ValidationError):
        GroupUserCreateRequest(user_email="nodomain@")


def test_production_rejects_localhost_email(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    with pytest.raises(ValidationError):
        GroupUserCreateRequest(user_email="dev@localhost")


def test_production_accepts_real_email(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    m = GroupUserCreateRequest(user_email="user@company.com")
    assert m.user_email == "user@company.com"


@pytest.mark.parametrize("environment", ["development", "local", ""])
def test_databricks_apps_is_production_whatever_environment_says(
    monkeypatch, environment
):
    """Inside Databricks Apps local-dev leniency never applies."""
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("DATABRICKS_APP_NAME", "kasal")
    with pytest.raises(ValidationError):
        GroupUserCreateRequest(user_email="dev@localhost")
