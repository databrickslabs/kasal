"""Local storage folders: home-directory defaults locally, app-relative in Apps."""

import logging
from pathlib import Path

import pytest

from src.utils import memory_paths, storage_paths

_APP = {
    "DATABRICKS_APP_NAME": "kasal",
    "DATABRICKS_APP_PORT": "8000",
    "DATABRICKS_WORKSPACE_ID": "123",
    "DATABRICKS_HOST": "https://example.com",
}


@pytest.fixture
def home(tmp_path, monkeypatch):
    for key in (
        *_APP,
        "KASAL_MEMORY_DIR",
        "KASAL_ENGINE_STORAGE_DIR",
        "CREWAI_STORAGE_DIR",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    monkeypatch.setattr("src.core.paths.BACKEND_ROOT", tmp_path / "app")
    monkeypatch.setattr(memory_paths, "_warned_in_apps", False)
    return tmp_path


def test_local_dev_keeps_the_home_directory_defaults(home):
    assert memory_paths.local_memory_root() == home / "home" / ".kasal" / "memory"
    assert storage_paths.db_storage_path() == str(
        home / "home" / ".local" / "share" / "kasal_engine"
    )


def test_inside_apps_defaults_are_app_relative_and_warned(home, monkeypatch, caplog):
    for key, value in _APP.items():
        monkeypatch.setenv(key, value)
    with caplog.at_level(logging.WARNING, logger="src.utils.memory_paths"):
        root = memory_paths.local_memory_root()
    assert root == home / "app" / "data" / "memory"
    assert storage_paths.db_storage_path() == str(home / "app" / "data" / "engine")
    assert "does not survive a redeploy" in caplog.text


def test_startup_check_is_silent_outside_apps(home, caplog):
    with caplog.at_level(logging.WARNING, logger="src.utils.memory_paths"):
        memory_paths.warn_if_local_memory_is_ephemeral()
    assert caplog.text == ""


def test_explicit_override_still_wins_inside_apps(home, monkeypatch):
    for key, value in _APP.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("KASAL_MEMORY_DIR", str(home / "custom"))
    assert memory_paths.local_memory_root() == home / "custom"
