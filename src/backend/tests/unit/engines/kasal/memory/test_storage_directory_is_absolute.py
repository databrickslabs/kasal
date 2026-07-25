"""``CREWAI_STORAGE_DIR`` must always be an ABSOLUTE path.

``kasal_engine.utils.db_storage_path`` resolves a relative value against the
process CWD — which for the backend is the source tree. The databricks and
lakebase branches set bare directory NAMES, so a Databricks-backed run created

    src/backend/kasal_databricks_<crew_id>/memory.db

inside the repo and left it there. It is gitignored, so it accumulated silently;
one such directory was found months later with an empty database in it.

The DEFAULT branch had already been fixed to return an absolute path, with a
docstring explaining precisely this hazard. The other two were missed — hence
this test, which covers all three rather than the one that broke.
"""

import os
from pathlib import Path

import pytest

from src.engines.kasal.memory.crew_memory_service import CrewMemoryService


@pytest.fixture
def service():
    return CrewMemoryService({"group_id": "g1"}, None)


@pytest.fixture(autouse=True)
def _restore_env():
    original = os.environ.get("CREWAI_STORAGE_DIR")
    yield
    if original is None:
        os.environ.pop("CREWAI_STORAGE_DIR", None)
    else:
        os.environ["CREWAI_STORAGE_DIR"] = original


@pytest.mark.parametrize("backend_type", ["databricks", "lakebase", "default"])
def test_storage_dir_is_absolute(service, backend_type):
    os.environ.pop("CREWAI_STORAGE_DIR", None)
    service.setup_storage_directory("crew_1", {"backend_type": backend_type})
    value = os.environ["CREWAI_STORAGE_DIR"]
    assert Path(value).is_absolute(), (
        f"{backend_type} set a relative CREWAI_STORAGE_DIR ({value!r}); "
        "db_storage_path would resolve it under the backend source tree"
    )


@pytest.mark.parametrize("backend_type", ["databricks", "lakebase", "default"])
def test_storage_dir_lives_under_the_memory_root(service, backend_type):
    """One known root for every backend, so nothing lands in the repo."""
    from src.utils.memory_paths import local_memory_root

    os.environ.pop("CREWAI_STORAGE_DIR", None)
    service.setup_storage_directory("crew_1", {"backend_type": backend_type})
    assert Path(os.environ["CREWAI_STORAGE_DIR"]).is_relative_to(local_memory_root())


def test_unknown_backend_leaves_the_env_untouched(service):
    """Only the three known backends configure a directory."""
    os.environ.pop("CREWAI_STORAGE_DIR", None)
    service.setup_storage_directory("crew_1", {"backend_type": "something_else"})
    assert "CREWAI_STORAGE_DIR" not in os.environ


def test_no_config_leaves_the_env_untouched(service):
    os.environ.pop("CREWAI_STORAGE_DIR", None)
    service.setup_storage_directory("crew_1", {})
    assert "CREWAI_STORAGE_DIR" not in os.environ
