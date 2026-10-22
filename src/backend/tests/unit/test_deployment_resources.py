"""Tests for deployment_resources.validate_resources - the deploy-time preflight."""

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src")
)

from deployment_resources import validate_resources

# <repo>/src/app.yaml: parents[4] of <repo>/src/backend/tests/unit/<file>.
APP_YAML = Path(__file__).resolve().parents[4] / "src" / "app.yaml"

ENV = [
    {"name": "KASAL_SQL_WAREHOUSE_ID", "valueFrom": "sql-warehouse"},
    {"name": "KASAL_LAKEBASE_RESOURCE", "valueFrom": "lakebase"},
    {"name": "KASAL_LOG_LEVEL", "value": "INFO"},
]
REQUIRE = {"name": "KASAL_REQUIRE_LAKEBASE_RESOURCE", "value": "true"}


def _resource(name, database=None, postgres=None):
    return SimpleNamespace(name=name, database=database, postgres=postgres)


def _validate(tmp_path, env, resources):
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text(yaml.safe_dump({"env": env}))
    client = MagicMock()
    client.apps.get.return_value = SimpleNamespace(resources=resources)
    return validate_resources(client, "kasal", app_yaml)


def test_lakebase_is_optional_by_default(tmp_path):
    """No lakebase resource: the deploy proceeds and reports it as unbound."""
    bound = _validate(tmp_path, ENV, [_resource("sql-warehouse")])
    assert bound == {"KASAL_SQL_WAREHOUSE_ID"}


def test_attached_lakebase_is_reported_bound(tmp_path):
    resources = [_resource("sql-warehouse"), _resource("lakebase", database=object())]
    bound = _validate(tmp_path, ENV, resources)
    assert bound == {"KASAL_SQL_WAREHOUSE_ID", "KASAL_LAKEBASE_RESOURCE"}


def test_required_flag_makes_lakebase_mandatory(tmp_path):
    with pytest.raises(ValueError, match="lakebase"):
        _validate(tmp_path, ENV + [REQUIRE], [_resource("sql-warehouse")])


def test_other_resources_stay_required(tmp_path):
    with pytest.raises(ValueError, match="sql-warehouse"):
        _validate(tmp_path, ENV, [])


def test_misnamed_database_is_rejected(tmp_path):
    """An attached database under another name would silently run on SQLite."""
    resources = [_resource("sql-warehouse"), _resource("db", postgres=object())]
    with pytest.raises(ValueError, match="named 'lakebase'"):
        _validate(tmp_path, ENV, resources)


def test_two_databases_are_rejected(tmp_path):
    resources = [
        _resource("sql-warehouse"),
        _resource("lakebase", database=object()),
        _resource("other", database=object()),
    ]
    with pytest.raises(ValueError, match="exactly one"):
        _validate(tmp_path, ENV, resources)


def test_shipped_app_yaml_does_not_require_lakebase():
    """src/app.yaml keeps the strict flag opt-in (commented out)."""
    env = yaml.safe_load(APP_YAML.read_text())["env"]
    assert "lakebase" in {row.get("valueFrom") for row in env}
    assert "KASAL_REQUIRE_LAKEBASE_RESOURCE" not in {row["name"] for row in env}
