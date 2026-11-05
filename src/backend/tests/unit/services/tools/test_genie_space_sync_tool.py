"""GenieSpaceSyncTool: action dispatch and I/O wiring, with auth, Genie and SQL faked."""

import json
from types import SimpleNamespace

import pytest

from src.services.tools import genie_space_sync_tool as tool_module
from src.services.tools.genie_space_sync_tool import GenieSpaceSyncTool
from src.services.tools.genie_space_sync_utils.genie_api import GenieApiError
from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
from .genie_space_sync_utils._fakes import (
    SPACE_ID,
    FakeGenie,
    FakeWarehouse,
    sample_tables,
    serialized_space,
)

HOST = "https://example.azuredatabricks.net"
NEW_ID = "f" * 32


class _Auth:
    def __init__(self, workspace_url: str) -> None:
        self.workspace_url = workspace_url

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": "Bearer test"}


def _space_record() -> dict[str, object]:
    return {
        "space_id": SPACE_ID,
        "title": "Sales",
        "description": "d",
        "serialized_space": json.dumps(serialized_space()),
    }


@pytest.fixture
def env(monkeypatch):
    state = SimpleNamespace(
        auth=_Auth(HOST),
        genie=FakeGenie({}),
        warehouse=FakeWarehouse(sample_tables()),
    )
    monkeypatch.setattr(
        tool_module, "resolve_tool_auth", lambda host_override=None: state.auth
    )
    monkeypatch.setattr(tool_module, "requests_http", lambda host, headers: state.genie)
    monkeypatch.setattr(
        MetricViewDeployerTool,
        "_execute_sql_sync",
        staticmethod(lambda sql, url, wh, headers: state.warehouse(sql)),
    )
    return state


def _run(tool, **kwargs):
    return json.loads(tool._run(**kwargs))


def test_tool_identity_and_default_config():
    tool = GenieSpaceSyncTool(warehouse_id="wh-default", check_sql=False)
    assert tool.name == "Genie Space Sync"
    assert tool._default_config == {"warehouse_id": "wh-default", "check_sql": False}


def test_an_unknown_action_is_an_error_and_touches_nothing(env):
    result = _run(GenieSpaceSyncTool(), action="delete")
    assert "action must be one of" in result["error"]
    assert env.genie.calls == []


def test_extract_returns_the_five_files_and_counts(env):
    env.genie = FakeGenie({("GET", f"/{SPACE_ID}"): _space_record()})
    result = _run(GenieSpaceSyncTool(), action="extract", space_id=SPACE_ID)

    assert result["action"] == "extract"
    assert result["title"] == "Sales"
    assert set(result["files"]) == {
        "header.yaml",
        "config.yaml",
        "data_sources.yaml",
        "instructions.yaml",
        "benchmarks.yaml",
    }
    assert result["counts"]["example_sqls"] == 1
    assert env.genie.methods() == ["GET"]


def test_extract_of_a_missing_space_reports_it(env):
    env.genie = FakeGenie({("GET", f"/{SPACE_ID}"): GenieApiError(404, "no")})
    result = _run(GenieSpaceSyncTool(), action="extract", space_id=SPACE_ID)
    assert "not found" in result["error"]


def test_a_malformed_space_id_is_refused_before_any_call(env):
    result = _run(GenieSpaceSyncTool(), action="extract", space_id="../secrets")
    assert "32-character" in result["error"]
    assert env.genie.calls == []


def _extract_files() -> str:
    from src.services.tools.genie_space_sync_utils.space_files import (
        serialized_to_files,
    )

    files = serialized_to_files(
        serialized_space(), {"space_id": SPACE_ID, "title": "Sales", "description": "d"}
    )
    return json.dumps(files)


def test_deploy_without_confirmation_is_a_dry_run_that_only_reads(env):
    env.genie = FakeGenie(
        {
            ("GET", f"/{SPACE_ID}"): GenieApiError(404, "no"),
            ("GET", ""): {"spaces": []},
        }
    )
    result = _run(
        GenieSpaceSyncTool(),
        action="deploy",
        space_files=_extract_files(),
        warehouse_id="wh-1",
    )

    assert result["dry_run"] is True
    assert result["operation"] == "create"
    assert "POST" not in env.genie.methods()
    assert "confirm_deploy" in result["note"]


def test_confirmed_deploy_creates_and_returns_files_with_the_new_id(env):
    env.genie = FakeGenie(
        {
            ("GET", f"/{SPACE_ID}"): GenieApiError(404, "no"),
            ("GET", ""): {"spaces": []},
            ("POST", ""): {"space_id": NEW_ID},
        }
    )
    result = _run(
        GenieSpaceSyncTool(),
        action="deploy",
        space_files=_extract_files(),
        warehouse_id="wh-1",
        confirm_deploy=True,
    )

    assert result["dry_run"] is False
    assert result["operation"] == "created"
    assert result["space_id"] == NEW_ID
    assert result["url"].endswith(f"/genie/rooms/{NEW_ID}")
    header = result["files"]["header.yaml"]
    assert NEW_ID in header
    assert "POST" in env.genie.methods()


def test_confirmed_deploy_updates_the_space_named_in_the_header(env):
    env.genie = FakeGenie(
        {
            ("GET", f"/{SPACE_ID}"): {"space_id": SPACE_ID},
            ("PATCH", f"/{SPACE_ID}"): {"space_id": SPACE_ID},
        }
    )
    result = _run(
        GenieSpaceSyncTool(),
        action="deploy",
        space_files=_extract_files(),
        warehouse_id="wh-1",
        confirm_deploy="true",
    )

    assert result["operation"] == "updated"
    assert result["space_id"] == SPACE_ID
    assert "POST" not in env.genie.methods()
    assert "PATCH" in env.genie.methods()


def test_deploy_needs_a_warehouse_and_a_title(env):
    result = _run(GenieSpaceSyncTool(), action="deploy", space_files=_extract_files())
    assert "warehouse_id is required" in result["error"]

    no_title = json.dumps({"header.yaml": "space_id: ''\n"})
    result = _run(
        GenieSpaceSyncTool(), action="deploy", space_files=no_title, warehouse_id="wh"
    )
    assert "title" in result["error"]


def test_the_default_warehouse_is_used_when_the_call_has_none(env):
    env.genie = FakeGenie(
        {
            ("GET", f"/{SPACE_ID}"): GenieApiError(404, "no"),
            ("GET", ""): {"spaces": []},
        }
    )
    result = _run(
        GenieSpaceSyncTool(warehouse_id="wh-default"),
        action="deploy",
        space_files=_extract_files(),
    )
    assert result["dry_run"] is True


def test_check_offline_runs_against_the_files_and_reports_verdicts(env):
    result = _run(
        GenieSpaceSyncTool(),
        action="check",
        space_files=_extract_files(),
        warehouse_id="wh-1",
    )

    assert result["action"] == "check"
    assert result["source"] == "files"
    assert result["ok"] is True
    assert result["sql_checked"] is True
    assert result["verdicts"]
    assert env.genie.calls == []


def test_check_live_reads_the_space_then_checks_it(env):
    env.genie = FakeGenie({("GET", f"/{SPACE_ID}"): _space_record()})
    result = _run(
        GenieSpaceSyncTool(),
        action="check",
        space_id=SPACE_ID,
        warehouse_id="wh-1",
        check_sql=False,
    )

    assert result["source"] == "live"
    assert result["sql_checked"] is False
    assert all(not s.startswith("SELECT COUNT") for s in env.warehouse.statements)


def test_check_without_sql_warehouse_reports_failed_statements(env):
    result = _run(GenieSpaceSyncTool(), action="check", space_files=_extract_files())
    assert result["ok"] is False
    failed = [v for v in result["verdicts"] if v["status"] == "fail"]
    assert any("warehouse_id is required" in v["detail"] for v in failed)


def test_check_needs_a_space_or_files(env):
    result = _run(GenieSpaceSyncTool(), action="check", warehouse_id="wh-1")
    assert "space_id (live) or space_files" in result["error"]


def test_a_bad_mapping_set_is_an_error_not_a_verdict(env):
    result = _run(
        GenieSpaceSyncTool(),
        action="check",
        space_files=_extract_files(),
        warehouse_id="wh-1",
        reconciliation_mappings="[1, 2]",
    )
    assert "reconciliation_mappings must be a JSON object" in result["error"]


def test_an_untrusted_workspace_host_is_refused(env):
    env.auth = _Auth("https://attacker.example.com")
    result = _run(GenieSpaceSyncTool(), action="extract", space_id=SPACE_ID)
    assert "not a trusted Databricks host" in result["error"]


def test_failed_authentication_is_an_error(env, monkeypatch):
    monkeypatch.setattr(
        tool_module, "resolve_tool_auth", lambda host_override=None: None
    )
    result = _run(GenieSpaceSyncTool(), action="extract", space_id=SPACE_ID)
    assert result["error"] == "Databricks authentication failed"
