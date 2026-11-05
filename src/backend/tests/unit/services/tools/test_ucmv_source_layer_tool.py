"""UCMVSourceLayerTool: action dispatch, the no-I/O emit path, deploy wiring."""

import json

from src.services.tools.ucmv_source_layer_tool import (
    ACTIONS,
    UCMVSourceLayerTool,
    _sql_result,
)
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlError, SqlResult

ENTRIES = json.dumps(
    [
        {"table": "Dim", "source_table": "raw.sales.dim"},
        {"table": "Fact", "native_sql": "SELECT * FROM raw.sales.fact"},
    ]
)


def _tool(**overrides):
    cfg = {"catalog": "cat", "schema_name": "sch", "view_specs_json": ENTRIES}
    cfg.update(overrides)
    return UCMVSourceLayerTool(**cfg)


def test_default_config_and_actions():
    tool = UCMVSourceLayerTool()
    assert tool.name == "UCMV Source Layer Manager"
    assert tool._default_config["action"] == "emit"
    assert tool._default_config["materialized"] is True
    assert ACTIONS == ("emit", "deploy", "load_snapshots", "refresh", "count_check")


def test_unknown_action_is_reported_not_raised():
    out = json.loads(_tool()._run(action="drop_everything"))
    assert "must be one of" in out["error"]


def test_emit_needs_no_connection_and_returns_views():
    out = json.loads(
        _tool(
            row_filters=json.dumps(
                {"Fact": {"function": "main.gov.fn", "columns": ["c"]}}
            )
        )._run(action="emit")
    )
    assert out["action"] == "emit"
    assert set(out["views"]) == {"Dim", "Fact"}
    assert "ROW FILTER" in out["views"]["Fact"]["ddl"]
    assert out["views"]["Dim"]["row_filter"] is None


def test_missing_catalog_is_a_clear_error():
    out = json.loads(UCMVSourceLayerTool(view_specs_json=ENTRIES)._run(action="emit"))
    assert "catalog and schema_name are required" in out["error"]


def test_bad_row_filter_config_fails_with_message():
    bad = json.dumps({"Fact": {"function": "main.gov.fn", "constants": ["z" * 300]}})
    out = json.loads(_tool(row_filters=bad)._run(action="emit"))
    assert "255" in out["error"] or "characters" in out["error"]


def test_deploy_runs_views_in_order_and_stops_on_failure():
    executed = []

    def fake_sql(statement):
        executed.append(statement)
        if "src_fact" in statement:
            raise SqlError("no such table")
        return SqlResult()

    tool = _tool()
    tool._sql = lambda _get: fake_sql  # no Databricks auth in tests
    out = json.loads(tool._run(action="deploy"))
    assert out["all_deployed"] is False
    statuses = [s["status"] for s in out["deploy"]]
    assert "failed" in statuses
    assert len(executed) == 2


def test_refresh_requires_catalog_before_any_connection():
    out = json.loads(UCMVSourceLayerTool(views="src_a")._run(action="refresh"))
    assert "catalog and schema_name are required" in out["error"]


def test_count_check_requires_pbi_table():
    out = json.loads(_tool(uc_object="sch.src_sales")._run(action="count_check"))
    assert out["error"] == "pbi_table is required for count_check"


def test_sql_result_parses_statement_payload_without_failing_on_empty():
    payload = {
        "manifest": {"schema": {"columns": [{"name": "table_name"}, {"name": "n"}]}},
        "result": {"data_array": [["a", "1"], [None, None]]},
    }
    result = _sql_result(payload)
    assert result.columns == ["table_name", "n"]
    assert result.rows == [["a", "1"], [None, None]]
    assert _sql_result({}).rows == []
