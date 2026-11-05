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
    assert ACTIONS == (
        "emit",
        "deploy",
        "load_snapshots",
        "refresh",
        "count_check",
        "setup_access",
    )


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


def test_setup_access_is_dry_run_by_default_and_runs_only_when_confirmed():
    tool = _tool()
    cfg = {
        "access_schema": "cat.common",
        "reader_groups": "grp_a, grp_b",
        "access_rows": json.dumps(
            [
                {
                    "group_name": "grp_a",
                    "field": "company_code",
                    "value": "C1",
                    "reason": "r",
                }
            ]
        ),
    }
    dry = json.loads(tool._run(action="setup_access", **cfg))
    assert dry["mode"] == "dry_run"
    assert any("CREATE OR REPLACE FUNCTION" in s for s in dry["statements"])
    assert any(
        "GRANT EXECUTE ON FUNCTION" in s and "`grp_b`" in s for s in dry["statements"]
    )
    assert any(s.startswith("MERGE INTO") for s in dry["statements"])

    executed = []
    tool._sql = lambda _get: lambda statement: executed.append(statement) or SqlResult()
    done = json.loads(tool._run(action="setup_access", confirm=True, **cfg))
    assert done["mode"] == "executed"
    assert done["all_applied"] is True
    assert executed == dry["statements"]


def test_setup_access_requires_a_catalog_schema_access_schema():
    out = json.loads(_tool()._run(action="setup_access", access_schema="just_schema"))
    assert "access_schema must be catalog.schema" in out["error"]


def test_emit_reports_fact_rls_warning_and_accepts_shorthand():
    entries = json.dumps(
        [
            {"table": "Dim", "source_table": "raw.sales.dim"},
            {"table": "Fact", "source_table": "raw.sales.fact"},
        ]
    )
    relationships = json.dumps(
        [
            {
                "from_table": "Fact",
                "from_column": "k",
                "to_table": "Dim",
                "to_column": "k",
            }
        ]
    )
    out = json.loads(
        _tool(
            view_specs_json=entries,
            access_schema="cat.common",
            row_filters=json.dumps(
                {"Dim": {"value_access": {"field": "company_code", "column": "cc"}}}
            ),
            relationships_json=relationships,
        )._run(action="emit")
    )
    assert out["rls_warnings"] == [
        "Fact is not filtered by company_code: a filter on Dim does not reach it; "
        "add the column to the fact MV and a value_access filter"
    ]
    assert out["views"]["Dim"]["row_filter"]["function"] == "cat.common.value_access"
