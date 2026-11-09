"""Space checks against a fake warehouse: data sources, SQL, snippets, percentages, prose."""

import copy

from src.services.tools.genie_space_sync_utils.checks_report import build_report
from src.services.tools.genie_space_sync_utils.checks_space import (
    check_data_sources,
    check_identifiers,
    check_percent_scaling,
    check_snippets,
    check_sql_items,
    unknown_identifiers,
)
from src.services.tools.genie_space_sync_utils.metric_views import ViewCatalog
from src.services.tools.genie_space_sync_utils.verdicts import FAIL, PASS, REVIEW

from ._fakes import FakeWarehouse, sample_tables, serialized_space


def _statuses(verdicts):
    return {(v.check, v.target): v.status for v in verdicts}


def _catalog(warehouse):
    return ViewCatalog(warehouse)


def test_a_clean_space_passes_the_data_source_checks():
    warehouse = FakeWarehouse(sample_tables())
    verdicts = check_data_sources(serialized_space(), _catalog(warehouse))

    assert {v.status for v in verdicts} == {PASS}
    assert len(verdicts) == 2


def test_a_column_setting_on_a_missing_column_fails():
    space = serialized_space()
    space["data_sources"]["tables"][0]["column_configs"].append(
        {"column_name": "retired_column"}
    )
    verdicts = check_data_sources(space, _catalog(FakeWarehouse(sample_tables())))

    failed = [v for v in verdicts if v.status == FAIL]
    assert len(failed) == 1
    assert "retired_column" in failed[0].detail


def test_an_unreadable_source_fails_with_the_error():
    tables = sample_tables()
    del tables["main.sales.regions"]
    verdicts = check_data_sources(serialized_space(), _catalog(FakeWarehouse(tables)))

    failed = [v for v in verdicts if v.status == FAIL]
    assert [v.target for v in failed] == ["main.sales.regions"]
    assert "TABLE_OR_VIEW_NOT_FOUND" in failed[0].detail


def test_example_and_benchmark_sql_run_as_counts_and_pass():
    warehouse = FakeWarehouse(sample_tables())
    verdicts = check_sql_items(serialized_space(), warehouse)

    assert {v.check for v in verdicts} == {"example_sql", "benchmark_sql"}
    assert all(v.status == PASS and v.detail == "7 rows" for v in verdicts)
    # Every statement is wrapped so only a count comes back.
    sql_statements = [s for s in warehouse.statements if "COUNT(*)" in s]
    assert len(sql_statements) == 2


def test_a_failing_example_sql_fails_with_the_error():
    space = serialized_space()
    warehouse = FakeWarehouse(sample_tables(), broken=("order_count",))
    verdicts = check_sql_items(space, warehouse)

    example = [v for v in verdicts if v.check == "example_sql"][0]
    assert example.status == FAIL
    assert "UNRESOLVED_COLUMN" in example.detail


def test_sql_that_uses_an_unattached_table_is_a_review_hint():
    space = serialized_space()
    space["instructions"]["example_question_sqls"][0]["sql"] = [
        "SELECT * FROM main.sales.customers"
    ]
    verdicts = check_sql_items(space, FakeWarehouse(sample_tables()))

    hints = [v for v in verdicts if v.status == REVIEW]
    assert hints and "main.sales.customers" in hints[0].detail


def test_snippets_run_against_their_view():
    warehouse = FakeWarehouse(sample_tables())
    verdicts = check_snippets(serialized_space(), warehouse)

    assert [v.status for v in verdicts] == [PASS]
    assert any("AS orders" in s for s in warehouse.statements)


def test_a_snippet_that_does_not_execute_fails():
    space = serialized_space()
    space["instructions"]["sql_snippets"]["filters"][0]["sql"] = ["orders.bad = 1"]
    verdicts = check_snippets(space, FakeWarehouse(sample_tables(), broken=("bad",)))
    assert verdicts[0].status == FAIL


def test_a_direct_percentage_scaling_fails_and_a_fallback_expression_is_reviewed():
    space = serialized_space()
    space["benchmarks"]["questions"][0]["answer"][0]["content"] = [
        "SELECT MEASURE(order_fill_rate_pct) * 100 FROM main.sales.orders"
    ]
    space["instructions"]["example_question_sqls"][0]["sql"] = [
        "SELECT COALESCE(MEASURE(order_fill_rate_pct), 0) * 100 AS x FROM main.sales.orders"
    ]
    verdicts = check_percent_scaling(space, _catalog(FakeWarehouse(sample_tables())))

    assert ("benchmark_percent", "Fill rate?") in _statuses(verdicts)
    assert _statuses(verdicts)[("benchmark_percent", "Fill rate?")] == FAIL
    example = [v for v in verdicts if v.check == "example_percent"]
    assert [v.status for v in example] == [REVIEW]


def test_no_percentage_measures_means_no_scaling_verdicts():
    tables = {
        "main.sales.orders": metric_view_without_percentages(),
        "main.sales.regions": sample_tables()["main.sales.regions"],
    }
    space = serialized_space()
    space["benchmarks"]["questions"][0]["answer"][0]["content"] = [
        "SELECT MEASURE(order_fill_rate_pct) * 100 FROM main.sales.orders"
    ]
    assert check_percent_scaling(space, _catalog(FakeWarehouse(tables))) == []


def metric_view_without_percentages() -> str:
    import json

    return json.dumps(
        {
            "columns": [{"name": "order_count", "is_measure": True}],
            "view_text": "version: 1.1\nsource: x\nmeasures:\n  - name: order_count\n"
            "    expr: COUNT(1)\n",
        }
    )


def test_unknown_identifiers_skip_known_names_and_table_fragments():
    known = {"order_count", "region"}
    tables = ["main.sales.orders"]
    text = "Use order_count and region, not order_totl; Not a table part."
    assert unknown_identifiers(text, known, tables) == ["order_totl"]


def test_check_identifiers_reports_prose_that_names_nothing_deployed():
    space = serialized_space()
    space["instructions"]["text_instructions"][0]["content"] = [
        "Count order_totl per region_code."
    ]
    verdicts = check_identifiers(space, _catalog(FakeWarehouse(sample_tables())))

    hints = [v for v in verdicts if v.check == "identifier"]
    assert hints and hints[0].status == REVIEW
    assert "order_totl" in hints[0].detail and "region_code" in hints[0].detail


def test_build_report_is_ok_when_nothing_fails_and_review_does_not_flip_it():
    warehouse = FakeWarehouse(sample_tables())
    report = build_report(serialized_space(), {}, warehouse, check_sql=True)

    assert report["ok"] is True
    assert report["sql_checked"] is True
    summary = report["summary"]
    assert isinstance(summary, dict) and summary[FAIL] == 0


def test_build_report_without_sql_skips_every_warehouse_statement():
    warehouse = FakeWarehouse(sample_tables())
    report = build_report(serialized_space(), {}, warehouse, check_sql=False)

    assert not any("COUNT(*)" in statement for statement in warehouse.statements)
    checks = {v["check"] for v in report["verdicts"]}
    assert "example_sql" not in checks and "snippet_filters" not in checks


def test_build_report_fails_when_a_statement_breaks():
    space = copy.deepcopy(serialized_space())
    space["instructions"]["example_question_sqls"][0]["sql"] = ["SELECT broken_col"]
    report = build_report(
        space,
        {},
        FakeWarehouse(sample_tables(), broken=("broken_col",)),
        check_sql=True,
    )
    assert report["ok"] is False
