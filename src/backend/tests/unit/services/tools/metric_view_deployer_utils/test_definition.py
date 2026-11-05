"""Definition verification: deployed view_text vs the YAML spec (fake SQL runner, no network)."""

import json

from src.services.tools.metric_view_deployer_utils.definition import (
    count_measures_without_format,
    definition_differences,
    definition_report,
    deployed_spec,
    item_names,
    parse_yaml_spec,
    verify_view_definition,
    view_text_from_describe,
)

YAML = """version: 0.1
source: main.sales.orders
joins:
  - name: customers
    source: main.sales.customers
    on: source.customer_id = customers.id
filter: status = 'closed'
dimensions:
  - name: region
    expr: region
    comment: Sales region
measures:
  - name: total_revenue
    expr: SUM(amount)
    comment: Gross revenue
    display_name: Revenue
    format:
      type: currency
    synonyms:
      - sales
  - name: order_count
    expr: COUNT(1)
"""


def _describe(view_text: str | None, columns=()) -> dict:
    doc: dict = {"columns": [{"name": n, "is_measure": m} for n, m in columns]}
    if view_text is not None:
        doc["view_text"] = view_text
    return {
        "success": True,
        "state": "SUCCEEDED",
        "data": {"result": {"data_array": [[json.dumps(doc)]]}},
    }


def _deployed(yaml_text: str = YAML) -> dict:
    return parse_yaml_spec(yaml_text)


class TestDefinitionDifferences:
    def test_identical_definitions_have_no_differences(self):
        assert definition_differences(_deployed(), _deployed()) == []

    def test_whitespace_only_difference_in_expr_is_ignored(self):
        deployed = _deployed()
        deployed["measures"][0]["expr"] = "SUM(amount)\n"
        assert definition_differences(_deployed(), deployed) == []

    def test_changed_expr_is_reported_per_measure(self):
        deployed = _deployed()
        deployed["measures"][0]["expr"] = "SUM(net_amount)"
        assert definition_differences(_deployed(), deployed) == [
            "measure total_revenue (expr)"
        ]

    def test_format_display_name_comment_and_synonyms_are_compared(self):
        deployed = _deployed()
        measure = deployed["measures"][0]
        measure["format"] = {"type": "number"}
        measure["display_name"] = "Turnover"
        measure["comment"] = "Net revenue"
        measure["synonyms"] = ["turnover"]
        diffs = definition_differences(_deployed(), deployed)
        assert diffs == [
            "measure total_revenue (comment, display_name, format, synonyms)"
        ]

    def test_window_difference_is_compared(self):
        ours = _deployed()
        ours["measures"][1]["window"] = [{"order": "date", "range": "trailing 1 month"}]
        assert definition_differences(ours, _deployed()) == [
            "measure order_count (window)"
        ]

    def test_missing_and_extra_items_are_reported(self):
        deployed = _deployed()
        deployed["measures"] = [deployed["measures"][0]]
        deployed["dimensions"].append({"name": "channel", "expr": "channel"})
        diffs = definition_differences(_deployed(), deployed)
        assert "measure order_count (missing)" in diffs
        assert "dimension channel (only deployed)" in diffs

    def test_source_joins_and_filter_are_compared(self):
        deployed = _deployed()
        deployed["source"] = "main.sales.orders_v2"
        deployed["filter"] = "status = 'open'"
        deployed["joins"] = []
        assert definition_differences(_deployed(), deployed) == [
            "source",
            "joins",
            "filter",
        ]

    def test_stale_definition_of_old_view_is_caught(self):
        """A failed deploy leaves the old view: its different expression must surface."""
        old_view = YAML.replace("SUM(amount)", "SUM(legacy_amount)")
        assert definition_differences(_deployed(), _deployed(old_view)) == [
            "measure total_revenue (expr)"
        ]


class TestMeasuresWithoutFormat:
    def test_counts_measures_lacking_format(self):
        assert count_measures_without_format(_deployed()) == 1

    def test_item_names_lists_measures_in_order(self):
        assert item_names(_deployed(), "measures") == ["total_revenue", "order_count"]
        assert item_names(_deployed(), "dimensions") == ["region"]
        assert item_names({}, "measures") == []


class TestParsingDescribe:
    def test_view_text_extracted_from_describe_payload(self):
        assert view_text_from_describe(_describe("header\n" + YAML)["data"]) == (
            "header\n" + YAML
        )

    def test_missing_view_text_returns_none(self):
        assert view_text_from_describe(_describe(None)["data"]) is None

    def test_malformed_payload_returns_none(self):
        assert (
            view_text_from_describe({"result": {"data_array": [["not json"]]}}) is None
        )
        assert view_text_from_describe(None) is None

    def test_deployed_spec_starts_at_version_key(self):
        spec = deployed_spec("CREATE-OUTPUT-PREFIX\n" + YAML)
        assert spec is not None and spec["source"] == "main.sales.orders"

    def test_text_without_version_key_is_not_a_metric_view(self):
        assert deployed_spec("plain table, no metrics") is None


class TestDefinitionReport:
    def test_verified_when_live_definition_equals_yaml(self):
        report = definition_report(_deployed(), _describe("pre\n" + YAML)["data"])
        assert report["verified"] is True
        assert report["difference_count"] == 0
        assert report["measures_without_format"] == 1

    def test_differences_are_listed_with_count(self):
        drifted = YAML.replace("SUM(amount)", "SUM(old)")
        report = definition_report(_deployed(), _describe("pre\n" + drifted)["data"])
        assert report["verified"] is False
        assert report["differences"] == ["measure total_revenue (expr)"]

    def test_unreadable_definition_is_not_verified(self):
        report = definition_report(_deployed(), _describe(None)["data"])
        assert report["verified"] is None


class TestVerifyViewDefinition:
    def test_verified_status(self):
        runner_calls = []

        def run_sql(sql):
            runner_calls.append(sql)
            return _describe("pre\n" + YAML)

        result = verify_view_definition(run_sql, "main.default.fact", _deployed())
        assert result["status"] == "verified"
        assert runner_calls == ["DESCRIBE TABLE EXTENDED main.default.fact AS JSON"]

    def test_differs_status(self):
        result = verify_view_definition(
            lambda sql: _describe("pre\n" + YAML.replace("SUM(amount)", "AVG(amount)")),
            "main.default.fact",
            _deployed(),
        )
        assert result["status"] == "differs"
        assert result["differences"] == ["measure total_revenue (expr)"]

    def test_failed_describe_is_missing(self):
        result = verify_view_definition(
            lambda sql: {"success": False, "error": "TABLE_OR_VIEW_NOT_FOUND"},
            "main.default.fact",
            _deployed(),
        )
        assert result["status"] == "missing"

    def test_non_metric_view_status(self):
        result = verify_view_definition(
            lambda sql: _describe("a plain table"), "main.default.fact", _deployed()
        )
        assert result["status"] == "not_metric_view"
