"""Only compared row counts verify a table, and the report says so plainly."""

from src.services.tools.metric_view_utils.row_count_check import diff_counts
from src.services.tools.mquery_conversion_pipeline_tool import (
    MqueryConversionPipelineTool,
)


def test_matching_counts_verify_and_unreadable_ones_do_not():
    assert diff_counts({"rows": [[10]]}, {"data": [{"c": 10}]}) is None
    assert diff_counts({"rows": [[10]]}, {"data": []}).startswith("unverified")
    assert diff_counts({}, {"data": [{"c": 10}]}).startswith("unverified")


def test_unverified_row_shows_only_the_warning():
    report = MqueryConversionPipelineTool()._format_validation_report(
        [{"table": "orders", "status": "unverified", "error": "unverified: x"}],
        [],
        [],
        "main.default",
    )
    line = next(row for row in report.splitlines() if "`orders`" in row)
    assert "NOT VERIFIED" in line
    assert "❌" not in line
    assert "0 of 1 Databricks tables verified" in report
