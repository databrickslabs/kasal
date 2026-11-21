"""Only compared row counts verify a table, and the report says so plainly."""

from src.services.tools.metric_view_utils.row_count_check import diff_counts


def test_matching_counts_verify_and_unreadable_ones_do_not():
    assert diff_counts({"rows": [[10]]}, {"data": [{"c": 10}]}) is None
    assert diff_counts({"rows": [[10]]}, {"data": []}).startswith("unverified")
    assert diff_counts({}, {"data": [{"c": 10}]}).startswith("unverified")
