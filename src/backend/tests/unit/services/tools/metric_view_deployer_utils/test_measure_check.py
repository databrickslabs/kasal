"""Per-measure execution check (fake SQL runner, fake clock, no network)."""

from unittest.mock import MagicMock, patch

from src.services.tools.metric_view_deployer_utils import measure_check
from src.services.tools.metric_view_deployer_utils.measure_check import (
    check_view_measures,
    measure_statement,
)

VIEW = "main.default.fact_sales"


def _ok(value="1"):
    return {"success": True, "data": {"result": {"data_array": [[value]]}}}


class TestMeasureStatement:
    def test_global_statement_is_a_count_of_measure_rows(self):
        assert measure_statement(VIEW, "total_revenue", None) == (
            f"SELECT COUNT(*) FROM (SELECT MEASURE(`total_revenue`) AS m FROM {VIEW})"
        )

    def test_grouped_statement_groups_by_dimension(self):
        assert measure_statement(VIEW, "total_revenue", "region") == (
            "SELECT COUNT(*) FROM (SELECT `region`, MEASURE(`total_revenue`) AS m "
            f"FROM {VIEW} GROUP BY `region`)"
        )


class TestCheckViewMeasures:
    def test_every_measure_executes(self):
        sql_seen = []

        def run_sql(sql):
            sql_seen.append(sql)
            return _ok()

        report = check_view_measures(run_sql, VIEW, ["a", "b"])
        assert len(sql_seen) == 2
        assert report["measures"] == 2
        assert report["failing"] == 0
        assert report["per_measure"] == {"a": {"status": "ok"}, "b": {"status": "ok"}}

    def test_failing_measure_reports_truncated_error(self):
        def run_sql(sql):
            if "`bad`" in sql:
                return {"success": False, "error": "x" * 1000}
            return _ok()

        report = check_view_measures(run_sql, VIEW, ["good", "bad"])
        assert report["failing"] == 1
        bad = report["per_measure"]["bad"]
        assert bad["status"] == "fail"
        assert len(bad["error"]) <= 300

    def test_grouped_run_reports_group_count_only(self):
        report = check_view_measures(
            lambda sql: _ok("42"), VIEW, ["a"], group_by="region"
        )
        assert report["per_measure"]["a"] == {"status": "ok", "groups": 42}

    def test_timing_flags_slow_measures(self):
        clock = MagicMock()
        clock.perf_counter.side_effect = [0.0, 40.0, 100.0, 101.5]
        with patch.object(measure_check, "time", clock):
            report = check_view_measures(
                lambda sql: _ok(),
                VIEW,
                ["slow_one", "fast_one"],
                timing=True,
                slow_s=30.0,
            )
        assert report["slow"] == 1
        assert report["per_measure"]["slow_one"] == {"status": "slow", "seconds": 40.0}
        assert report["per_measure"]["fast_one"] == {"status": "ok", "seconds": 1.5}

    def test_no_timing_fields_without_timing_flag(self):
        report = check_view_measures(lambda sql: _ok(), VIEW, ["a"])
        assert "seconds" not in report["per_measure"]["a"]
