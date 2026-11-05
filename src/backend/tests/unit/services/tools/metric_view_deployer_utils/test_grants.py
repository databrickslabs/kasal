"""Direct-grant preservation around a metric view redeploy (fake SQL runner, no network)."""

import pytest

from src.services.tools.metric_view_deployer_utils.grants import (
    DirectGrant,
    GrantReadError,
    grant_statement,
    read_direct_grants,
    restore_grants,
)

VIEW = "main.default.fact_sales"


def _rows(*rows) -> dict:
    return {
        "success": True,
        "state": "SUCCEEDED",
        "data": {"result": {"data_array": [list(r) for r in rows]}},
    }


class TestReadDirectGrants:
    def test_query_targets_information_schema_with_direct_filter(self):
        sql_seen = []

        def run_sql(sql):
            sql_seen.append(sql)
            return _rows(("analysts", "SELECT"))

        read_direct_grants(run_sql, "main", "default", "fact_sales")
        assert len(sql_seen) == 1
        sql = sql_seen[0]
        assert "FROM `main`.information_schema.table_privileges" in sql
        assert "table_schema = 'default'" in sql
        assert "table_name = 'fact_sales'" in sql
        assert "inherited_from = 'NONE'" in sql

    def test_returns_sorted_grants(self):
        grants, skipped = read_direct_grants(
            lambda sql: _rows(("b-group", "SELECT"), ("a-group", "SELECT")),
            "main",
            "default",
            "fact_sales",
        )
        assert grants == [
            DirectGrant("a-group", "SELECT"),
            DirectGrant("b-group", "SELECT"),
        ]
        assert skipped == 0

    def test_ownership_and_malformed_rows_are_skipped_not_reapplied(self):
        grants, skipped = read_direct_grants(
            lambda sql: _rows(("owner", "OWNERSHIP"), ("ok", "SELECT"), ("bad",)),
            "main",
            "default",
            "fact_sales",
        )
        assert grants == [DirectGrant("ok", "SELECT")]
        assert skipped == 2

    def test_no_rows_means_no_grants(self):
        grants, skipped = read_direct_grants(
            lambda sql: {"success": True, "state": "SUCCEEDED", "data": {"result": {}}},
            "main",
            "default",
            "fact_sales",
        )
        assert grants == []
        assert skipped == 0

    def test_failed_read_raises_with_http_status(self):
        with pytest.raises(GrantReadError) as info:
            read_direct_grants(
                lambda sql: {"success": False, "http_status": 403, "error": "denied"},
                "main",
                "default",
                "fact_sales",
            )
        assert info.value.http_status == 403
        assert "denied" in str(info.value)


class TestGrantStatement:
    def test_grant_statement_quotes_grantee(self):
        statement = grant_statement(VIEW, DirectGrant("user@example.com", "SELECT"))
        assert statement == f"GRANT SELECT ON TABLE {VIEW} TO `user@example.com`"

    def test_backticks_in_grantee_are_escaped(self):
        statement = grant_statement(VIEW, DirectGrant("odd`name", "SELECT"))
        assert statement.endswith("TO `odd``name`")


class TestRestoreGrants:
    def test_reapplies_every_grant_and_confirms_same_set(self):
        before = [DirectGrant("analysts", "SELECT")]
        statements = []

        def run_sql(sql):
            statements.append(sql)
            if sql.startswith("GRANT"):
                return {"success": True}
            return _rows(("analysts", "SELECT"))

        report = restore_grants(run_sql, VIEW, "main", "default", "fact_sales", before)
        assert statements[0] == f"GRANT SELECT ON TABLE {VIEW} TO `analysts`"
        assert report == {"before": 1, "after": 1, "preserved": True, "failures": []}

    def test_failed_grant_is_reported_and_not_preserved(self):
        before = [DirectGrant("analysts", "SELECT"), DirectGrant("ops", "SELECT")]

        def run_sql(sql):
            if sql.startswith("GRANT") and "analysts" in sql:
                return {"success": False, "error": "principal not found"}
            if sql.startswith("GRANT"):
                return {"success": True}
            return _rows(("ops", "SELECT"))

        report = restore_grants(run_sql, VIEW, "main", "default", "fact_sales", before)
        assert report["preserved"] is False
        assert report["after"] == 1
        assert report["failures"] == ["SELECT to analysts: principal not found"]

    def test_count_match_with_different_members_is_not_preserved(self):
        before = [DirectGrant("analysts", "SELECT")]

        def run_sql(sql):
            if sql.startswith("GRANT"):
                return {"success": True}
            return _rows(("someone-else", "SELECT"))

        report = restore_grants(run_sql, VIEW, "main", "default", "fact_sales", before)
        assert report["preserved"] is False
        assert report["after"] == 1

    def test_failed_reread_is_not_preserved(self):
        before = [DirectGrant("analysts", "SELECT")]

        def run_sql(sql):
            if sql.startswith("GRANT"):
                return {"success": True}
            return {"success": False, "http_status": 500, "error": "boom"}

        report = restore_grants(run_sql, VIEW, "main", "default", "fact_sales", before)
        assert report["preserved"] is False
        assert report["after"] is None
        assert any("re-read" in f for f in report["failures"])
