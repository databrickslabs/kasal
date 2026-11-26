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


NO_INFO_SCHEMA = {
    "success": False,
    "error": "[TABLE_OR_VIEW_NOT_FOUND] The table or view `cat`.`information_schema`.`table_privileges` cannot be found.",
}


class TestCatalogWithoutInformationSchema:
    """Seen in a customer catalog: every redeploy was refused because the grants could not be read."""

    def test_a_view_that_does_not_exist_yet_has_nothing_to_keep(self):
        sql_seen = []

        def run_sql(sql):
            sql_seen.append(sql)
            if "information_schema" in sql:
                return NO_INFO_SCHEMA
            return {
                "success": False,
                "error": "[TABLE_OR_VIEW_NOT_FOUND] `cat`.`sch`.`v` cannot be found.",
            }

        assert read_direct_grants(run_sql, "cat", "sch", "v") == ([], 0)
        assert sql_seen[1] == "SHOW GRANTS ON TABLE `cat`.`sch`.`v`"

    def test_show_grants_on_a_missing_view_answers_with_another_error_class(self):
        """The exact answer from the customer workspace."""

        def run_sql(sql):
            if "information_schema" in sql:
                return NO_INFO_SCHEMA
            return {
                "success": False,
                "error": "[RequestId=00000000 ErrorClass=TABLE_DOES_NOT_EXIST.RESOURCE_DOES_NOT_EXIST] "
                "Table 'cat.sch.v' does not exist.",
            }

        assert read_direct_grants(run_sql, "cat", "sch", "v") == ([], 0)

    def test_an_existing_view_keeps_its_own_grants_not_the_inherited_ones(self):
        def run_sql(sql):
            if "information_schema" in sql:
                return NO_INFO_SCHEMA
            return _rows(
                ("analysts", "SELECT", "TABLE", "cat.sch.v"),
                ("owners", "OWNERSHIP", "TABLE", "cat.sch.v"),
                ("everyone", "USE SCHEMA", "SCHEMA", "cat.sch"),
                ("readers", "SELECT", "CATALOG", "cat"),
            )

        grants, skipped = read_direct_grants(run_sql, "cat", "sch", "v")
        assert grants == [DirectGrant("analysts", "SELECT")]
        assert skipped == 1  # OWNERSHIP is never re-applied

    def test_when_show_grants_fails_too_the_redeploy_is_still_refused(self):
        def run_sql(sql):
            if "information_schema" in sql:
                return NO_INFO_SCHEMA
            return {
                "success": False,
                "error": "PERMISSION_DENIED: no MANAGE on cat.sch.v",
            }

        with pytest.raises(GrantReadError, match="PERMISSION_DENIED"):
            read_direct_grants(run_sql, "cat", "sch", "v")

    def test_other_read_errors_still_refuse_the_redeploy(self):
        def run_sql(sql):
            return {
                "success": False,
                "error": "PERMISSION_DENIED on information_schema",
            }

        with pytest.raises(GrantReadError):
            read_direct_grants(run_sql, "cat", "sch", "v")


class TestHyphenatedCatalog:
    """Unity Catalog allows hyphens (``acme-dev-catalog``); the deployer turned them into
    underscores and deployed into a catalog that does not exist."""

    def test_names_keep_their_hyphens_and_are_quoted_in_sql(self):
        from src.services.tools.metric_view_deployer_tool import _view_parts
        from src.services.tools.metric_view_deployer_utils.sql_helpers import view_fqn

        cat, sch, view = _view_parts("Fact_Sales", "acme-dev-catalog", "team_sandbox")
        assert (cat, sch, view) == ("acme-dev-catalog", "team_sandbox", "fact_sales")
        assert view_fqn(cat, sch, view) == "`acme-dev-catalog`.team_sandbox.fact_sales"
        assert view_fqn("main", "default", "fact_sales") == "main.default.fact_sales"

    def test_other_characters_are_still_replaced(self):
        from src.services.tools.metric_view_deployer_tool import _view_parts

        assert _view_parts("x", "cat; DROP", "s`c")[:2] == ("cat__DROP", "s_c")
