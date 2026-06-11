"""#54: harden the measure-expression SQL denylist (defense-in-depth)."""
from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import _check_dangerous_sql


class TestCheckDangerousSql:
    def test_allows_legitimate_measures(self):
        assert _check_dangerous_sql("SUM(source.amount)") is True
        assert _check_dangerous_sql("COUNT(DISTINCT source.id)") is True
        assert _check_dangerous_sql("") is True

    def test_blocks_dollar_quote_and_ddl(self):
        assert _check_dangerous_sql("x $$ y") is False           # dollar-quote breakout
        assert _check_dangerous_sql("CREATE TABLE evil(x INT)") is False
        assert _check_dangerous_sql("CREATE OR REPLACE VIEW v AS SELECT 1") is False
        assert _check_dangerous_sql("1; DROP TABLE x") is False
        assert _check_dangerous_sql("1; CREATE TABLE y(z INT)") is False
        assert _check_dangerous_sql("DELETE FROM t") is False
