"""Dangerous-SQL check of a metric view YAML: values are checked, review comments are not."""

from src.services.tools.metric_view_deployer_utils.yaml_safety import yaml_is_safe

_REVIEW_NOTE = """version: '1.1'
source: cat.sch.fact
measures:
  - name: sales
    expr: SUM(source.amount)
  # Untranslatable:
  #   - Cross Fact Share
  #       SOURCE-VIEW DRAFT (build this, then a UCMV on it):
  #         CREATE OR REPLACE VIEW <catalog>.<schema>.fact__unioned AS
  #           SELECT * FROM `fact_a` UNION ALL SELECT * FROM `fact_b`
  #         ;
"""


def test_a_review_draft_in_a_comment_does_not_block_the_deploy():
    assert yaml_is_safe(_REVIEW_NOTE)


def test_dangerous_sql_in_a_value_is_still_refused():
    for expr in ("SUM(x); DROP TABLE t", "GRANT SELECT ON t TO u", "CREATE OR REPLACE VIEW v AS SELECT 1"):
        text = f"version: '1.1'\nsource: cat.sch.fact\nmeasures:\n  - name: m\n    expr: {expr!r}\n"
        assert not yaml_is_safe(text), expr


def test_dangerous_sql_in_a_block_scalar_or_source_is_refused():
    block = "version: '1.1'\nsource: |-\n  SELECT 1\n  # not a YAML comment here\n  ; DROP TABLE t\nmeasures: []\n"
    assert not yaml_is_safe(block)
    keyed = "version: '1.1'\nsource: cat.sch.fact\njoins:\n  - name: d\n    on: 'a = b; DELETE FROM t'\n"
    assert not yaml_is_safe(keyed)


def test_dollar_quotes_are_refused_even_in_a_comment():
    assert not yaml_is_safe(_REVIEW_NOTE + "# $$ ends the DDL body\n")


def test_yaml_that_does_not_parse_is_checked_as_text():
    assert not yaml_is_safe("measures: [\n# DROP TABLE t\n")
    assert yaml_is_safe("measures: [\n")
