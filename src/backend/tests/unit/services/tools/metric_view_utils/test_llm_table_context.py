"""The table block the DAX LLM reads names the columns the source view really has."""

from types import SimpleNamespace

from src.services.tools.metric_view_utils.table_processor import _fact_table_context


def _table():
    # The Power Query selects ``inner_code AS code``: the parser's aggregate columns carry the
    # inner name, the deployed view only has ``code``.
    return SimpleNamespace(
        aggregate_columns=[{"name": "amount", "source_col": "amount"}, {"name": "inner_code"}],
        group_by_columns=["region"],
        dim_source_tables={},
        static_filters=[],
    )


def test_the_real_view_columns_replace_the_parsed_ones():
    text = _fact_table_context(_table(), "c.s.src_fact", [], ["amount", "code", "region"])
    assert "the ONLY names valid as source.<col>): amount, code, region" in text
    assert "inner_code" not in text


def test_without_known_columns_the_parsed_ones_are_listed_as_before():
    text = _fact_table_context(_table(), "c.s.fact", [])
    assert "- fact source columns (use as source.<col>): amount, inner_code" in text
