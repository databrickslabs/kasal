"""Power Query column expressions (Table.AddColumn) and keyed Table.Distinct in the source layer."""

import pytest

from src.services.tools.metric_view_utils.m_column_expressions import (
    m_column_expression_to_sql as to_sql,
)
from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder


@pytest.mark.parametrize(
    "m, sql",
    [
        (
            'each if Text.Length(Text.From([code])) = 3 then "0" & Text.From([code]) else Text.From([code])',
            "CASE WHEN (length(string(code)) = 3) THEN concat('0', string(code)) ELSE string(code) END",
        ),
        ('each [a] & "-" & [b]', "concat(concat(a, '-'), b)"),
        (
            'each if [x] = null then "n/a" else Text.Upper([x])',
            "CASE WHEN (x IS NULL) THEN 'n/a' ELSE upper(x) END",
        ),
        ("each [x] <> null", "(x IS NOT NULL)"),
        (
            'each if [a] > 0 and not ([b] = "X") then 1 else 0',
            "CASE WHEN ((a > 0) AND (NOT ((b = 'X')))) THEN 1 ELSE 0 END",
        ),
        ('each Text.PadStart(Text.From([c]), 4, "0")', "lpad(string(c), 4, '0')"),
        ('each "it\'s"', "'it''s'"),
    ],
)
def test_supported_expressions(m, sql):
    assert to_sql(m) == sql


@pytest.mark.parametrize(
    "m",
    [
        "each Date.Year([d])",
        "each List.Sum({1, 2})",
        'each [a b] & "x"',
        "each if [a] then 1",
        "",
    ],
)
def test_anything_else_is_not_translated(m):
    assert to_sql(m) is None


MQUERY = """let
    Source = Value.NativeQuery(conn, "SELECT * from md.ca_dim_company", null, [EnableFolding=true]),
    #"Added Custom" = Table.AddColumn(Source, "comp_code", each
        if Text.Length(Text.From([co_code_bw])) = 3 then "0" & Text.From([co_code_bw]) else Text.From([co_code_bw]),
        type text),
    #"Removed Columns" = Table.RemoveColumns(#"Added Custom",{"region_sequence_id", "region"}),
    #"Removed Duplicates" = Table.Distinct(#"Removed Columns", {"comp_code"})
in
    #"Removed Duplicates"
"""


def test_a_derived_key_dimension_folds_completely():
    folder = MTransformFolder()
    sql = folder.fold_from_mquery(
        "SELECT * FROM `p`.`md`.`ca_dim_company`", MQUERY, None
    )
    assert (
        "* EXCEPT (region, region_sequence_id)" in sql
    )  # the new column is not in EXCEPT
    assert "END AS comp_code" in sql
    assert "PARTITION BY comp_code" in sql and sql.rstrip().endswith(
        "WHERE _kasal_rn = 1"
    )
    assert folder.transform_todos == []


def test_an_untranslatable_added_column_is_a_todo_not_dropped():
    mquery = MQUERY.replace(
        "each\n        if Text.Length", "each Date.Year([d]) + 0 * Text.Length"
    )
    folder = MTransformFolder()
    folder.fold_from_mquery("SELECT * FROM t", mquery, None)
    assert any("Table.AddColumn" in todo for todo in folder.transform_todos)
