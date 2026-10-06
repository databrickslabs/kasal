"""Audit of Power Query M transformation steps that may not reach the SQL."""

from src.services.tools.metric_view_utils.mquery_transform_audit import (
    audit_mquery_transformations,
)


def test_flags_filter_group_and_join():
    m = (
        'let Source=Sql.Database("s","d"), '
        "F=Table.SelectRows(Source, each [x]>0), "
        'G=Table.Group(F,{"k"},{{"t",each List.Sum([v])}}), '
        'J=Table.NestedJoin(G,{"k"},Dim,{"k"},"d") in J'
    )
    labels = audit_mquery_transformations(m)
    joined = " ".join(labels)
    assert "Row filter" in joined
    assert "Aggregation" in joined
    assert "Join" in joined


def test_plain_sql_is_not_audited():
    assert audit_mquery_transformations("SELECT * FROM cat.sch.t") == []


def test_embedded_native_query_is_preserved_so_not_flagged():
    # The native query carries its own WHERE/GROUP BY verbatim into the SQL.
    m = 'let Source=Sql.Database("s","d",[Query="SELECT a FROM t WHERE b>0 GROUP BY a"]) in Source'
    assert audit_mquery_transformations(m) == []


def test_plain_table_read_has_no_transforms():
    m = 'let Source=Databricks.Catalogs(), T=Source{[Name="t"]}[Data] in T'
    assert audit_mquery_transformations(m) == []


def test_empty_or_non_string_is_safe():
    assert audit_mquery_transformations("") == []
    assert audit_mquery_transformations(None) == []  # type: ignore[arg-type]


def test_union_and_added_column_flagged():
    m = "let A=Table.Combine({T1,T2}), " 'B=Table.AddColumn(A,"c",each [x]+[y]) in B'
    labels = audit_mquery_transformations(m)
    joined = " ".join(labels)
    assert "Union" in joined
    assert "Computed/derived column" in joined
