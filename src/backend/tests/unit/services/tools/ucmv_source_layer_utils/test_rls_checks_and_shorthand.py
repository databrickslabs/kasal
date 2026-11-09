"""Row-filter shorthand, the fact-needs-the-RLS-column check and value_access suggestions."""

import json

import pytest

from src.services.tools.metric_view_utils.source_layer_emitter import RowFilter
from src.services.tools.ucmv_source_layer_utils.inputs import parse_row_filters
from src.services.tools.ucmv_source_layer_utils.rls_checks import (
    Relationship,
    fact_rls_findings,
    parse_relationships,
    relationships_from_scope,
    rls_suggestion,
    value_access_field,
)
from src.services.tools.ucmv_source_layer_utils.view_specs import (
    emit_source_layer,
    rls_review_hints,
)

ENTRIES = json.dumps(
    [
        {"table": "Dim", "source_table": "raw.sales.dim"},
        {"table": "Fact", "source_table": "raw.sales.fact"},
    ]
)
REL = Relationship("Fact", "dim_key", "Dim", "dim_key")


def test_value_access_shorthand_expands_to_access_function():
    filters = parse_row_filters(
        json.dumps(
            {"Dim": {"value_access": {"field": "company_code", "column": "comp_code"}}}
        ),
        "cat.common",
    )
    assert filters["Dim"] == RowFilter(
        function="cat.common.value_access",
        columns=["comp_code"],
        constants=["company_code"],
    )


def test_object_access_shorthand_and_list_form_and_default_key():
    filters = parse_row_filters(
        json.dumps(
            {
                "*": {"object_access": ["grp_a", " grp_b "]},
                "Fact": {"object_access": ""},
            }
        ),
        "cat.common",
    )
    assert filters["*"].function == "cat.common.object_access"
    assert filters["*"].constants == ["grp_a;grp_b"]
    assert filters["Fact"].constants == [""]


def test_explicit_form_still_works():
    filters = parse_row_filters(
        json.dumps(
            {"Fact": {"function": "main.gov.fn", "columns": ["c"], "constants": ["x"]}}
        )
    )
    assert filters["Fact"] == RowFilter("main.gov.fn", ["c"], ["x"])


@pytest.mark.parametrize(
    "spec, schema, message",
    [
        (
            {"value_access": {"field": "f", "column": "c"}},
            None,
            "access_schema must be catalog.schema",
        ),
        ({"value_access": {"field": "f"}}, "cat.common", "needs both field and column"),
        ({"value_access": "f"}, "cat.common", "must be an object"),
        ({"object_access": 5}, "cat.common", "object_access must be"),
        (
            {"object_access": "g", "function": "x.y.z"},
            "cat.common",
            "cannot be combined",
        ),
        (
            {"value_access": {"field": "f", "column": "c"}, "object_access": "g"},
            "cat.common",
            "exactly one",
        ),
        ({"object_access": "g" * 300}, "cat.common", "255"),
    ],
)
def test_shorthand_errors_name_the_fix(spec, schema, message):
    with pytest.raises(ValueError, match=message):
        parse_row_filters(json.dumps({"Fact": spec}), schema)


def test_relationships_json_and_scope_relationship_rows_are_parsed():
    rels = parse_relationships(
        json.dumps(
            [
                {
                    "from_table": "Fact",
                    "from_column": "k",
                    "to_table": "Dim",
                    "to_column": "k",
                }
            ]
        )
    )
    assert rels == [Relationship("Fact", "k", "Dim", "k")]
    scope = [
        {
            "object_type": "relationship",
            "pbi_table": "Fact",
            "name": "Fact[k] -> Dim[k]",
            "decision": "active",
        },
        {
            "object_type": "relationship",
            "pbi_table": "Fact",
            "name": "Fact[x] -> Dim[x]",
            "decision": "inactive",
        },
    ]
    assert relationships_from_scope(scope) == [Relationship("Fact", "k", "Dim", "k")]
    with pytest.raises(ValueError, match="needs from_table"):
        parse_relationships(json.dumps([{"from_table": "Fact"}]))


def test_fact_without_the_same_field_is_warned_and_with_it_is_not():
    dim_filter = {"Dim": RowFilter("cat.c.value_access", ["cc"], ["company_code"])}
    assert fact_rls_findings([REL], dim_filter, {"Dim", "Fact"}) == [
        "Fact is not filtered by company_code: a filter on Dim does not reach it; "
        "add the column to the fact MV and a value_access filter"
    ]
    both = dict(
        dim_filter, Fact=RowFilter("cat.c.value_access", ["cc"], ["Company_Code"])
    )
    assert fact_rls_findings([REL], both, {"Dim", "Fact"}) == []


def test_default_star_filter_counts_for_the_fact_and_missing_fact_is_skipped():
    star = {"*": RowFilter("cat.c.value_access", ["cc"], ["company_code"])}
    assert fact_rls_findings([REL], star, {"Dim", "Fact"}) == []
    assert fact_rls_findings([REL], star, {"Dim"}) == []  # fact not in this layer
    assert value_access_field(RowFilter("cat.c.object_access", [], ["g"])) is None


def test_emit_source_layer_returns_warnings_from_scope_and_json():
    scope = json.dumps(
        [
            {
                "object_type": "relationship",
                "pbi_table": "Fact",
                "name": "Fact[dim_key] -> Dim[dim_key]",
                "decision": "active",
            }
        ]
    )
    out = emit_source_layer(
        entries_json=ENTRIES,
        catalog="cat",
        schema="sch",
        row_filters_json=json.dumps(
            {"Dim": {"value_access": {"field": "company_code", "column": "cc"}}}
        ),
        materialized=True,
        scope_json=scope,
        access_schema="cat.common",
    )
    assert len(out["rls_warnings"]) == 1
    assert out["rls_warnings"][0].startswith("Fact is not filtered by company_code")


@pytest.mark.parametrize(
    "dax, match, values",
    [
        ('Dim[country] = "DE"', "equals", ["DE"]),
        ("'Dim'[site_id] = 42", "equals", ["42"]),
        ('Dim[country] IN {"DE", "AT"}', "in", ["DE", "AT"]),
        ("Dim[email] = USERPRINCIPALNAME()", "user", []),
        ('LOOKUPVALUE(Dim[x], Dim[y], "a") = 1', None, None),
        ('Dim[country] IN {"DE", Dim[other]}', None, None),
    ],
)
def test_simple_rls_expressions_become_suggestions_others_do_not(dax, match, values):
    suggestion = rls_suggestion(dax, "Sales Reader")
    if match is None:
        assert suggestion is None
        return
    assert suggestion["match"] == match
    assert suggestion["values"] == values
    assert (
        suggestion["row_filter"]["value_access"]["field"]
        == suggestion["row_filter"]["value_access"]["column"]
    )


def test_rls_hint_carries_suggestion_from_row_or_dax_map_only_when_dax_known():
    rows = [
        {
            "object_type": "rls_filter",
            "pbi_table": "Dim",
            "name": "Reader",
            "reason": "r",
        },
        {
            "object_type": "rls_filter",
            "pbi_table": "Fact",
            "name": "Reader",
            "reason": "r",
        },
    ]
    hints = rls_review_hints(rows, {}, {"Dim": 'Dim[country] = "DE"'})
    assert hints[0]["suggestion"]["values"] == ["DE"]
    assert (
        hints[0]["suggestion"]["access_rows_template"][0]["reason"]
        == "Power BI role Reader"
    )
    assert hints[1]["suggestion"] is None
