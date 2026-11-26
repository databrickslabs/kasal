"""SQL text helpers and the deployed-view catalog, with no warehouse."""

import json

import pytest

from src.services.tools.genie_space_sync_utils.metric_views import (
    ViewCatalog,
    describe_statement,
    parse_describe,
)
from src.services.tools.genie_space_sync_utils.sql_checks import (
    count_statement,
    percent_scaling,
    quote_name,
    referenced_tables,
    snippet_statement,
    strip_statement,
)

from ._fakes import (
    FakeWarehouse,
    metric_view_describe,
    plain_table_describe,
    sample_tables,
)


def test_count_statement_strips_semicolons_and_closes_on_its_own_line():
    wrapped = count_statement("SELECT 1;  ;")
    assert wrapped == "SELECT COUNT(*) AS n FROM (\nSELECT 1\n)"
    commented = count_statement("SELECT 1\n-- trailing note  ")
    assert commented.endswith("-- trailing note\n)")


def test_strip_statement_removes_repeated_semicolons():
    assert strip_statement("  SELECT 1 ;  ;") == "SELECT 1"


def test_quote_name_accepts_three_part_names_only():
    assert quote_name("main.sales.orders") == "`main`.`sales`.`orders`"
    for bad in ("main.sales", "a.b.c.d", "main.sales.orders; DROP", "main..x"):
        with pytest.raises(ValueError):
            quote_name(bad)


def test_referenced_tables_drops_a_column_part_and_lowercases():
    sql = "SELECT `Main`.`Sales`.`Orders`.region, x FROM main.sales.regions r"
    assert referenced_tables(sql) == {"main.sales.orders", "main.sales.regions"}


def test_snippet_statement_targets_the_view_its_columns_are_qualified_with():
    tables = ["main.sales.regions", "main.sales.orders"]
    statement = snippet_statement("filters", "orders.region = 'EU'", tables)
    assert statement is not None
    assert "FROM `main`.`sales`.`orders` AS orders" in statement
    assert "WHERE orders.region = 'EU'" in statement


def test_snippet_statement_without_qualifier_uses_the_first_table():
    statement = snippet_statement(
        "expressions", "upper(region)", ["main.sales.regions"]
    )
    assert statement is not None
    assert "FROM `main`.`sales`.`regions`" in statement


def test_snippet_statement_refuses_unknown_kinds_and_empty_spaces():
    assert snippet_statement("widgets", "1", ["main.sales.regions"]) is None
    with pytest.raises(ValueError):
        snippet_statement("measures", "COUNT(1)", [])


PCT = frozenset({"fill_pct"})


def test_percent_scaling_flags_a_scaled_measure_call_directly():
    direct, computed = percent_scaling("SELECT MEASURE(fill_pct) * 100 FROM v", PCT)
    assert direct == ["fill_pct"]
    assert computed == []
    direct, _ = percent_scaling("SELECT 100 * MEASURE(`fill_pct`) FROM v", PCT)
    assert direct == ["fill_pct"]


def test_percent_scaling_treats_a_same_line_expression_as_review_only():
    sql = "SELECT COALESCE(MEASURE(fill_pct), 0) * 100 AS fill_pct_pp FROM v"
    direct, computed = percent_scaling(sql, PCT)
    assert direct == []
    assert computed == ["fill_pct"]


def test_percent_scaling_ignores_other_multipliers_and_other_measures():
    assert percent_scaling("SELECT MEASURE(fill_pct) * 1000 FROM v", PCT) == ([], [])
    assert percent_scaling("SELECT MEASURE(count_x) * 100 FROM v", PCT) == ([], [])


def test_parse_describe_reads_metric_view_dimensions_measures_and_percentages():
    info = parse_describe("main.sales.orders", metric_view_describe())

    assert info.is_metric_view
    assert info.dimensions == {"region"}
    assert info.measures == {"order_fill_rate_pct", "order_count"}
    assert info.percent_measures == {"order_fill_rate_pct"}
    assert "region" in info.columns and "order_count" in info.columns


def test_parse_describe_of_a_plain_table_has_no_measures():
    info = parse_describe(
        "main.sales.regions", plain_table_describe(["region", "name"])
    )
    assert not info.is_metric_view
    assert info.measures == frozenset()
    assert info.dimensions == {"region", "name"}


@pytest.mark.parametrize(
    "text",
    [
        "not json",
        json.dumps({"columns": [], "view_text": "version: 1\nmeasures: [\n"}),
        json.dumps(["a", "list"]),
    ],
)
def test_parse_describe_refuses_malformed_documents(text):
    with pytest.raises(ValueError):
        parse_describe("main.sales.orders", text)


def test_describe_statement_is_the_extended_json_form():
    assert describe_statement("main.sales.orders") == (
        "DESCRIBE TABLE EXTENDED `main`.`sales`.`orders` AS JSON"
    )


def test_catalog_loads_each_view_once_and_records_failures():
    warehouse = FakeWarehouse(sample_tables())
    catalog = ViewCatalog(warehouse)

    assert catalog.view("main.sales.orders") is not None
    catalog.view("main.sales.orders")
    assert len(warehouse.statements) == 1

    assert catalog.view("main.sales.gone") is None
    assert "TABLE_OR_VIEW_NOT_FOUND" in catalog.errors["main.sales.gone"]


def test_catalog_rejects_a_bad_identifier_without_running_sql():
    warehouse = FakeWarehouse(sample_tables())
    catalog = ViewCatalog(warehouse)

    assert catalog.view("not-three-parts") is None
    assert warehouse.statements == []
    assert "catalog.schema.name" in catalog.errors["not-three-parts"]


def test_catalog_aggregates_percentages_and_known_names():
    catalog = ViewCatalog(FakeWarehouse(sample_tables()))
    tables = ["main.sales.orders", "main.sales.regions"]

    assert catalog.percent_measures(tables) == {"order_fill_rate_pct"}
    known = catalog.known_names(tables)
    assert {"orders", "regions", "order_count", "region", "name"} <= known
