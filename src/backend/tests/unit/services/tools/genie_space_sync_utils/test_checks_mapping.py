"""Reconciliation mapping checks against deployed metric views."""

import json

import pytest

from src.services.tools.genie_space_sync_utils.checks_mapping import (
    check_mappings,
    parse_mapping_set,
)
from src.services.tools.genie_space_sync_utils.metric_views import ViewCatalog
from src.services.tools.genie_space_sync_utils.verdicts import FAIL, PASS, REVIEW

from ._fakes import FakeWarehouse, plain_table_describe

VIEW_WITH_TIME = json.dumps(
    {
        "columns": [
            {"name": "region"},
            {"name": "order_month"},
            {"name": "order_count", "is_measure": True},
        ],
        "view_text": (
            "version: 1.1\nsource: main.sales.orders\n"
            "dimensions:\n  - name: region\n    expr: source.region\n"
            "  - name: order_month\n    expr: source.month\n"
            "measures:\n  - name: order_count\n    expr: COUNT(1)\n"
        ),
    }
)


def _mapping(table="main.sales.orders", measure="order_count", column="region"):
    return f"""binding:
  ucmv_table: {table}
  pbi_semantic_model_id: model-1
  pbi_fact_table: Orders
  default_dimension: region
dimensions:
  region:
    ucmv_column: {column}
    pbi_dim_table: Regions
    pbi_dim_column: Region
time_dimension:
  grain: month
  pbi_column: Calendar[Date]
  ucmv:
    mode: column
    name: order_month
measures:
  - ucmv_measure: {measure}
    pbi_kind: direct
    pbi_measure: Order Count
"""


def _warehouse(extra=None):
    tables = {
        "main.sales.orders": VIEW_WITH_TIME,
        "main.sales.regions": plain_table_describe(["region"]),
    }
    tables.update(extra or {})
    return FakeWarehouse(tables)


def _catalog(extra=None):
    return ViewCatalog(_warehouse(extra))


def test_a_mapping_matching_its_view_passes():
    verdicts = check_mappings(
        {"orders.yml": _mapping()}, _catalog(), ["main.sales.orders"]
    )
    assert [v.status for v in verdicts] == [PASS]


def test_a_mapped_measure_missing_from_the_view_fails():
    verdicts = check_mappings(
        {"orders.yml": _mapping(measure="renamed_measure")}, _catalog(), []
    )
    assert verdicts[0].status == FAIL
    assert "renamed_measure" in verdicts[0].detail


def test_a_mapped_dimension_missing_from_the_view_fails():
    verdicts = check_mappings({"orders.yml": _mapping(column="gone")}, _catalog(), [])
    assert verdicts[0].status == FAIL
    assert "gone" in verdicts[0].detail


def test_a_mapping_to_a_plain_table_fails_as_not_a_metric_view():
    verdicts = check_mappings(
        {"regions.yml": _mapping(table="main.sales.regions")}, _catalog(), []
    )
    assert verdicts[0].status == FAIL
    assert "not a metric view" in verdicts[0].detail


def test_a_mapping_to_an_unreadable_view_fails_with_the_reason():
    verdicts = check_mappings(
        {"ghost.yml": _mapping(table="main.sales.ghost")}, _catalog(), []
    )
    assert verdicts[0].status == FAIL
    assert "not readable" in verdicts[0].detail


def test_malformed_mapping_yaml_fails_without_reading_anything():
    warehouse = _warehouse()
    verdicts = check_mappings(
        {"bad.yml": "just: a string\n- oops"}, ViewCatalog(warehouse), []
    )
    assert verdicts[0].status == FAIL
    assert warehouse.statements == []


def test_a_path_is_never_read_as_a_mapping():
    warehouse = _warehouse()
    verdicts = check_mappings({"x": "/etc/passwd.yaml"}, ViewCatalog(warehouse), [])
    assert verdicts[0].status == FAIL
    assert warehouse.statements == []


def test_a_metric_view_in_the_space_without_a_mapping_is_a_review_hint():
    verdicts = check_mappings(
        {"orders.yml": _mapping()}, _catalog(), ["main.sales.orders"]
    )
    assert all(v.status != REVIEW for v in verdicts)

    verdicts = check_mappings(
        {"regions.yml": _mapping(table="main.sales.regions")},
        _catalog(),
        ["main.sales.orders"],
    )
    coverage = [v for v in verdicts if v.check == "mapping_coverage"]
    assert [v.target for v in coverage] == ["main.sales.orders"]
    assert coverage[0].status == REVIEW


def test_no_mappings_means_no_mapping_verdicts():
    assert check_mappings({}, _catalog(), ["main.sales.orders"]) == []


def test_parse_mapping_set_accepts_a_json_object_and_nothing_else():
    assert parse_mapping_set(json.dumps({"a.yml": "x: 1"})) == {"a.yml": "x: 1"}
    assert parse_mapping_set(None) == {}
    assert parse_mapping_set("") == {}
    with pytest.raises(ValueError):
        parse_mapping_set(json.dumps(["a.yml"]))
    with pytest.raises(ValueError):
        parse_mapping_set(json.dumps({"a.yml": 3}))
