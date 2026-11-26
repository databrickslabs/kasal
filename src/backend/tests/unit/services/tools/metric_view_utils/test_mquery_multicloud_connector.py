"""Databricks on AWS/GCP is read in Power BI through ``DatabricksMultiCloud.Catalogs``.

Every pattern that knew ``Databricks.Catalogs`` must know it too; before, those tables came back as
"unrecognized M source shape" with no source table and dropped out of the conversion.
"""

import pytest

from src.services.powerbi.scope import tmdl_model
from src.services.tools.metric_view_utils.mquery_parser import (
    classify_mquery_source,
    extract_source_table,
    looks_like_raw_mquery,
    resolve_mquery_to_sql,
)
from src.services.tools.metric_view_utils.mquery_steps import classify_m_steps

CONNECTORS = ["Databricks.Catalogs", "DatabricksMultiCloud.Catalogs"]


def _navigation_m(connector: str) -> str:
    return (
        "let\n"
        f'    Source = {connector}("h.cloud.databricks.com", "/sql/1.0/warehouses/abc", '
        "[Catalog=null, Database=null]),\n"
        '    c = Source{[Name="cat",Kind="Database"]}[Data],\n'
        '    s = c{[Name="sch",Kind="Schema"]}[Data],\n'
        '    t = s{[Name="tbl",Kind="Table"]}[Data]\n'
        "in\n"
        "    t"
    )


@pytest.mark.parametrize("connector", CONNECTORS)
def test_the_source_table_is_found(connector):
    m = _navigation_m(connector)
    assert classify_mquery_source(m)[0] == "extractable"
    assert extract_source_table(m) == "cat.sch.tbl"
    assert resolve_mquery_to_sql(m) == "SELECT * FROM cat.sch.tbl"


@pytest.mark.parametrize("connector", CONNECTORS)
def test_every_step_is_reproduced(connector):
    _, unhandled = classify_m_steps(_navigation_m(connector))
    assert unhandled == []


@pytest.mark.parametrize("connector", CONNECTORS)
def test_it_counts_as_raw_m(connector):
    assert looks_like_raw_mquery(f'{connector}("h", "p")')


@pytest.mark.parametrize("connector", CONNECTORS + ["DatabricksMultiCloud.Query"])
def test_the_scope_review_classes_it_as_databricks(connector):
    assert tmdl_model._DATABRICKS.search(f'Source = {connector}("h", "p")')
