"""Entries -> DDL with row filters, RLS hints, and dependency-safe deploy order."""

import pytest

from src.services.tools.ucmv_source_layer_utils.deploy_order import deploy_order
from src.services.tools.ucmv_source_layer_utils.view_specs import (
    emit_source_layer,
    parse_view_entries,
)

ENTRIES = """[
  {"table": "Dim Country", "source_table": "raw.sales.countries",
   "calc_columns": [{"name": "label", "expression": "[name] & \\"x\\""}]},
  {"table": "Fact Sales", "native_sql": "SELECT * FROM raw.sales.orders",
   "calc_columns": [{"name": "net", "sql": "amount - discount"}]}
]"""
ROW_FILTERS = (
    '{"Fact Sales": {"function": "main.gov.company_fn", "columns": ["company_code"],'
    ' "constants": ["raw.sales.orders=obj"]}}'
)


def test_emit_builds_one_view_per_entry_with_default_target_names():
    out = emit_source_layer(
        entries_json=ENTRIES,
        catalog="cat",
        schema="sch",
        row_filters_json=None,
        materialized=True,
        scope_json=None,
    )
    views = out["views"]
    assert views["Dim Country"]["target"] == "`cat`.`sch`.`src_dim_country`"
    assert "FROM `raw`.`sales`.`countries`" in views["Dim Country"]["ddl"]
    assert any("label" in step for step in views["Dim Country"]["todo_steps"])
    assert views["Fact Sales"]["row_filter"] is None


def test_row_filter_attaches_to_its_table_only_and_summarises_without_values():
    out = emit_source_layer(
        entries_json=ENTRIES,
        catalog="cat",
        schema="sch",
        row_filters_json=ROW_FILTERS,
        materialized=True,
        scope_json=None,
    )
    fact = out["views"]["Fact Sales"]
    assert (
        "WITH ROW FILTER `main`.`gov`.`company_fn` ON ('raw.sales.orders=obj', `company_code`)"
        in fact["ddl"]
    )
    assert fact["row_filter"] == {
        "function": "main.gov.company_fn",
        "columns": ["company_code"],
        "constant_count": 1,
    }
    assert "raw.sales.orders=obj" not in str(out["views"]["Dim Country"]["row_filter"])
    assert "ROW FILTER" not in out["views"]["Dim Country"]["ddl"]


def test_calc_column_with_translated_sql_is_emitted_not_todo():
    out = emit_source_layer(
        entries_json=ENTRIES,
        catalog="cat",
        schema="sch",
        row_filters_json=None,
        materialized=True,
        scope_json=None,
    )
    fact = out["views"]["Fact Sales"]
    assert "amount - discount AS net" in fact["ddl"]
    assert not any("net" in step for step in fact["todo_steps"])


def test_non_materialized_emit_with_row_filter_fails_only_that_view():
    out = emit_source_layer(
        entries_json=ENTRIES,
        catalog="cat",
        schema="sch",
        row_filters_json=ROW_FILTERS,
        materialized=False,
        scope_json=None,
    )
    assert out["views"]["Fact Sales"]["error"]
    assert out["views"]["Dim Country"]["error"] is None


def test_rls_rows_become_hints_not_filters():
    scope = (
        "object_type,pbi_table,name,reason,decision\n"
        "rls_filter,Dim Country,EU reader,row-level security in 1 role,review\n"
        "rls_filter,Fact Sales,EU reader,row-level security,review\n"
        "measure,Fact Sales,Revenue,,keep\n"
    )
    out = emit_source_layer(
        entries_json=ENTRIES,
        catalog="cat",
        schema="sch",
        row_filters_json=ROW_FILTERS,
        materialized=True,
        scope_json=scope,
    )
    hints = out["rls_hints"]
    assert [h["pbi_table"] for h in hints] == ["Dim Country", "Fact Sales"]
    assert hints[0]["row_filter_configured"] is False
    assert hints[1]["row_filter_configured"] is True
    assert "ROW FILTER" not in out["views"]["Dim Country"]["ddl"]


@pytest.mark.parametrize(
    "raw, message",
    [
        ('[{"source_table": "a.b.c"}]', "needs a 'table'"),
        (
            '[{"table": "T", "source_table": "a.b.c", "native_sql": "SELECT 1"}]',
            "exactly one",
        ),
        ('[{"table": "T"}]', "exactly one"),
        ('[{"table": "T", "source_table": "bad"}]', "catalog.schema.table"),
        (
            '[{"table": "T", "source_table": "a.b.c"}, {"table": "T", "source_table": "a.b.c"}]',
            "listed twice",
        ),
    ],
)
def test_entry_validation_messages(raw, message):
    with pytest.raises(ValueError, match=message):
        parse_view_entries(raw)


def test_deploy_order_puts_referenced_views_first():
    views = {
        "fact": (
            "`c`.`s`.`src_fact`",
            "CREATE VIEW `c`.`s`.`src_fact` AS SELECT * FROM `c`.`s`.`src_dim`",
        ),
        "dim": (
            "`c`.`s`.`src_dim`",
            "CREATE VIEW `c`.`s`.`src_dim` AS SELECT * FROM raw.x.y",
        ),
    }
    assert deploy_order(views) == ["dim", "fact"]


def test_deploy_order_does_not_mistake_prefix_names():
    views = {
        "a": ("`c`.`s`.`src_a`", "AS SELECT * FROM raw.x.y"),
        "ab": ("`c`.`s`.`src_ab`", "AS SELECT * FROM `c`.`s`.`src_a2`"),
    }
    assert deploy_order(views) == ["a", "ab"]


def test_deploy_order_detects_cycles():
    views = {
        "a": ("`c`.`s`.`src_a`", "AS SELECT * FROM `c`.`s`.`src_b`"),
        "b": ("`c`.`s`.`src_b`", "AS SELECT * FROM `c`.`s`.`src_a`"),
    }
    with pytest.raises(ValueError, match="circular"):
        deploy_order(views)
