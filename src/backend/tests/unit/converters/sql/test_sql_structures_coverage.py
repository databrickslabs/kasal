"""
Coverage tests for src/converters/services/sql/helpers/sql_structures.py
Targets missing lines: 69, 104-105, 134-145, 191, 256-257, 259, 266-269, 293,
399-400, 421, 447-449, 486-512, 517, 535, 543, 559-561, 584-637, 657-680,
685-718, 753, 781, 822-827, 831, 836-847, 873, 887-890, 893-894
"""
import pytest
from unittest.mock import MagicMock, patch
from src.converters.services.sql.helpers.sql_structures import SQLStructureExpander
from src.converters.services.sql.models import (
    SQLDialect, SQLMeasure, SQLDefinition, SQLAggregationType, SQLTranslationOptions,
    SQLStructure
)
from src.converters.base.models import KPI, KPIDefinition, Structure


def make_kpi(name="revenue", formula="sales_amount", agg="SUM", table="fact_sales",
             filters=None, fields_for_constant_selection=None, apply_structures=None):
    return KPI(
        technical_name=name,
        description=f"KPI {name}",
        formula=formula,
        source_table=table,
        aggregation_type=agg,
        filters=filters or [],
        fields_for_constant_selection=fields_for_constant_selection or [],
        apply_structures=apply_structures or [],
    )


def make_definition(kpis=None, structures=None, variables=None):
    return KPIDefinition(
        technical_name="test_def",
        description="Test Definition",
        kpis=kpis or [],
        structures=structures or {},
        default_variables=variables or {},
    )


def make_measure(name="m1", sql_expr="SUM(amount)", agg=SQLAggregationType.SUM,
                 table="fact_table", filters=None, group_by=None, technical_name=None):
    return SQLMeasure(
        name=name,
        sql_expression=sql_expr,
        aggregation_type=agg,
        source_table=table,
        filters=filters or [],
        group_by_columns=group_by or [],
        technical_name=technical_name or name,
        dialect=SQLDialect.DATABRICKS,
    )


def make_sql_definition(measures=None, schema=None):
    sql_def = SQLDefinition(
        dialect=SQLDialect.DATABRICKS,
        technical_name="test",
        description="test",
    )
    if measures:
        sql_def.sql_measures = measures
    if schema:
        sql_def.database_schema = schema
    return sql_def


# ─── SQLStructureExpander construction ────────────────────────────────────────

def test_constructor_standard():
    expander = SQLStructureExpander(dialect=SQLDialect.STANDARD)
    assert expander.dialect == SQLDialect.STANDARD


def test_constructor_databricks():
    expander = SQLStructureExpander(dialect=SQLDialect.DATABRICKS)
    assert expander.dialect == SQLDialect.DATABRICKS


# ─── process_definition - no structures ───────────────────────────────────────

def test_process_definition_no_structures():
    expander = SQLStructureExpander()
    kpi = make_kpi()
    definition = make_definition(kpis=[kpi])
    result = expander.process_definition(definition)
    assert result is not None
    assert len(result.sql_measures) == 1


def test_process_definition_with_filters():
    expander = SQLStructureExpander()
    kpi = make_kpi(filters=["region = 'US'"])
    definition = make_definition(kpis=[kpi])
    result = expander.process_definition(definition)
    assert result is not None


# ─── _is_base_kbi ─────────────────────────────────────────────────────────────

def test_is_base_kbi_no_formula():
    expander = SQLStructureExpander()
    kpi = make_kpi(formula="")
    assert expander._is_base_kbi(kpi) is True


def test_is_base_kbi_simple_column():
    expander = SQLStructureExpander()
    kpi = make_kpi(formula="sales_amount")
    assert expander._is_base_kbi(kpi) is True


def test_is_base_kbi_complex_formula_no_deps():
    expander = SQLStructureExpander()
    kpi = make_kpi(formula="amount * 0.1 + discount")
    # complex formula but no KBI dependencies
    assert expander._is_base_kbi(kpi) is True


# ─── _build_kbi_dependency_tree ───────────────────────────────────────────────

def test_build_kbi_dependency_tree_base_kbi():
    expander = SQLStructureExpander()
    kpi = make_kpi()  # simple column formula = base KBI
    expander._build_kbi_dependency_tree(kpi)
    assert len(expander._base_kbi_contexts) > 0


def test_build_kbi_dependency_tree_with_parent_kbis():
    expander = SQLStructureExpander()
    base_kpi = make_kpi(name="base", formula="sales_amount")
    parent_kpi = make_kpi(name="parent", formula="[base]")
    expander._build_kbi_dependency_tree(base_kpi, parent_kbis=[parent_kpi])
    # Should create contexts for each parent chain
    assert len(expander._base_kbi_contexts) >= 1


# ─── _extract_formula_kbis ────────────────────────────────────────────────────

def test_extract_formula_kbis_empty_formula():
    expander = SQLStructureExpander()
    kpi = make_kpi(formula="")
    result = expander._extract_formula_kbis(kpi)
    assert result == []


def test_extract_formula_kbis_simple():
    expander = SQLStructureExpander()
    kpi = make_kpi(formula="sales_amount")
    result = expander._extract_formula_kbis(kpi)
    assert isinstance(result, list)


# ─── _is_simple_column_reference ─────────────────────────────────────────────

def test_is_simple_column_reference_valid():
    expander = SQLStructureExpander()
    assert expander._is_simple_column_reference("sales_amount") is True
    assert expander._is_simple_column_reference("my_col_1") is True


def test_is_simple_column_reference_invalid():
    expander = SQLStructureExpander()
    assert expander._is_simple_column_reference("") is False
    assert expander._is_simple_column_reference("col + col2") is False
    assert expander._is_simple_column_reference("SUM(col)") is False


# ─── _generate_technical_name ─────────────────────────────────────────────────

def test_generate_technical_name_empty():
    expander = SQLStructureExpander()
    assert expander._generate_technical_name("") == "unnamed_measure"
    assert expander._generate_technical_name(None) == "unnamed_measure"


def test_generate_technical_name_normal():
    expander = SQLStructureExpander()
    result = expander._generate_technical_name("Total Revenue Amount")
    assert " " not in result
    assert result == "total_revenue_amount"


def test_generate_technical_name_special_chars():
    expander = SQLStructureExpander()
    result = expander._generate_technical_name("Revenue (USD) 2024!")
    assert "!" not in result
    assert "(" not in result


# ─── _map_to_sql_aggregation_type ────────────────────────────────────────────

def test_map_to_sql_aggregation_type_none():
    expander = SQLStructureExpander()
    assert expander._map_to_sql_aggregation_type(None) == SQLAggregationType.SUM


def test_map_to_sql_aggregation_type_empty():
    expander = SQLStructureExpander()
    assert expander._map_to_sql_aggregation_type("") == SQLAggregationType.SUM


def test_map_to_sql_aggregation_type_all():
    expander = SQLStructureExpander()
    assert expander._map_to_sql_aggregation_type("COUNT") == SQLAggregationType.COUNT
    assert expander._map_to_sql_aggregation_type("COUNTROWS") == SQLAggregationType.COUNT
    assert expander._map_to_sql_aggregation_type("AVERAGE") == SQLAggregationType.AVG
    assert expander._map_to_sql_aggregation_type("MIN") == SQLAggregationType.MIN
    assert expander._map_to_sql_aggregation_type("MAX") == SQLAggregationType.MAX
    assert expander._map_to_sql_aggregation_type("DISTINCTCOUNT") == SQLAggregationType.COUNT_DISTINCT
    assert expander._map_to_sql_aggregation_type("CALCULATED") == SQLAggregationType.SUM
    assert expander._map_to_sql_aggregation_type("UNKNOWN") == SQLAggregationType.SUM


# ─── _process_filters_for_constant_selection ────────────────────────────────

def test_process_filters_for_constant_selection_no_const_fields():
    expander = SQLStructureExpander()
    filters = ["region = 'US'", "year = 2024"]
    result = expander._process_filters_for_constant_selection(filters, [], MagicMock())
    assert result == filters


def test_process_filters_for_constant_selection_excludes_const_field():
    expander = SQLStructureExpander()
    filters = ["region = 'US'", "year = 2024", "const_field = 'X'"]
    result = expander._process_filters_for_constant_selection(filters, ["const_field"], MagicMock())
    assert len(result) == 2
    assert "const_field = 'X'" not in result


def test_process_filters_for_constant_selection_no_matches():
    expander = SQLStructureExpander()
    filters = ["region = 'US'"]
    result = expander._process_filters_for_constant_selection(filters, ["some_other_field"], MagicMock())
    assert len(result) == 1


# ─── generate_sql_queries_from_definition ────────────────────────────────────

def test_generate_sql_queries_no_measures():
    expander = SQLStructureExpander()
    sql_def = make_sql_definition(measures=[])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    result = expander.generate_sql_queries_from_definition(sql_def, options)
    assert result == []


def test_generate_sql_queries_separate_measures():
    expander = SQLStructureExpander()
    m1 = make_measure("m1")
    m2 = make_measure("m2", sql_expr="COUNT(order_id)", agg=SQLAggregationType.COUNT)
    sql_def = make_sql_definition(measures=[m1, m2])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS, separate_measures=True)
    result = expander.generate_sql_queries_from_definition(sql_def, options)
    assert len(result) == 2


def test_generate_sql_queries_combined():
    expander = SQLStructureExpander()
    m1 = make_measure("m1")
    sql_def = make_sql_definition(measures=[m1])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS, separate_measures=False)
    result = expander.generate_sql_queries_from_definition(sql_def, options)
    assert len(result) == 1


def test_generate_sql_queries_no_options_uses_defaults():
    expander = SQLStructureExpander()
    m1 = make_measure("m1")
    sql_def = make_sql_definition(measures=[m1])
    result = expander.generate_sql_queries_from_definition(sql_def)
    assert len(result) >= 0  # Should not raise


# ─── _create_query_for_sql_measure ───────────────────────────────────────────

def test_create_query_for_sql_measure_basic():
    expander = SQLStructureExpander()
    m = make_measure("m1", group_by=["region"])
    sql_def = make_sql_definition(measures=[m])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_query_for_sql_measure(m, sql_def, options)
    assert query is not None


def test_create_query_for_sql_measure_with_schema():
    expander = SQLStructureExpander()
    m = make_measure("m1")
    sql_def = make_sql_definition(measures=[m], schema="my_schema")
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_query_for_sql_measure(m, sql_def, options)
    assert query is not None


def test_create_query_for_sql_measure_with_const_selection():
    expander = SQLStructureExpander()
    kpi = make_kpi(fields_for_constant_selection=["region"])
    m = make_measure("m1")
    m.original_kbi = kpi
    sql_def = make_sql_definition(measures=[m])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_query_for_sql_measure(m, sql_def, options)
    assert query is not None


def test_create_query_for_sql_measure_exception_aggregation():
    expander = SQLStructureExpander()
    kpi = make_kpi(agg="EXCEPTION_AGGREGATION")
    m = make_measure("m1")
    m.original_kbi = kpi
    sql_def = make_sql_definition(measures=[m])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_query_for_sql_measure(m, sql_def, options)
    assert query is not None


# ─── _create_exception_aggregation_query ────────────────────────────────────

def test_create_exception_aggregation_query_with_filters():
    expander = SQLStructureExpander()
    m = make_measure("exc", filters=["status = 1"])
    sql_def = make_sql_definition(measures=[m])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_exception_aggregation_query(m, sql_def, options)
    assert query is not None


def test_create_exception_aggregation_query_with_schema():
    expander = SQLStructureExpander()
    m = make_measure("exc")
    sql_def = make_sql_definition(measures=[m], schema="my_schema")
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_exception_aggregation_query(m, sql_def, options)
    assert query is not None


# ─── _create_combined_sql_query ──────────────────────────────────────────────

def test_create_combined_sql_query_single_table():
    expander = SQLStructureExpander()
    m1 = make_measure("m1", table="fact_sales")
    m2 = make_measure("m2", sql_expr="COUNT(*)", agg=SQLAggregationType.COUNT, table="fact_sales")
    sql_def = make_sql_definition(measures=[m1, m2])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_combined_sql_query([m1, m2], sql_def, options)
    assert query is not None


def test_create_combined_sql_query_multi_table():
    expander = SQLStructureExpander()
    m1 = make_measure("m1", table="fact_sales")
    m2 = make_measure("m2", table="fact_orders")
    sql_def = make_sql_definition(measures=[m1, m2])
    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    query = expander._create_combined_sql_query([m1, m2], sql_def, options)
    assert query is not None


# ─── process_definition - with structures ────────────────────────────────────

def test_process_definition_with_structures():
    expander = SQLStructureExpander()
    kpi = make_kpi(apply_structures=["YTD"])
    structure = Structure(
        description="Year to Date",
        filters=["year = YEAR(CURRENT_DATE)"],
        formula=None,
        display_sign=1,
    )
    definition = make_definition(
        kpis=[kpi],
        structures={"YTD": structure}
    )
    result = expander.process_definition(definition)
    assert result is not None
    assert len(result.sql_measures) >= 1


def test_process_definition_kpi_without_apply_structures():
    expander = SQLStructureExpander()
    kpi = make_kpi()  # No apply_structures
    structure = Structure(
        description="YTD",
        filters=["year = 2024"],
        formula=None,
        display_sign=1,
    )
    definition = make_definition(
        kpis=[kpi],
        structures={"YTD": structure}
    )
    result = expander.process_definition(definition)
    assert result is not None
    assert len(result.sql_measures) >= 1


# ─── _quote_identifier ───────────────────────────────────────────────────────

def test_quote_identifier_databricks():
    expander = SQLStructureExpander(dialect=SQLDialect.DATABRICKS)
    result = expander._quote_identifier("my_col")
    assert "`" in result


def test_quote_identifier_standard():
    expander = SQLStructureExpander(dialect=SQLDialect.STANDARD)
    result = expander._quote_identifier("my_col")
    assert '"' in result
