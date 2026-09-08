"""
Coverage tests for src/services/converters/formats/sql/yaml_to_sql.py
Targets uncovered lines: 150-157, 161-162, 166, 169, 179, 338, 381-382,
444-470, 479-514, 520-551
"""

from unittest.mock import MagicMock

from src.services.converters.base.models import KPI, KPIDefinition
from src.services.converters.formats.sql.models import (
    SQLAggregationType,
    SQLDefinition,
    SQLDialect,
    SQLMeasure,
    SQLQuery,
    SQLTranslationOptions,
    SQLTranslationResult,
)
from src.services.converters.formats.sql.yaml_to_sql import SQLGenerator


def make_kpi(
    name="revenue", formula="sales_amount", agg="SUM", table="fact_sales", filters=None
):
    return KPI(
        technical_name=name,
        description=f"KPI {name}",
        formula=formula,
        source_table=table,
        aggregation_type=agg,
        filters=filters or [],
    )


def make_definition(
    kpis=None, structures=None, description="Test Definition", variables=None
):
    return KPIDefinition(
        technical_name="test_def",
        description=description,
        kpis=kpis or [],
        structures=structures or {},
        default_variables=variables or {},
    )


# ─── SQLGenerator construction ────────────────────────────────────────────────


def test_sql_generator_databricks_dialect():
    gen = SQLGenerator(dialect=SQLDialect.DATABRICKS)
    assert gen.dialect == SQLDialect.DATABRICKS
    assert gen.dialect_config["quote_char"] == "`"


def test_sql_generator_standard_dialect():
    gen = SQLGenerator(dialect=SQLDialect.STANDARD)
    assert gen.dialect == SQLDialect.STANDARD
    assert gen.dialect_config["quote_char"] == '"'


def test_quote_identifier():
    gen = SQLGenerator(SQLDialect.DATABRICKS)
    assert gen.quote_identifier("my_col") == "`my_col`"


# ─── _estimate_complexity ─────────────────────────────────────────────────────


def test_estimate_complexity_low():
    gen = SQLGenerator()
    definition = MagicMock()
    definition.sql_measures = [MagicMock(filters=[]) for _ in range(3)]
    definition.sql_structures = {}
    result = gen._estimate_complexity(definition)
    assert result == "LOW"


def test_estimate_complexity_medium_many_measures():
    gen = SQLGenerator()
    definition = MagicMock()
    definition.sql_measures = [MagicMock(filters=[]) for _ in range(7)]
    definition.sql_structures = {}
    result = gen._estimate_complexity(definition)
    assert result == "MEDIUM"


def test_estimate_complexity_medium_has_filters():
    gen = SQLGenerator()
    definition = MagicMock()
    definition.sql_measures = [MagicMock(filters=["region = 'US'"]) for _ in range(3)]
    definition.sql_structures = {}
    result = gen._estimate_complexity(definition)
    assert result == "MEDIUM"


def test_estimate_complexity_high_many_measures():
    gen = SQLGenerator()
    definition = MagicMock()
    definition.sql_measures = [MagicMock(filters=[]) for _ in range(12)]
    definition.sql_structures = True
    result = gen._estimate_complexity(definition)
    assert result == "HIGH"


def test_estimate_complexity_high_has_structures():
    gen = SQLGenerator()
    definition = MagicMock()
    definition.sql_measures = [MagicMock(filters=[]) for _ in range(3)]
    definition.sql_structures = {"some_struct": MagicMock()}
    result = gen._estimate_complexity(definition)
    assert result == "HIGH"


# ─── _enhance_result_with_analysis ────────────────────────────────────────────


def make_result(sql_queries=None, sql_measures=None):
    """Make a SQLTranslationResult with proper defaults."""
    from src.services.converters.formats.sql.models import (
        SQLDialect,
        SQLQuery,
        SQLTranslationOptions,
    )

    # Convert any MagicMock queries into real SQLQuery objects or skip them
    real_queries = []
    if sql_queries:
        for q in sql_queries:
            if isinstance(q, SQLQuery):
                real_queries.append(q)
            # MagicMock queries are not accepted by pydantic - use real ones only
    return SQLTranslationResult(
        sql_queries=real_queries,
        sql_measures=sql_measures or [],
        sql_definition=SQLDefinition(
            dialect=SQLDialect.DATABRICKS, technical_name="test", description="test"
        ),
        translation_options=SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS),
        measures_count=0,
        queries_count=0,
        syntax_valid=True,
        validation_messages=[],
        estimated_complexity="LOW",
        optimization_suggestions=[],
    )


def make_sql_query(sql_text: str) -> "SQLQuery":
    """Create a real SQLQuery with given SQL text in select_clause."""
    return SQLQuery(
        dialect=SQLDialect.DATABRICKS,
        select_clause=sql_text,
        from_clause="fact_table",
        description="test query",
    )


def test_enhance_result_valid_query_no_issues():
    """Valid SELECT...FROM query - no validation issues."""
    gen = SQLGenerator()
    query = SQLQuery(
        dialect=SQLDialect.DATABRICKS,
        select_clause=["SUM(amount)"],
        from_clause="fact_sales",
        description="test",
    )
    result = make_result(sql_queries=[query])
    enhanced = gen._enhance_result_with_analysis(result)
    assert enhanced.syntax_valid is True
    assert enhanced.validation_messages == []


def test_enhance_result_unresolved_variables():
    """Query with unresolved $var should add validation message."""
    gen = SQLGenerator()
    query = SQLQuery(
        dialect=SQLDialect.DATABRICKS,
        select_clause=["SUM(amount)"],
        from_clause="fact_sales",
        where_clause=["date > $var_date"],
        description="test",
    )
    result = make_result(sql_queries=[query])
    enhanced = gen._enhance_result_with_analysis(result)
    assert any("unresolved variables" in m for m in enhanced.validation_messages)


def test_enhance_result_many_measures_suggestion():
    """More than 5 measures should suggest CTEs."""
    gen = SQLGenerator()
    measures = []
    for i in range(7):
        m = SQLMeasure(
            name=f"measure_{i}",
            sql_expression=f"SUM(col{i})",
            aggregation_type=SQLAggregationType.SUM,
            source_table="fact",
            dialect=SQLDialect.DATABRICKS,
        )
        measures.append(m)
    result = make_result(sql_measures=measures)

    enhanced = gen._enhance_result_with_analysis(result)
    assert any("CTE" in s for s in enhanced.optimization_suggestions)


def test_enhance_result_complex_filters_suggestion():
    """Measure with >3 filters should suggest filtered views."""
    gen = SQLGenerator()
    m = SQLMeasure(
        name="complex",
        sql_expression="SUM(col)",
        aggregation_type=SQLAggregationType.SUM,
        source_table="fact",
        dialect=SQLDialect.DATABRICKS,
        filters=["f1", "f2", "f3", "f4"],
    )
    result = make_result(sql_measures=[m])

    enhanced = gen._enhance_result_with_analysis(result)
    assert any("filtered views" in s for s in enhanced.optimization_suggestions)


# ─── _map_aggregation_type ────────────────────────────────────────────────────


# ─── _generate_sql_expression ─────────────────────────────────────────────────


# ─── _convert_formula_to_sql ──────────────────────────────────────────────────


# ─── _process_filters ─────────────────────────────────────────────────────────


# ─── _substitute_variables ────────────────────────────────────────────────────


# ─── generate_sql_from_kbi_definition - error handling ──────────────────────


def test_generate_sql_from_kbi_definition_returns_error_result_on_exception():
    gen = SQLGenerator()
    definition = make_definition(kpis=[make_kpi()])

    # Force the structure processor to raise
    gen.structure_processor.process_definition = MagicMock(
        side_effect=Exception("processing error")
    )

    options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
    result = gen.generate_sql_from_kbi_definition(definition, options)

    assert result.syntax_valid is False
    assert result.measures_count == 0
    assert any("Generation failed" in m for m in result.validation_messages)


def test_generate_sql_from_kbi_definition_no_options():
    gen = SQLGenerator()
    definition = make_definition(kpis=[make_kpi()])

    mock_sql_def = MagicMock()
    mock_sql_def.sql_measures = []
    mock_sql_def.sql_structures = {}
    mock_sql_def.description = "Test"
    mock_sql_def.technical_name = "test"

    gen.structure_processor.process_definition = MagicMock(return_value=mock_sql_def)
    gen.structure_processor.generate_sql_queries_from_definition = MagicMock(
        return_value=[]
    )

    result = gen.generate_sql_from_kbi_definition(definition)
    assert result is not None
