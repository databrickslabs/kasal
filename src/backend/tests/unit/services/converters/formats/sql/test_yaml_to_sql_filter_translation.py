"""
Extended unit tests for converters/formats/sql/yaml_to_sql.py - Part 2

Targets uncovered lines: 108-111, 150-179, 203-226, 284, 290, 300, 338,
381-382, 444-470, 479-514, 520-551
"""

import pytest

from src.services.converters.base.models import KPI, KPIDefinition
from src.services.converters.formats.sql.models import (
    SQLAggregationType,
    SQLDialect,
    SQLTranslationOptions,
)
from src.services.converters.formats.sql.yaml_to_sql import SQLGenerator


def make_kpi(technical_name="sales", formula="amount", agg="SUM", **kwargs):
    return KPI(
        description=kwargs.pop("description", "Test KPI"),
        technical_name=technical_name,
        formula=formula,
        source_table=kwargs.pop("source_table", "Sales"),
        aggregation_type=agg,
        **kwargs,
    )


def make_definition(kpis=None, variables=None, **kwargs):
    return KPIDefinition(
        description="Test Metrics",
        technical_name="test_metrics",
        kpis=kpis or [],
        default_variables=variables or {},
        **kwargs,
    )


class TestSQLGeneratorInit2:
    def test_default_dialect_is_databricks(self):
        gen = SQLGenerator()
        assert gen.dialect == SQLDialect.DATABRICKS

    def test_standard_dialect(self):
        gen = SQLGenerator(dialect=SQLDialect.STANDARD)
        assert gen.dialect == SQLDialect.STANDARD

    def test_quote_identifier_databricks(self):
        gen = SQLGenerator(dialect=SQLDialect.DATABRICKS)
        assert gen.quote_identifier("sales") == "`sales`"

    def test_quote_identifier_standard(self):
        gen = SQLGenerator(dialect=SQLDialect.STANDARD)
        assert gen.quote_identifier("sales") == '"sales"'


class TestGenerateSQLFromKBIDefinition2:
    @pytest.fixture
    def gen(self):
        return SQLGenerator()

    def test_basic_generation(self, gen):
        kpi = make_kpi()
        definition = make_definition(kpis=[kpi])
        result = gen.generate_sql_from_kbi_definition(definition)
        assert result is not None

    def test_error_handling_returns_error_result(self, gen):
        """Lines 108-111: exception path returns invalid result."""
        from unittest.mock import patch

        kpi = make_kpi()
        definition = make_definition(kpis=[kpi])
        with patch.object(
            gen.structure_processor,
            "process_definition",
            side_effect=Exception("test error"),
        ):
            result = gen.generate_sql_from_kbi_definition(definition)
            assert result.syntax_valid is False
            assert any("Generation failed" in m for m in result.validation_messages)

    def test_custom_options_used(self, gen):
        kpi = make_kpi()
        definition = make_definition(kpis=[kpi])
        options = SQLTranslationOptions(
            target_dialect=SQLDialect.STANDARD,
            include_comments=False,
        )
        result = gen.generate_sql_from_kbi_definition(definition, options)
        assert result.translation_options.target_dialect == SQLDialect.STANDARD


class TestEstimateComplexity2:
    @pytest.fixture
    def gen(self):
        return SQLGenerator()

    def test_low_complexity_single_measure(self, gen):
        from src.services.converters.formats.sql.models import SQLDefinition, SQLMeasure

        sql_def = SQLDefinition(description="T", technical_name="t")
        sql_def.sql_measures = [
            SQLMeasure(
                name="s",
                sql_expression="SUM(x)",
                aggregation_type=SQLAggregationType.SUM,
                source_table="t",
            )
        ]
        assert gen._estimate_complexity(sql_def) == "LOW"

    def test_medium_complexity_6_measures_with_filter(self, gen):
        from src.services.converters.formats.sql.models import SQLDefinition, SQLMeasure

        sql_def = SQLDefinition(description="T", technical_name="t")
        sql_def.sql_measures = [
            SQLMeasure(
                name=f"m{i}",
                sql_expression=f"SUM(x{i})",
                aggregation_type=SQLAggregationType.SUM,
                source_table="t",
                filters=["x=1"],
            )
            for i in range(6)
        ]
        result = gen._estimate_complexity(sql_def)
        assert result in ("MEDIUM", "HIGH")

    def test_high_complexity_11_measures(self, gen):
        from src.services.converters.formats.sql.models import SQLDefinition, SQLMeasure

        sql_def = SQLDefinition(description="T", technical_name="t")
        sql_def.sql_measures = [
            SQLMeasure(
                name=f"m{i}",
                sql_expression=f"SUM(x{i})",
                aggregation_type=SQLAggregationType.SUM,
                source_table="t",
            )
            for i in range(11)
        ]
        assert gen._estimate_complexity(sql_def) == "HIGH"
