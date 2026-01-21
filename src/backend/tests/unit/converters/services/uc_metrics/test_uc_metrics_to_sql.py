"""
Unit tests for converters/services/uc_metrics/uc_metrics_to_sql.py

Tests UC Metrics to SQL transpilation including YAML parsing, SQL generation,
window functions, and documentation generation.
"""

import pytest
from src.converters.services.uc_metrics.uc_metrics_to_sql import UCMetricsToSqlTranspiler


class TestUCMetricsToSqlTranspiler:
    """Tests for UCMetricsToSqlTranspiler class"""

    @pytest.fixture
    def transpiler(self):
        """Create UCMetricsToSqlTranspiler instance for testing"""
        return UCMetricsToSqlTranspiler()

    @pytest.fixture
    def simple_uc_metrics(self):
        """Simple UC Metrics definition for testing"""
        return {
            "version": "0.1",
            "description": "Sales metrics",
            "source": "main.sales.transactions",
            "measures": [
                {
                    "name": "total_revenue",
                    "expr": "SUM(amount)"
                }
            ]
        }

    @pytest.fixture
    def uc_metrics_with_filter(self):
        """UC Metrics definition with filter"""
        return {
            "version": "0.1",
            "description": "Filtered metrics",
            "source": "main.sales.transactions",
            "filter": "year = 2024",
            "measures": [
                {
                    "name": "total_revenue",
                    "expr": "SUM(amount)"
                }
            ]
        }

    @pytest.fixture
    def uc_metrics_multiple_measures(self):
        """UC Metrics with multiple measures"""
        return {
            "version": "0.1",
            "description": "Multiple metrics",
            "source": "main.sales.transactions",
            "measures": [
                {
                    "name": "total_revenue",
                    "expr": "SUM(amount)"
                },
                {
                    "name": "transaction_count",
                    "expr": "COUNT(*)"
                },
                {
                    "name": "avg_transaction",
                    "expr": "AVG(amount)"
                }
            ]
        }

    # ========== Initialization Tests ==========

    def test_transpiler_initialization(self, transpiler):
        """Test UCMetricsToSqlTranspiler initializes correctly"""
        assert transpiler is not None
        assert hasattr(transpiler, 'logger')

    # ========== Transpile From YAML Tests ==========

    def test_transpile_from_yaml_simple(self, transpiler):
        """Test transpiling from YAML string"""
        yaml_string = """
version: "0.1"
description: "Test metrics"
source: "main.test.table"
measures:
  - name: "total"
    expr: "SUM(value)"
"""
        result = transpiler.transpile_from_yaml(yaml_string)

        assert isinstance(result, list)
        assert len(result) == 1
        assert "SUM(value)" in result[0]
        assert "total" in result[0]

    def test_transpile_from_yaml_invalid(self, transpiler):
        """Test transpiling invalid YAML raises error"""
        invalid_yaml = "{ invalid: yaml: syntax"

        with pytest.raises(ValueError, match="Invalid YAML format"):
            transpiler.transpile_from_yaml(invalid_yaml)

    def test_transpile_from_yaml_with_table_override(self, transpiler):
        """Test transpiling with table reference override"""
        yaml_string = """
version: "0.1"
source: "main.old.table"
measures:
  - name: "total"
    expr: "SUM(value)"
"""
        result = transpiler.transpile_from_yaml(yaml_string, table_reference="main.new.table")

        assert len(result) == 1
        assert "main.new.table" in result[0]
        assert "main.old.table" not in result[0]

    # ========== Transpile Tests ==========

    def test_transpile_simple(self, transpiler, simple_uc_metrics):
        """Test transpiling simple UC Metrics"""
        result = transpiler.transpile(simple_uc_metrics)

        assert isinstance(result, list)
        assert len(result) == 1
        assert "SELECT SUM(amount)" in result[0]
        assert "total_revenue" in result[0]
        assert "main.sales.transactions" in result[0]

    def test_transpile_with_filter(self, transpiler, uc_metrics_with_filter):
        """Test transpiling with filter condition"""
        result = transpiler.transpile(uc_metrics_with_filter)

        assert len(result) == 1
        assert "WHERE year = 2024" in result[0]

    def test_transpile_multiple_measures(self, transpiler, uc_metrics_multiple_measures):
        """Test transpiling multiple measures"""
        result = transpiler.transpile(uc_metrics_multiple_measures)

        assert len(result) == 3
        assert any("total_revenue" in sql for sql in result)
        assert any("transaction_count" in sql for sql in result)
        assert any("avg_transaction" in sql for sql in result)

    def test_transpile_with_table_override(self, transpiler, simple_uc_metrics):
        """Test transpiling with table reference override"""
        result = transpiler.transpile(simple_uc_metrics, table_reference="catalog.schema.table")

        assert "catalog.schema.table" in result[0]
        assert "main.sales.transactions" not in result[0]

    def test_transpile_invalid_format(self, transpiler):
        """Test transpiling invalid format raises error"""
        with pytest.raises(ValueError, match="dictionary"):
            transpiler.transpile("not a dict")

    def test_transpile_missing_measures(self, transpiler):
        """Test transpiling without measures raises error"""
        invalid_metrics = {
            "version": "0.1",
            "source": "table"
        }

        with pytest.raises(ValueError, match="measures"):
            transpiler.transpile(invalid_metrics)

    def test_transpile_empty_measures(self, transpiler):
        """Test transpiling with empty measures list raises error"""
        invalid_metrics = {
            "version": "0.1",
            "source": "table",
            "measures": []
        }

        with pytest.raises(ValueError, match="cannot be empty"):
            transpiler.transpile(invalid_metrics)

    def test_transpile_measure_missing_name(self, transpiler):
        """Test transpiling measure without name raises error"""
        invalid_metrics = {
            "version": "0.1",
            "source": "table",
            "measures": [
                {"expr": "SUM(value)"}
            ]
        }

        with pytest.raises(ValueError, match="name"):
            transpiler.transpile(invalid_metrics)

    def test_transpile_measure_missing_expr(self, transpiler):
        """Test transpiling measure without expr raises error"""
        invalid_metrics = {
            "version": "0.1",
            "source": "table",
            "measures": [
                {"name": "total"}
            ]
        }

        with pytest.raises(ValueError, match="expr"):
            transpiler.transpile(invalid_metrics)

    # ========== Transpile to Single Query Tests ==========

    def test_transpile_to_single_query_simple(self, transpiler, simple_uc_metrics):
        """Test generating single query with one measure"""
        result = transpiler.transpile_to_single_query(simple_uc_metrics)

        assert isinstance(result, str)
        assert "SELECT" in result
        assert "SUM(amount) as total_revenue" in result
        assert "FROM main.sales.transactions" in result

    def test_transpile_to_single_query_multiple_measures(self, transpiler, uc_metrics_multiple_measures):
        """Test generating single query with multiple measures"""
        result = transpiler.transpile_to_single_query(uc_metrics_multiple_measures)

        assert "total_revenue" in result
        assert "transaction_count" in result
        assert "avg_transaction" in result
        assert result.count("SELECT") == 1  # Only one SELECT
        assert "," in result  # Measures separated by commas

    def test_transpile_to_single_query_with_filter(self, transpiler, uc_metrics_with_filter):
        """Test generating single query with filter"""
        result = transpiler.transpile_to_single_query(uc_metrics_with_filter)

        assert "WHERE year = 2024" in result

    def test_transpile_to_single_query_with_table_override(self, transpiler, simple_uc_metrics):
        """Test generating single query with table override"""
        result = transpiler.transpile_to_single_query(
            simple_uc_metrics,
            table_reference="custom.table.name"
        )

        assert "custom.table.name" in result

    # ========== Extract Table References Tests ==========

    def test_extract_table_references_simple(self, transpiler, simple_uc_metrics):
        """Test extracting table references"""
        result = transpiler.extract_table_references(simple_uc_metrics)

        assert isinstance(result, list)
        assert len(result) >= 1
        assert "main.sales.transactions" in result

    def test_extract_table_references_multiple(self, transpiler):
        """Test extracting multiple table references"""
        uc_metrics = {
            "version": "0.1",
            "source": "main.sales.transactions",
            "measures": [
                {
                    "name": "total",
                    "expr": "SUM(catalog.schema.table.value)"
                }
            ]
        }
        result = transpiler.extract_table_references(uc_metrics)

        assert len(result) >= 1
        assert "main.sales.transactions" in result

    def test_extract_table_references_no_source(self, transpiler):
        """Test extracting table references without source"""
        uc_metrics = {
            "version": "0.1",
            "measures": [
                {
                    "name": "total",
                    "expr": "SUM(value)"
                }
            ]
        }
        result = transpiler.extract_table_references(uc_metrics)

        assert isinstance(result, list)
        # May be empty or contain extracted references from expressions

    def test_extract_table_references_no_duplicates(self, transpiler):
        """Test extracting table references removes duplicates"""
        uc_metrics = {
            "version": "0.1",
            "source": "main.sales.table",
            "measures": [
                {"name": "m1", "expr": "SUM(main.sales.table.value)"},
                {"name": "m2", "expr": "COUNT(main.sales.table.id)"}
            ]
        }
        result = transpiler.extract_table_references(uc_metrics)

        # Should have unique references only
        assert len(result) == len(set(result))

    # ========== Generate SQL Documentation Tests ==========

    def test_generate_sql_documentation_simple(self, transpiler, simple_uc_metrics):
        """Test generating SQL documentation"""
        result = transpiler.generate_sql_documentation(simple_uc_metrics)

        assert isinstance(result, str)
        assert "UC Metrics to SQL Transpilation" in result
        assert "Version: 0.1" in result
        assert "Description: Sales metrics" in result
        assert "total_revenue" in result

    def test_generate_sql_documentation_includes_individual_queries(self, transpiler, uc_metrics_multiple_measures):
        """Test documentation includes individual queries"""
        result = transpiler.generate_sql_documentation(uc_metrics_multiple_measures)

        assert "Individual Measure Queries" in result
        assert "total_revenue" in result
        assert "transaction_count" in result
        assert "avg_transaction" in result

    def test_generate_sql_documentation_includes_consolidated_query(self, transpiler, uc_metrics_multiple_measures):
        """Test documentation includes consolidated query"""
        result = transpiler.generate_sql_documentation(uc_metrics_multiple_measures)

        assert "Consolidated Query" in result
        # Should have all measures in one query
        consolidated_section = result.split("Consolidated Query")[1]
        assert "total_revenue" in consolidated_section
        assert "transaction_count" in consolidated_section

    def test_generate_sql_documentation_with_filter(self, transpiler, uc_metrics_with_filter):
        """Test documentation includes filter information"""
        result = transpiler.generate_sql_documentation(uc_metrics_with_filter)

        assert "Common Filter: year = 2024" in result

    def test_generate_sql_documentation_includes_table_references(self, transpiler, simple_uc_metrics):
        """Test documentation includes table references"""
        result = transpiler.generate_sql_documentation(simple_uc_metrics)

        assert "Source Tables:" in result
        assert "main.sales.transactions" in result

    # ========== Window Function Tests ==========

    def test_transpile_with_window_function(self, transpiler):
        """Test transpiling measure with window function"""
        uc_metrics = {
            "version": "0.1",
            "source": "main.sales.table",
            "measures": [
                {
                    "name": "last_value",
                    "expr": "SUM(amount)",
                    "window": {
                        "order": "date",
                        "semiadditive": "last"
                    }
                }
            ]
        }
        result = transpiler.transpile(uc_metrics)

        assert len(result) == 1
        assert "LAST_VALUE" in result[0] or "window:" in result[0]

    def test_transpile_window_function_list_config(self, transpiler):
        """Test transpiling with window config as list"""
        uc_metrics = {
            "version": "0.1",
            "source": "main.sales.table",
            "measures": [
                {
                    "name": "windowed",
                    "expr": "SUM(value)",
                    "window": [
                        {
                            "order": "timestamp",
                            "semiadditive": "last"
                        }
                    ]
                }
            ]
        }
        result = transpiler.transpile(uc_metrics)

        assert len(result) == 1
        # Should handle window configuration
        assert "windowed" in result[0]

    # ========== Edge Cases ==========

    def test_transpile_no_source_table(self, transpiler):
        """Test transpiling without source table"""
        uc_metrics = {
            "version": "0.1",
            "measures": [
                {
                    "name": "const",
                    "expr": "42"
                }
            ]
        }
        result = transpiler.transpile(uc_metrics)

        assert len(result) == 1
        assert "SELECT 42 as const" in result[0]
        # Should not have FROM clause
        assert "FROM" not in result[0] or result[0].count("FROM") == 0

    def test_transpile_complex_expression(self, transpiler):
        """Test transpiling complex SQL expression"""
        uc_metrics = {
            "version": "0.1",
            "source": "main.sales.table",
            "measures": [
                {
                    "name": "profit_margin",
                    "expr": "CAST((SUM(revenue) - SUM(cost)) / NULLIF(SUM(revenue), 0) AS DECIMAL(10,2)) * 100"
                }
            ]
        }
        result = transpiler.transpile(uc_metrics)

        assert len(result) == 1
        assert "profit_margin" in result[0]
        assert "CAST" in result[0] or "DECIMAL" in result[0] or "SUM" in result[0]

    def test_transpile_with_special_characters_in_names(self, transpiler):
        """Test transpiling with special characters in measure names"""
        uc_metrics = {
            "version": "0.1",
            "source": "table",
            "measures": [
                {
                    "name": "total_revenue_usd",
                    "expr": "SUM(amount)"
                }
            ]
        }
        result = transpiler.transpile(uc_metrics)

        assert "total_revenue_usd" in result[0]
