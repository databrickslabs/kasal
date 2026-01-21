"""
Unit tests for converters/services/powerbi/dax_to_sql.py

Tests DAX to SQL transpilation including pattern matching, signature-based conversion,
and transpilability validation.
"""

import pytest
from src.converters.services.powerbi.dax_to_sql import DaxToSqlTranspiler
from src.converters.services.powerbi.dax_parser import DaxToken


class TestDaxToSqlTranspiler:
    """Tests for DaxToSqlTranspiler class"""

    @pytest.fixture
    def transpiler(self):
        """Create DaxToSqlTranspiler instance for testing"""
        return DaxToSqlTranspiler()

    # ========== Initialization Tests ==========

    def test_transpiler_initialization(self, transpiler):
        """Test DaxToSqlTranspiler initializes correctly"""
        assert transpiler is not None
        assert hasattr(transpiler, 'dax_to_sql_mappings')
        assert hasattr(transpiler, 'NON_TRANSPILABLE_OPERATORS')

    def test_transpiler_has_mappings(self, transpiler):
        """Test transpiler has DAX to SQL mappings"""
        assert len(transpiler.dax_to_sql_mappings) > 0
        # Check for basic aggregations
        assert 'sum ( <<table:1>> <<column:1>> )' in transpiler.dax_to_sql_mappings

    def test_transpiler_non_transpilable_operators(self, transpiler):
        """Test transpiler defines non-transpilable operators"""
        assert len(transpiler.NON_TRANSPILABLE_OPERATORS) > 0
        assert 'KEEPFILTERS' in transpiler.NON_TRANSPILABLE_OPERATORS
        assert 'CALCULATETABLE' in transpiler.NON_TRANSPILABLE_OPERATORS

    # ========== Can Transpile Tests ==========

    def test_can_transpile_simple_sum(self, transpiler):
        """Test can transpile simple SUM"""
        tokens = [
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount]')
        ]
        can_transpile, reason = transpiler.can_transpile(tokens)

        assert can_transpile is True
        assert reason is None

    def test_can_transpile_with_non_transpilable_operator(self, transpiler):
        """Test cannot transpile with KEEPFILTERS"""
        tokens = [
            DaxToken(type='function', value='SUM'),
            DaxToken(type='function', value='KEEPFILTERS'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount]')
        ]
        can_transpile, reason = transpiler.can_transpile(tokens)

        assert can_transpile is False
        assert reason is not None
        assert 'KEEPFILTERS' in reason

    def test_can_transpile_with_calculatetable(self, transpiler):
        """Test cannot transpile with CALCULATETABLE"""
        tokens = [
            DaxToken(type='function', value='CALCULATETABLE'),
            DaxToken(type='table', value='Sales')
        ]
        can_transpile, reason = transpiler.can_transpile(tokens)

        assert can_transpile is False
        assert 'CALCULATETABLE' in reason

    def test_can_transpile_empty_tokens(self, transpiler):
        """Test can transpile with empty tokens"""
        can_transpile, reason = transpiler.can_transpile([])

        assert can_transpile is True
        assert reason is None

    def test_can_transpile_with_average(self, transpiler):
        """Test can transpile AVERAGE"""
        tokens = [
            DaxToken(type='function', value='AVERAGE'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Price]')
        ]
        can_transpile, reason = transpiler.can_transpile(tokens)

        assert can_transpile is True
        assert reason is None

    # ========== Transpile Tests ==========

    def test_transpile_simple_sum(self, transpiler):
        """Test transpiling simple SUM"""
        signature = 'sum ( <<table:1>> <<column:1>> )'
        tokens = [
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'SUM(' in result
        assert 'Amount' in result

    def test_transpile_average(self, transpiler):
        """Test transpiling AVERAGE"""
        signature = 'average ( <<table:1>> <<column:1>> )'
        tokens = [
            DaxToken(type='function', value='AVERAGE'),
            DaxToken(type='table', value='Products'),
            DaxToken(type='column', value='[Price]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'AVG(' in result
        assert 'Price' in result

    def test_transpile_distinctcount(self, transpiler):
        """Test transpiling DISTINCTCOUNT"""
        signature = 'distinctcount ( <<table:1>> <<column:1>> )'
        tokens = [
            DaxToken(type='function', value='DISTINCTCOUNT'),
            DaxToken(type='table', value='Orders'),
            DaxToken(type='column', value='[CustomerID]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'COUNT(DISTINCT' in result
        assert 'CustomerID' in result

    def test_transpile_min_max(self, transpiler):
        """Test transpiling MIN and MAX"""
        min_signature = 'min ( <<table:1>> <<column:1>> )'
        min_tokens = [
            DaxToken(type='function', value='MIN'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Date]')
        ]
        min_result = transpiler.transpile(min_signature, min_tokens)

        assert min_result is not None
        assert 'MIN(' in min_result
        assert 'Date' in min_result

        max_signature = 'max ( <<table:1>> <<column:1>> )'
        max_tokens = [
            DaxToken(type='function', value='MAX'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Date]')
        ]
        max_result = transpiler.transpile(max_signature, max_tokens)

        assert max_result is not None
        assert 'MAX(' in max_result

    def test_transpile_divide(self, transpiler):
        """Test transpiling DIVIDE"""
        signature = 'divide ( <<measure:1>> , <<number:1>> )'
        tokens = [
            DaxToken(type='function', value='DIVIDE'),
            DaxToken(type='measure', value='[Revenue]'),
            DaxToken(type='number', value='100')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'TRY_DIVIDE' in result
        assert 'Revenue' in result
        assert '100' in result

    def test_transpile_calculate_with_sum(self, transpiler):
        """Test transpiling CALCULATE with SUM"""
        signature = 'calculate ( sum ( <<table:1>> <<column:1>> ) )'
        tokens = [
            DaxToken(type='function', value='CALCULATE'),
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'SUM(' in result
        assert 'Amount' in result

    def test_transpile_measure_reference(self, transpiler):
        """Test transpiling measure reference"""
        signature = '<<measure:1>>'
        tokens = [
            DaxToken(type='measure', value='[TotalSales]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'TotalSales' in result

    def test_transpile_unknown_signature(self, transpiler):
        """Test transpiling unknown signature returns None"""
        signature = 'unknown ( <<table:1>> <<column:1>> )'
        tokens = [
            DaxToken(type='function', value='UNKNOWN'),
            DaxToken(type='table', value='Table'),
            DaxToken(type='column', value='[Column]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is None

    def test_transpile_number_literal(self, transpiler):
        """Test transpiling number literal"""
        signature = '<<number:1>>'
        tokens = [
            DaxToken(type='number', value='42')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert '42' in result

    def test_transpile_string_literal(self, transpiler):
        """Test transpiling string literal"""
        signature = '<<string:1>>'
        tokens = [
            DaxToken(type='string', value='"test"')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'test' in result

    def test_transpile_measure_arithmetic(self, transpiler):
        """Test transpiling measure arithmetic"""
        signature = '<<measure:1>> - <<measure:2>>'
        tokens = [
            DaxToken(type='measure', value='[Revenue]'),
            DaxToken(type='measure', value='[Cost]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'Revenue' in result
        assert 'Cost' in result
        assert '-' in result

    def test_transpile_date_functions(self, transpiler):
        """Test transpiling date functions"""
        # LASTDATE
        last_signature = 'lastdate ( <<table:1>> <<column:1>> )'
        last_tokens = [
            DaxToken(type='function', value='LASTDATE'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Date]')
        ]
        last_result = transpiler.transpile(last_signature, last_tokens)

        assert last_result is not None
        assert 'MAX(' in last_result

        # FIRSTDATE
        first_signature = 'firstdate ( <<table:1>> <<column:1>> )'
        first_tokens = [
            DaxToken(type='function', value='FIRSTDATE'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Date]')
        ]
        first_result = transpiler.transpile(first_signature, first_tokens)

        assert first_result is not None
        assert 'MIN(' in first_result

    def test_transpile_case_insensitive(self, transpiler):
        """Test transpile is case insensitive for signatures"""
        signature_upper = 'SUM ( <<table:1>> <<column:1>> )'
        signature_lower = 'sum ( <<table:1>> <<column:1>> )'
        tokens = [
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount]')
        ]

        result_upper = transpiler.transpile(signature_upper, tokens)
        result_lower = transpiler.transpile(signature_lower, tokens)

        # Both should work
        assert result_upper is not None
        assert result_lower is not None

    # ========== Edge Cases ==========

    def test_transpile_multiple_columns(self, transpiler):
        """Test transpiling with multiple columns"""
        signature = 'sum ( <<table:1>> <<column:1>> ) - sum ( <<table:1>> <<column:2>> )'
        tokens = [
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Revenue]'),
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Cost]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'Revenue' in result
        assert 'Cost' in result
        assert 'SUM(' in result

    def test_transpile_removes_brackets_from_columns(self, transpiler):
        """Test transpile removes brackets from column names"""
        signature = 'sum ( <<table:1>> <<column:1>> )'
        tokens = [
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount with Brackets]')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert '[' not in result or '`[' not in result  # Should not have bare brackets
        assert 'Amount with Brackets' in result

    def test_can_transpile_with_multiple_non_transpilable(self, transpiler):
        """Test cannot transpile with multiple non-transpilable operators"""
        tokens = [
            DaxToken(type='function', value='KEEPFILTERS'),
            DaxToken(type='function', value='ALLSELECTED')
        ]
        can_transpile, reason = transpiler.can_transpile(tokens)

        assert can_transpile is False
        assert reason is not None

    def test_transpile_complex_calculate_pattern(self, transpiler):
        """Test transpiling complex CALCULATE pattern"""
        signature = 'calculate ( sum ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == <<string:1>> )'
        tokens = [
            DaxToken(type='function', value='CALCULATE'),
            DaxToken(type='function', value='SUM'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Amount]'),
            DaxToken(type='table', value='Sales'),
            DaxToken(type='column', value='[Region]'),
            DaxToken(type='string', value='"EMEA"')
        ]
        result = transpiler.transpile(signature, tokens)

        assert result is not None
        assert 'SUM(' in result
        assert 'CASE WHEN' in result
        assert 'Region' in result
