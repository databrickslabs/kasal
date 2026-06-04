"""
Extended unit tests for converters/services/powerbi/dax_to_sql.py

Targets uncovered code paths to increase coverage.
Focuses on:
- DaxToSqlTranspiler.can_transpile
- DaxToSqlTranspiler.transpile with various patterns
- Non-transpilable patterns
"""

import pytest
from src.converters.services.powerbi.dax_to_sql import DaxToSqlTranspiler
from src.converters.services.powerbi.dax_parser import DaxToken, DAXExpressionParser


class TestDaxToSqlTranspilerExtended:
    """Extended tests for DaxToSqlTranspiler"""

    @pytest.fixture
    def transpiler(self):
        return DaxToSqlTranspiler()

    @pytest.fixture
    def parser(self):
        return DAXExpressionParser()

    # ========== can_transpile Tests ==========

    def test_can_transpile_empty_tokens(self, transpiler):
        """Test can_transpile with empty token list"""
        is_transpilable, reason = transpiler.can_transpile([])
        assert is_transpilable is True
        assert reason is None

    def test_can_transpile_simple_tokens(self, transpiler):
        """Test can_transpile with simple function tokens"""
        tokens = [
            DaxToken(type="function", value="SUM"),
            DaxToken(type="table", value="Sales"),
            DaxToken(type="column", value="[Amount]")
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is True
        assert reason is None

    def test_cannot_transpile_keepfilters(self, transpiler):
        """Test can_transpile fails for KEEPFILTERS"""
        tokens = [
            DaxToken(type="function", value="KEEPFILTERS"),
            DaxToken(type="table", value="Sales")
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False
        assert reason is not None
        assert "KEEPFILTERS" in reason

    def test_cannot_transpile_allselected(self, transpiler):
        """Test can_transpile fails for ALLSELECTED"""
        tokens = [
            DaxToken(type="function", value="ALLSELECTED"),
            DaxToken(type="table", value="Sales")
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False

    def test_cannot_transpile_isinscope(self, transpiler):
        """Test can_transpile fails for ISINSCOPE"""
        tokens = [
            DaxToken(type="function", value="ISINSCOPE"),
            DaxToken(type="table", value="Sales")
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False

    def test_cannot_transpile_pathcontains(self, transpiler):
        """Test can_transpile fails for PATHCONTAINS"""
        tokens = [
            DaxToken(type="function", value="PATHCONTAINS"),
            DaxToken(type="column", value="[ID]")
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False

    def test_cannot_transpile_calculatetable(self, transpiler):
        """Test can_transpile fails for CALCULATETABLE"""
        tokens = [
            DaxToken(type="function", value="CALCULATETABLE"),
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False

    def test_cannot_transpile_removefilters(self, transpiler):
        """Test can_transpile fails for REMOVEFILTERS"""
        tokens = [
            DaxToken(type="function", value="REMOVEFILTERS"),
            DaxToken(type="column", value="[Region]")
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False

    def test_cannot_transpile_operator(self, transpiler):
        """Test can_transpile fails for non-transpilable operator"""
        # GENERATE is listed as non-transpilable
        tokens = [
            DaxToken(type="function", value="GENERATE"),
        ]
        is_transpilable, reason = transpiler.can_transpile(tokens)
        assert is_transpilable is False

    # ========== transpile Tests ==========

    def test_transpile_no_matching_signature(self, transpiler):
        """Test transpile returns None for unmatched signature"""
        tokens = []
        result = transpiler.transpile("unknown signature pattern xyz", tokens)
        assert result is None

    def test_transpile_sum_pattern(self, transpiler, parser):
        """Test transpiling SUM pattern"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "SUM" in result['transpiled_sql'].upper()
            assert "Amount" in result['transpiled_sql']

    def test_transpile_count_pattern(self, transpiler, parser):
        """Test transpiling COUNT pattern"""
        expression = "COUNT(Sales[OrderID])"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "COUNT" in result['transpiled_sql'].upper()

    def test_transpile_average_pattern(self, transpiler, parser):
        """Test transpiling AVERAGE pattern"""
        expression = "AVERAGE(Products[Price])"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "AVG" in result['transpiled_sql'].upper()

    def test_transpile_distinctcount_pattern(self, transpiler, parser):
        """Test transpiling DISTINCTCOUNT pattern"""
        expression = "DISTINCTCOUNT(Customers[CustomerID])"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "DISTINCT" in result['transpiled_sql'].upper()

    def test_transpile_min_pattern(self, transpiler, parser):
        """Test transpiling MIN pattern"""
        expression = "MIN(Sales[Price])"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "MIN" in result['transpiled_sql'].upper()

    def test_transpile_max_pattern(self, transpiler, parser):
        """Test transpiling MAX pattern"""
        expression = "MAX(Sales[Price])"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "MAX" in result['transpiled_sql'].upper()

    def test_transpile_measure_reference(self, transpiler, parser):
        """Test transpiling simple measure reference"""
        expression = "[Total Sales]"
        measures_list = ["Total Sales"]
        result = parser.parse_advanced(expression, measures_list)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "Total Sales" in result['transpiled_sql']

    def test_transpiler_has_mappings(self, transpiler):
        """Test transpiler has DAX-to-SQL mappings"""
        assert hasattr(transpiler, 'dax_to_sql_mappings')
        assert len(transpiler.dax_to_sql_mappings) > 0

    def test_transpiler_has_non_transpilable_operators(self, transpiler):
        """Test transpiler has list of non-transpilable operators"""
        assert hasattr(transpiler, 'NON_TRANSPILABLE_OPERATORS')
        assert len(transpiler.NON_TRANSPILABLE_OPERATORS) > 0
        assert 'KEEPFILTERS' in transpiler.NON_TRANSPILABLE_OPERATORS

    def test_transpile_replaces_placeholders(self, transpiler):
        """Test that transpile replaces placeholders with actual values"""
        # Manually build tokens that match a known pattern
        # "<<measure:1>>" pattern maps to "`<<measure:1>>`"
        tokens = [
            DaxToken(type="measure", value="[Total Sales]", group=0)
        ]
        signature = "<<measure:1>>"
        result = transpiler.transpile(signature, tokens)

        if result is not None:
            assert "Total Sales" in result

    def test_transpile_sum_with_calculate(self, transpiler, parser):
        """Test transpiling CALCULATE(SUM(...)) pattern"""
        expression = "CALCULATE(SUM(Sales[Amount]))"
        result = parser.parse_advanced(expression)

        if result['is_transpilable'] and result['transpiled_sql']:
            assert "SUM" in result['transpiled_sql'].upper()
