"""
Unit tests for converters/services/powerbi/dax_parser.py

Tests DAX expression parsing with tokenization, signature generation, and transpilability checking.
"""

import pytest
import json
from src.converters.services.powerbi.dax_parser import (
    DaxToken,
    DAXExpressionParser
)


class TestDaxToken:
    """Tests for DaxToken dataclass"""

    def test_token_initialization(self):
        """Test DaxToken initializes correctly"""
        token = DaxToken(type='function', value='SUM', group=1, sequence=0)

        assert token.type == 'function'
        assert token.value == 'SUM'
        assert token.group == 1
        assert token.sequence == 0

    def test_token_defaults(self):
        """Test DaxToken default values"""
        token = DaxToken(type='operator', value='+')

        assert token.group == 0
        assert token.parent_group == 0
        assert token.sequence == 0
        assert token.group_type == ''

    def test_token_to_dict(self):
        """Test token conversion to dictionary"""
        token = DaxToken(type='column', value='[Amount]', group=1, sequence=5)
        result = token.to_dict()

        assert isinstance(result, dict)
        assert result['type'] == 'column'
        assert result['value'] == '[Amount]'
        assert result['group'] == 1
        assert result['sequence'] == 5

    def test_token_to_json(self):
        """Test token conversion to JSON"""
        token = DaxToken(type='number', value='100', group=0)
        result = token.to_json()

        assert isinstance(result, str)
        data = json.loads(result)
        assert data['type'] == 'number'
        assert data['value'] == '100'

    def test_token_from_dict(self):
        """Test token creation from dictionary"""
        data = {
            'type': 'function',
            'value': 'COUNT',
            'group': 2,
            'parent_group': 1,
            'sequence': 3,
            'group_type': 'comparison'
        }
        token = DaxToken.from_dict(data)

        assert token.type == 'function'
        assert token.value == 'COUNT'
        assert token.group == 2
        assert token.parent_group == 1
        assert token.sequence == 3
        assert token.group_type == 'comparison'


class TestDAXExpressionParser:
    """Tests for DAXExpressionParser class"""

    @pytest.fixture
    def parser(self):
        """Create parser instance for testing"""
        return DAXExpressionParser()

    # ========== Simple Parse Tests ==========

    def test_parse_simple_sum(self, parser):
        """Test parsing simple SUM expression"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse(expression)

        assert result['aggregation_type'] == 'SUM'
        assert result['source_table'] == 'Sales'
        assert result['base_formula'] == 'Amount'
        assert result['is_complex'] is False
        assert result['filters'] == []

    def test_parse_calculate_expression(self, parser):
        """Test parsing CALCULATE expression"""
        expression = "CALCULATE(SUM(Sales[Amount]), Region[Name] = \"EMEA\")"
        result = parser.parse(expression)

        assert result['aggregation_type'] == 'SUM'
        assert result['is_complex'] is True
        assert len(result['filters']) > 0

    def test_parse_empty_expression(self, parser):
        """Test parsing empty expression"""
        result = parser.parse("")

        assert result['base_formula'] == ''
        assert result['source_table'] is None
        assert result['aggregation_type'] == 'SUM'
        assert result['filters'] == []
        assert result['is_complex'] is False

    def test_parse_count_expression(self, parser):
        """Test parsing COUNT expression"""
        expression = "COUNT(Orders[OrderID])"
        result = parser.parse(expression)

        assert result['aggregation_type'] == 'COUNT'
        assert result['source_table'] == 'Orders'

    def test_parse_average_expression(self, parser):
        """Test parsing AVERAGE expression"""
        expression = "AVERAGE(Products[Price])"
        result = parser.parse(expression)

        assert result['aggregation_type'] == 'AVERAGE'
        assert result['source_table'] == 'Products'

    # ========== Advanced Parse Tests ==========

    def test_parse_advanced_simple_sum(self, parser):
        """Test advanced parsing of simple SUM"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        assert 'tokens' in result
        assert 'signature' in result
        assert 'generic_signature' in result
        assert len(result['tokens']) > 0
        assert result['aggregation_type'] == 'SUM'

    def test_parse_advanced_empty_expression(self, parser):
        """Test advanced parsing of empty expression"""
        result = parser.parse_advanced("")

        assert result['tokens'] == []
        assert result['signature'] == ''
        assert result['is_transpilable'] is False
        assert result['transpilability_reason'] == 'Empty expression'

    def test_parse_advanced_with_measures_list(self, parser):
        """Test advanced parsing with measures list for disambiguation"""
        expression = "[Total Sales] + [Total Cost]"
        measures_list = ['Total Sales', 'Total Cost']
        result = parser.parse_advanced(expression, measures_list)

        # Should identify both as measures, not columns
        measures = result['tokens']
        measure_tokens = [t for t in measures if t.type == 'measure']
        assert len(measure_tokens) >= 2

    def test_parse_advanced_tokenization(self, parser):
        """Test tokenization produces expected token types"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        tokens = result['tokens']
        token_types = [t.type for t in tokens]

        assert 'function' in token_types  # SUM
        assert 'table' in token_types     # Sales
        assert 'column' in token_types    # [Amount]

    def test_parse_advanced_operators(self, parser):
        """Test parsing expression with operators"""
        expression = "SUM(Sales[Amount]) * 0.9"
        result = parser.parse_advanced(expression)

        operators = result['operators']
        assert len(operators) > 0
        assert any(t.value == '*' for t in operators)

    def test_parse_advanced_functions_list(self, parser):
        """Test parsing extracts function tokens"""
        expression = "CALCULATE(SUM(Sales[Amount]))"
        result = parser.parse_advanced(expression)

        functions = result['functions']
        function_names = [f.value for f in functions]
        assert 'CALCULATE' in function_names or 'calculate' in function_names
        assert 'SUM' in function_names or 'sum' in function_names

    def test_parse_advanced_columns_list(self, parser):
        """Test parsing extracts column tokens"""
        expression = "Sales[Amount] + Sales[Quantity]"
        result = parser.parse_advanced(expression)

        columns = result['columns']
        assert len(columns) >= 2

    # ========== Signature Generation Tests ==========

    def test_signature_simple_expression(self, parser):
        """Test signature generation for simple expression"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        assert result['signature'] != ''
        assert result['generic_signature'] != ''
        # Generic signature should have placeholders
        assert '<<' in result['generic_signature']
        assert '>>' in result['generic_signature']

    def test_signature_generic_has_placeholders(self, parser):
        """Test generic signature uses type placeholders"""
        expression = "SUM(Table1[Col1]) + SUM(Table2[Col2])"
        result = parser.parse_advanced(expression)

        generic_sig = result['generic_signature']
        # Should have table, column placeholders
        assert '<<table:' in generic_sig
        assert '<<column:' in generic_sig

    def test_signature_consistency(self, parser):
        """Test same expression produces same signature"""
        expression = "SUM(Sales[Amount])"
        result1 = parser.parse_advanced(expression)
        result2 = parser.parse_advanced(expression)

        assert result1['signature'] == result2['signature']
        assert result1['generic_signature'] == result2['generic_signature']

    # ========== Transpilability Tests ==========

    def test_check_transpilability_simple(self, parser):
        """Test transpilability check for simple expression"""
        expression = "SUM(Sales[Amount])"
        is_transpilable, reason = parser.check_transpilability(expression)

        assert isinstance(is_transpilable, bool)
        if not is_transpilable:
            assert reason is not None

    def test_check_transpilability_with_measures(self, parser):
        """Test transpilability check with measures list"""
        expression = "[Total Sales]"
        measures_list = ['Total Sales']
        is_transpilable, reason = parser.check_transpilability(expression, measures_list)

        assert isinstance(is_transpilable, bool)

    def test_parse_advanced_transpilability_result(self, parser):
        """Test advanced parse includes transpilability info"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        assert 'is_transpilable' in result
        assert isinstance(result['is_transpilable'], bool)
        if not result['is_transpilable']:
            assert 'transpilability_reason' in result

    # ========== Edge Cases ==========

    def test_parse_none_expression(self, parser):
        """Test parsing None expression"""
        result = parser.parse(None)

        assert result['base_formula'] == ''
        assert result['source_table'] is None

    def test_parse_whitespace_only(self, parser):
        """Test parsing whitespace-only expression"""
        result = parser.parse("   ")

        assert result['base_formula'] == ''
        assert result['is_complex'] is False

    def test_parse_advanced_whitespace_only(self, parser):
        """Test advanced parse of whitespace-only expression"""
        result = parser.parse_advanced("   ")

        assert result['tokens'] == []
        assert result['is_transpilable'] is False

    def test_parse_complex_nested_expression(self, parser):
        """Test parsing complex nested expression"""
        expression = "CALCULATE(SUM(Sales[Amount]), FILTER(Region, Region[Name] = \"EMEA\"))"
        result = parser.parse(expression)

        assert result['is_complex'] is True
        assert result['aggregation_type'] == 'SUM'

    def test_parse_expression_with_numbers(self, parser):
        """Test parsing expression with numeric literals"""
        expression = "SUM(Sales[Amount]) * 1.15"
        result = parser.parse_advanced(expression)

        tokens = result['tokens']
        number_tokens = [t for t in tokens if t.type == 'number']
        assert len(number_tokens) > 0

    def test_parse_expression_with_strings(self, parser):
        """Test parsing expression with string literals"""
        expression = "FILTER(Region, Region[Name] = \"EMEA\")"
        result = parser.parse_advanced(expression)

        tokens = result['tokens']
        string_tokens = [t for t in tokens if t.type == 'string']
        assert len(string_tokens) > 0

    def test_parse_expression_with_parentheses(self, parser):
        """Test parsing tracks parentheses groups correctly"""
        expression = "(SUM(Sales[Amount]))"
        result = parser.parse_advanced(expression)

        tokens = result['tokens']
        paren_tokens = [t for t in tokens if t.type in ['open_paren', 'close_paren']]
        assert len(paren_tokens) > 0

    def test_parse_multiple_aggregations(self, parser):
        """Test parsing expression with multiple aggregations"""
        expression = "SUM(Sales[Amount]) / COUNT(Sales[OrderID])"
        result = parser.parse_advanced(expression)

        functions = result['functions']
        func_values = [f.value.upper() for f in functions]
        assert 'SUM' in func_values
        assert 'COUNT' in func_values
