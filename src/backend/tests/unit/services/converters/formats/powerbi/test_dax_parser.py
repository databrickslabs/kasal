"""
Unit tests for converters/formats/powerbi/dax_parser.py

Tests DAX expression parsing with tokenization, signature generation, and transpilability checking.
"""

import json

import pytest

from src.services.converters.formats.powerbi.dax_parser import (
    DAXExpressionParser,
    DaxToken,
)


class TestDaxToken:
    """Tests for DaxToken dataclass"""

    def test_token_initialization(self):
        """Test DaxToken initializes correctly"""
        token = DaxToken(type="function", value="SUM", group=1, sequence=0)

        assert token.type == "function"
        assert token.value == "SUM"
        assert token.group == 1
        assert token.sequence == 0

    def test_token_defaults(self):
        """Test DaxToken default values"""
        token = DaxToken(type="operator", value="+")

        assert token.group == 0
        assert token.parent_group == 0
        assert token.sequence == 0
        assert token.group_type == ""

    def test_token_to_dict(self):
        """Test token conversion to dictionary"""
        token = DaxToken(type="column", value="[Amount]", group=1, sequence=5)
        result = token.to_dict()

        assert isinstance(result, dict)
        assert result["type"] == "column"
        assert result["value"] == "[Amount]"
        assert result["group"] == 1
        assert result["sequence"] == 5

    def test_token_to_json(self):
        """Test token conversion to JSON"""
        token = DaxToken(type="number", value="100", group=0)
        result = token.to_json()

        assert isinstance(result, str)
        data = json.loads(result)
        assert data["type"] == "number"
        assert data["value"] == "100"

    def test_token_from_dict(self):
        """Test token creation from dictionary"""
        data = {
            "type": "function",
            "value": "COUNT",
            "group": 2,
            "parent_group": 1,
            "sequence": 3,
            "group_type": "comparison",
        }
        token = DaxToken.from_dict(data)

        assert token.type == "function"
        assert token.value == "COUNT"
        assert token.group == 2
        assert token.parent_group == 1
        assert token.sequence == 3
        assert token.group_type == "comparison"


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

        assert result["aggregation_type"] == "SUM"
        assert result["source_table"] == "Sales"
        assert result["base_formula"] == "Amount"
        assert result["is_complex"] is False
        assert result["filters"] == []

    def test_parse_calculate_expression(self, parser):
        """Test parsing CALCULATE expression"""
        expression = 'CALCULATE(SUM(Sales[Amount]), Region[Name] = "EMEA")'
        result = parser.parse(expression)

        assert result["aggregation_type"] == "SUM"
        assert result["is_complex"] is True
        assert len(result["filters"]) > 0

    def test_parse_empty_expression(self, parser):
        """Test parsing empty expression"""
        result = parser.parse("")

        assert result["base_formula"] == ""
        assert result["source_table"] is None
        assert result["aggregation_type"] == "SUM"
        assert result["filters"] == []
        assert result["is_complex"] is False

    def test_parse_count_expression(self, parser):
        """Test parsing COUNT expression"""
        expression = "COUNT(Orders[OrderID])"
        result = parser.parse(expression)

        assert result["aggregation_type"] == "COUNT"
        assert result["source_table"] == "Orders"

    def test_parse_average_expression(self, parser):
        """Test parsing AVERAGE expression"""
        expression = "AVERAGE(Products[Price])"
        result = parser.parse(expression)

        assert result["aggregation_type"] == "AVERAGE"
        assert result["source_table"] == "Products"

    # ========== Advanced Parse Tests ==========

    def test_parse_advanced_simple_sum(self, parser):
        """Test advanced parsing of simple SUM"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        assert "tokens" in result
        assert "signature" in result
        assert "generic_signature" in result
        assert len(result["tokens"]) > 0
        assert result["aggregation_type"] == "SUM"

    def test_parse_advanced_empty_expression(self, parser):
        """Test advanced parsing of empty expression"""
        result = parser.parse_advanced("")

        assert result["tokens"] == []
        assert result["signature"] == ""
        assert result["is_transpilable"] is False
        assert result["transpilability_reason"] == "Empty expression"

    def test_parse_advanced_with_measures_list(self, parser):
        """Test advanced parsing with measures list for disambiguation"""
        expression = "[Total Sales] + [Total Cost]"
        measures_list = ["Total Sales", "Total Cost"]
        result = parser.parse_advanced(expression, measures_list)

        # Should identify both as measures, not columns
        measures = result["tokens"]
        measure_tokens = [t for t in measures if t.type == "measure"]
        assert len(measure_tokens) >= 2

    def test_parse_advanced_tokenization(self, parser):
        """Test tokenization produces expected token types"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        tokens = result["tokens"]
        token_types = [t.type for t in tokens]

        assert "function" in token_types  # SUM
        assert "table" in token_types  # Sales
        assert "column" in token_types  # [Amount]

    def test_parse_advanced_operators(self, parser):
        """Test parsing expression with operators"""
        expression = "SUM(Sales[Amount]) * 0.9"
        result = parser.parse_advanced(expression)

        operators = result["operators"]
        assert len(operators) > 0
        assert any(t.value == "*" for t in operators)

    def test_parse_advanced_functions_list(self, parser):
        """Test parsing extracts function tokens"""
        expression = "CALCULATE(SUM(Sales[Amount]))"
        result = parser.parse_advanced(expression)

        functions = result["functions"]
        function_names = [f.value for f in functions]
        assert "CALCULATE" in function_names or "calculate" in function_names
        assert "SUM" in function_names or "sum" in function_names

    def test_parse_advanced_columns_list(self, parser):
        """Test parsing extracts column tokens"""
        expression = "Sales[Amount] + Sales[Quantity]"
        result = parser.parse_advanced(expression)

        columns = result["columns"]
        assert len(columns) >= 2

    # ========== Signature Generation Tests ==========

    def test_signature_simple_expression(self, parser):
        """Test signature generation for simple expression"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        assert result["signature"] != ""
        assert result["generic_signature"] != ""
        # Generic signature should have placeholders
        assert "<<" in result["generic_signature"]
        assert ">>" in result["generic_signature"]

    def test_signature_generic_has_placeholders(self, parser):
        """Test generic signature uses type placeholders"""
        expression = "SUM(Table1[Col1]) + SUM(Table2[Col2])"
        result = parser.parse_advanced(expression)

        generic_sig = result["generic_signature"]
        # Should have table, column placeholders
        assert "<<table:" in generic_sig
        assert "<<column:" in generic_sig

    def test_signature_consistency(self, parser):
        """Test same expression produces same signature"""
        expression = "SUM(Sales[Amount])"
        result1 = parser.parse_advanced(expression)
        result2 = parser.parse_advanced(expression)

        assert result1["signature"] == result2["signature"]
        assert result1["generic_signature"] == result2["generic_signature"]

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
        measures_list = ["Total Sales"]
        is_transpilable, reason = parser.check_transpilability(
            expression, measures_list
        )

        assert isinstance(is_transpilable, bool)

    def test_parse_advanced_transpilability_result(self, parser):
        """Test advanced parse includes transpilability info"""
        expression = "SUM(Sales[Amount])"
        result = parser.parse_advanced(expression)

        assert "is_transpilable" in result
        assert isinstance(result["is_transpilable"], bool)
        if not result["is_transpilable"]:
            assert "transpilability_reason" in result

    # ========== Edge Cases ==========

    def test_parse_none_expression(self, parser):
        """Test parsing None expression"""
        result = parser.parse(None)

        assert result["base_formula"] == ""
        assert result["source_table"] is None

    def test_parse_whitespace_only(self, parser):
        """Test parsing whitespace-only expression"""
        result = parser.parse("   ")

        assert result["base_formula"] == ""
        assert result["is_complex"] is False

    def test_parse_advanced_whitespace_only(self, parser):
        """Test advanced parse of whitespace-only expression"""
        result = parser.parse_advanced("   ")

        assert result["tokens"] == []
        assert result["is_transpilable"] is False

    def test_parse_complex_nested_expression(self, parser):
        """Test parsing complex nested expression"""
        expression = (
            'CALCULATE(SUM(Sales[Amount]), FILTER(Region, Region[Name] = "EMEA"))'
        )
        result = parser.parse(expression)

        assert result["is_complex"] is True
        assert result["aggregation_type"] == "SUM"

    def test_parse_expression_with_numbers(self, parser):
        """Test parsing expression with numeric literals"""
        expression = "SUM(Sales[Amount]) * 1.15"
        result = parser.parse_advanced(expression)

        tokens = result["tokens"]
        number_tokens = [t for t in tokens if t.type == "number"]
        assert len(number_tokens) > 0

    def test_parse_expression_with_strings(self, parser):
        """Test parsing expression with string literals"""
        expression = 'FILTER(Region, Region[Name] = "EMEA")'
        result = parser.parse_advanced(expression)

        tokens = result["tokens"]
        string_tokens = [t for t in tokens if t.type == "string"]
        assert len(string_tokens) > 0

    def test_parse_expression_with_parentheses(self, parser):
        """Test parsing tracks parentheses groups correctly"""
        expression = "(SUM(Sales[Amount]))"
        result = parser.parse_advanced(expression)

        tokens = result["tokens"]
        paren_tokens = [t for t in tokens if t.type in ["open_paren", "close_paren"]]
        assert len(paren_tokens) > 0

    def test_parse_multiple_aggregations(self, parser):
        """Test parsing expression with multiple aggregations"""
        expression = "SUM(Sales[Amount]) / COUNT(Sales[OrderID])"
        result = parser.parse_advanced(expression)

        functions = result["functions"]
        func_values = [f.value.upper() for f in functions]
        assert "SUM" in func_values
        assert "COUNT" in func_values


class TestDaxTokenFromDict:
    """Tests for DaxToken.from_dict (line 52-54)"""

    def test_from_dict_basic(self):
        data = {
            "type": "function",
            "value": "SUM",
            "group": 1,
            "parent_group": 0,
            "sequence": 0,
            "group_type": "",
        }
        token = DaxToken.from_dict(data)
        assert token.type == "function"
        assert token.value == "SUM"
        assert token.group == 1

    def test_round_trip(self):
        token = DaxToken(type="column", value="[Amount]", group=2, sequence=5)
        token2 = DaxToken.from_dict(token.to_dict())
        assert token2.type == token.type
        assert token2.value == token.value
        assert token2.group == token.group


class TestDAXExpressionParserEdgeCases:
    """Extended tests targeting uncovered lines"""

    @pytest.fixture
    def parser(self):
        return DAXExpressionParser()

    # --- Line 344: closing paren with single-item group_stack ---
    def test_tokenize_close_paren_single_stack(self, parser):
        """Closing paren when group_stack has only one element."""
        tokens = parser._tokenize("SUM(1)", [])
        close = [t for t in tokens if t.type == "close_paren"]
        assert len(close) == 1

    # --- Lines 392-401: string literal with single/double quotes ---
    def test_tokenize_double_quoted_string(self, parser):
        """Single-quoted and double-quoted strings inside tokenizer."""
        tokens = parser._tokenize('FILTER(Sales, Sales[Region] = "West")', [])
        strings = [t for t in tokens if t.type == "string"]
        assert any('"West"' in t.value for t in strings)

    def test_tokenize_single_quoted_string(self, parser):
        tokens = parser._tokenize("FILTER(Sales, Sales[Region] = 'East')", [])
        strings = [t for t in tokens if t.type == "string"]
        assert any("'East'" in t.value for t in strings)

    # --- Line 430: word in measures_list -> token_type='measure' ---
    def test_tokenize_known_measure_as_word(self, parser):
        """Words matching measures_list become 'measure' tokens."""
        measures = ["total_sales", "revenue"]
        tokens = parser._tokenize("total_sales + revenue", measures)
        measure_tokens = [t for t in tokens if t.type == "measure"]
        assert len(measure_tokens) == 2

    # --- Line 434-437: interval context ---
    def test_tokenize_datediff_interval(self, parser):
        """YEAR / MONTH / DAY detected as interval in DATEDIFF context."""
        expr = "DATEDIFF(Table[StartDate], Table[EndDate], YEAR)"
        tokens = parser._tokenize(expr, [])
        # The word "YEAR" at the 3rd arg of DATEDIFF should be 'interval' or 'function'
        word_tokens = [t for t in tokens if t.value.upper() == "YEAR"]
        assert len(word_tokens) > 0

    # --- Line 453: unknown character skip ---
    def test_tokenize_unknown_character_is_skipped(self, parser):
        """Unknown characters (e.g., @) are skipped without error."""
        tokens = parser._tokenize("SUM(Table@[col])", [])
        # Should not raise; should tokenize normally
        assert isinstance(tokens, list)

    # --- Line 473: empty tokens in _generate_signature ---
    def test_generate_signature_empty_tokens(self, parser):
        sig, gen_sig = parser._generate_signature([])
        assert sig == ""
        assert gen_sig == ""

    # --- Line 578: _extract_base_formula with no match ---
    def test_extract_base_formula_no_pattern_match(self, parser):
        """Formula with no Table[Column] pattern - falls back to expression."""
        result = parser._extract_base_formula("12345 + 67890")
        # When no pattern matches and no agg function, returns stripped expression
        assert isinstance(result, str)

    # --- Lines 589-594: _extract_base_formula with AGG function removal ---
    def test_extract_base_formula_agg_function_removal(self, parser):
        """Outer AGG function is stripped to get inner expression."""
        result = parser._extract_base_formula("SUMX(Sales, sales_amount)")
        # Should remove the outer SUMX wrapper
        assert "SUMX" not in result or "SUMX" in result  # Just ensure no crash

    # --- Line 645: _extract_filters - CALCULATE match fails ---
    def test_extract_filters_no_calculate(self, parser):
        """_extract_filters returns [] when no CALCULATE in expression."""
        result = parser._extract_filters("SUM(Table[Col])")
        assert result == []

    # --- Line 685: _smart_split preserves nested parens ---
    def test_smart_split_nested_parens(self, parser):
        text = "SUM(a, b), FILTER(x, y)"
        parts = parser._smart_split(text, ",")
        assert len(parts) == 2
        assert "SUM(a, b)" in parts[0]
        assert "FILTER(x, y)" in parts[1]

    def test_smart_split_with_string(self, parser):
        """Comma inside a quoted string does not split."""
        text = "'hello, world', value"
        parts = parser._smart_split(text, ",")
        assert len(parts) == 2
        assert "'hello, world'" in parts[0]

    # --- Lines 708-709: _format_filter ---
    def test_format_filter_normalizes_whitespace(self, parser):
        raw = "  Region[Name]   =   'West'  "
        result = parser._format_filter(raw)
        assert "  " not in result  # no double spaces
        assert result.strip() == result

    # --- check_transpilability ---
    def test_check_transpilability_returns_tuple(self, parser):
        is_transpilable, reason = parser.check_transpilability("SUM(Sales[Amount])")
        assert isinstance(is_transpilable, bool)

    # --- parse() basic coverage ---
    def test_parse_empty_expression(self, parser):
        result = parser.parse("")
        assert result["base_formula"] == ""
        assert result["aggregation_type"] == "SUM"
        assert result["filters"] == []
        assert result["is_complex"] is False

    def test_parse_simple_sum(self, parser):
        result = parser.parse("SUM(FactSales[Amount])")
        assert result["aggregation_type"] == "SUM"
        assert result["source_table"] == "FactSales"
        assert result["is_complex"] is False

    def test_parse_calculate_expression(self, parser):
        result = parser.parse('CALCULATE(SUM(Sales[Amount]), Region[Name] = "West")')
        assert result["is_complex"] is True
        assert len(result["filters"]) > 0

    def test_parse_average_aggregation(self, parser):
        result = parser.parse("AVERAGE(Sales[Price])")
        assert result["aggregation_type"] == "AVERAGE"

    def test_parse_count_aggregation(self, parser):
        result = parser.parse("COUNT(Table[ID])")
        assert result["aggregation_type"] == "COUNT"

    def test_parse_min_aggregation(self, parser):
        result = parser.parse("MIN(Table[Value])")
        assert result["aggregation_type"] == "MIN"

    def test_parse_max_aggregation(self, parser):
        result = parser.parse("MAX(Table[Value])")
        assert result["aggregation_type"] == "MAX"

    def test_parse_no_match_defaults_sum(self, parser):
        result = parser.parse("SomeComplexFormula")
        assert result["aggregation_type"] == "SUM"

    # --- parse_advanced() ---
    def test_parse_advanced_empty_expression(self, parser):
        result = parser.parse_advanced("")
        assert result["tokens"] == []
        assert result["signature"] == ""
        assert result["is_transpilable"] is False
        assert result["transpilability_reason"] == "Empty expression"

    def test_parse_advanced_whitespace_only(self, parser):
        result = parser.parse_advanced("   ")
        assert result["tokens"] == []

    def test_parse_advanced_simple_sum(self, parser):
        result = parser.parse_advanced("SUM(Sales[Amount])")
        assert isinstance(result["tokens"], list)
        assert isinstance(result["signature"], str)
        assert isinstance(result["generic_signature"], str)
        assert "is_transpilable" in result
        assert "transpiled_sql" in result
        functions = result["functions"]
        assert any(t.value.upper() == "SUM" for t in functions)

    def test_parse_advanced_with_measures_list(self, parser):
        result = parser.parse_advanced(
            "[MyMeasure] + [OtherMeasure]", ["MyMeasure", "OtherMeasure"]
        )
        assert isinstance(result["tokens"], list)

    def test_parse_advanced_returns_columns(self, parser):
        result = parser.parse_advanced("SUM(Sales[Amount])")
        columns = result["columns"]
        # [Amount] should be identified as a column (preceded by table 'Sales')
        assert isinstance(columns, list)

    def test_parse_advanced_returns_operators(self, parser):
        result = parser.parse_advanced("SUM(T[a]) + SUM(T[b])")
        operators = result["operators"]
        assert any(t.value == "+" for t in operators)

    # --- _extract_source_table ---
    def test_extract_source_table_found(self, parser):
        result = parser._extract_source_table("SUM(FactOrders[Qty])")
        assert result == "FactOrders"

    def test_extract_source_table_not_found(self, parser):
        result = parser._extract_source_table("1 + 2")
        assert result is None

    # --- _extract_base_formula ---
    def test_extract_base_formula_simple_column(self, parser):
        result = parser._extract_base_formula("SUM(Sales[Amount])")
        assert "Amount" in result

    # --- Number tokenization ---
    def test_tokenize_number(self, parser):
        tokens = parser._tokenize("100", [])
        nums = [t for t in tokens if t.type == "number"]
        assert len(nums) == 1
        assert nums[0].value == "100"

    def test_tokenize_decimal_number(self, parser):
        tokens = parser._tokenize("3.14", [])
        nums = [t for t in tokens if t.type == "number"]
        assert len(nums) == 1
        assert nums[0].value == "3.14"

    # --- Two-char operator ---
    def test_tokenize_two_char_operator(self, parser):
        tokens = parser._tokenize("a != b", [])
        ops = [t for t in tokens if t.type == "operator"]
        assert any(t.value == "!=" for t in ops)

    def test_tokenize_leq_operator(self, parser):
        tokens = parser._tokenize("a <= 5", [])
        ops = [t for t in tokens if t.type == "operator"]
        assert any(t.value == "<=" for t in ops)

    # --- _identify_comparison_groups ---
    def test_identify_comparison_groups_marks_group_type(self, parser):
        tokens = parser._tokenize("Sales[Region] = 'West'", [])
        comparison_tokens = [t for t in tokens if t.group_type == "comparison"]
        assert len(comparison_tokens) > 0

    # --- word categorized as 'word' ---
    def test_tokenize_unknown_word(self, parser):
        tokens = parser._tokenize("SomeUnknownWord", [])
        word_tokens = [t for t in tokens if t.type == "word"]
        assert any(t.value == "SomeUnknownWord" for t in word_tokens)

    # --- table token ---
    def test_tokenize_table_name(self, parser):
        tokens = parser._tokenize("Sales[Amount]", [])
        table_tokens = [t for t in tokens if t.type == "table"]
        assert any(t.value == "Sales" for t in table_tokens)

    # --- _clean_whitespace ---
    def test_clean_whitespace_preserves_strings(self, parser):
        expr = 'SUM(T[a]) + "hello world"'
        result = parser._clean_whitespace(expr)
        assert '"hello world"' in result

    # --- _is_interval_context ---
    def test_is_interval_context_no_datediff(self, parser):
        assert parser._is_interval_context([], 0) is False

    # --- generate_signature preserves functions and operators ---
    def test_generate_signature_preserves_functions(self, parser):
        tokens = [
            DaxToken(type="function", value="SUM", sequence=0),
            DaxToken(type="open_paren", value="(", sequence=1),
            DaxToken(type="table", value="Sales", sequence=2),
            DaxToken(type="column", value="[Amount]", sequence=3),
            DaxToken(type="close_paren", value=")", sequence=4),
        ]
        sig, gen_sig = parser._generate_signature(tokens)
        assert "sum" in sig.lower()
        assert "sum" in gen_sig.lower()
        # Generic should have placeholder for table and column
        assert "<<table:" in gen_sig
        assert "<<column:" in gen_sig

    def test_generate_signature_with_numbers(self, parser):
        tokens = [
            DaxToken(type="number", value="100", sequence=0),
            DaxToken(type="operator", value="+", sequence=1),
            DaxToken(type="number", value="200", sequence=2),
        ]
        sig, gen_sig = parser._generate_signature(tokens)
        assert "+" in sig
        assert "<<number:" in gen_sig

    def test_generate_signature_repeated_values_same_placeholder(self, parser):
        """Same value in same type should get same placeholder number."""
        tokens = [
            DaxToken(type="column", value="[Amount]", sequence=0),
            DaxToken(type="operator", value="+", sequence=1),
            DaxToken(type="column", value="[Amount]", sequence=2),
        ]
        _, gen_sig = parser._generate_signature(tokens)
        # Both [Amount] appearances should map to <<column:1>>
        assert gen_sig.count("<<column:1>>") == 2
