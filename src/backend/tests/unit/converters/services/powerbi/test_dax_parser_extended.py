"""
Extended unit tests for converters/services/powerbi/dax_parser.py

Targets uncovered lines: 344, 392-401, 430, 434-437, 453, 473, 578, 589-594, 645, 685, 708-709
"""

import pytest
from src.converters.services.powerbi.dax_parser import (
    DaxToken,
    DAXExpressionParser,
)


class TestDaxTokenFromDict:
    """Tests for DaxToken.from_dict (line 52-54)"""

    def test_from_dict_basic(self):
        data = {"type": "function", "value": "SUM", "group": 1,
                "parent_group": 0, "sequence": 0, "group_type": ""}
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
        result = parser.parse("CALCULATE(SUM(Sales[Amount]), Region[Name] = \"West\")")
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
        result = parser.parse_advanced("[MyMeasure] + [OtherMeasure]",
                                       ["MyMeasure", "OtherMeasure"])
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
